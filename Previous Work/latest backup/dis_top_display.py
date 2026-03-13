#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import signal
import sys
import threading
import time
from typing import List, Optional

import can
import zmq

CONFIG_PATH = "/home/pi/config.json"

try:
    from icons import audscii_trans  # your Audi charset map
except Exception:
    audscii_trans = None


def nice_or_ignore():
    try:
        os.nice(-10)
    except Exception:
        pass


def to8_bytes(text: str) -> List[int]:
    """8-char -> 8 bytes. Uses audscii_trans if present, else ASCII."""
    s = (text or "")[:8].ljust(8)
    if audscii_trans:
        out = []
        for c in s:
            oc = ord(c)
            if 0 <= oc < len(audscii_trans):
                out.append(audscii_trans[oc])
            else:
                out.append(0x20)
        return out
    return list(s.encode("ascii", errors="replace")[:8].ljust(8, b" "))


class TextScroller:
    def __init__(self, width=8, speed=0.35, pause=2.0, stagger=0.0):
        self.width = width
        self.raw_text = ""
        self.pos = 0
        self.last_tick = 0.0
        self.scroll_speed = float(speed)
        self.pause_time = float(pause)
        self.stagger_delay = float(stagger)
        self.wait_timer = 0.0
        self.current_bytes = [0x20] * 8
        self.lock = threading.Lock()

    def set_text(self, text: str):
        text = (text or "").strip()
        if text != self.raw_text:
            with self.lock:
                self.raw_text = text
                self.pos = 0
                self.wait_timer = time.time() + self.pause_time + self.stagger_delay
                self._update_bytes()

    def tick(self):
        if not self.raw_text:
            return

        # show static when short
        if len(self.raw_text) <= self.width:
            with self.lock:
                self.pos = 0
                self._update_bytes()
            return

        now = time.time()
        if now < self.wait_timer:
            return

        if now - self.last_tick > self.scroll_speed:
            with self.lock:
                max_pos = len(self.raw_text) - self.width
                if self.pos < max_pos:
                    self.pos += 1
                    if self.pos == max_pos:
                        self.wait_timer = now + self.pause_time
                else:
                    self.pos = 0
                    self.wait_timer = now + self.pause_time

                self._update_bytes()
                self.last_tick = now

    def _update_bytes(self):
        window = self.raw_text[self.pos:self.pos + self.width].ljust(self.width)
        self.current_bytes = to8_bytes(window)


class DISHybridAVLock:
    # OEM payload we observed on 0x363
    AV_PAYLOAD = bytes([0x20, 0x41, 0x56, 0x20, 0x20, 0x20, 0x20, 0x20])  # " AV     "

    def __init__(self):
        nice_or_ignore()

        with open(CONFIG_PATH, "r") as f:
            cfg = json.load(f)

        feat = cfg.get("features", {}).get("fis_display", {})
        if not feat.get("enabled", False):
            print("fis_display disabled; exiting.")
            sys.exit(0)

        self.running = True

        # CAN
        self.can_iface = cfg.get("can_interface", "can0")
        self.bus = can.Bus(interface="socketcan", channel=self.can_iface)

        # ZMQ
        self.ctx = zmq.Context()
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(cfg["zmq"]["publish_address"])
        self.sub.connect(cfg["zmq"]["hudiy_publish_address"])
        self.sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")

        # Display modes
        self.l1_mode = str(feat.get("line1_mode", "0"))
        self.l2_mode = str(feat.get("line2_mode", "0"))

        # Scrollers (use your same feel)
        self.s1 = TextScroller(8, feat.get("scroll_speed", 0.35), feat.get("scroll_pause", 2.0), 0.0)
        self.s2 = TextScroller(8, feat.get("scroll_speed", 0.35), feat.get("scroll_pause", 2.0),
                               feat.get("stagger_offset", 1.0))

        # ---- Key behavioral defaults (match your working AV-lock style) ----
        self.enable_470 = True          # SD-capture keepalive helped you
        self.keepalive_470_period = 0.05

        self.enable_602 = False         # IMPORTANT: do NOT send 0x602 (hybrid got worse)
        self.hb_period = 0.10

        self.refresh_period = 0.05      # gentle periodic refresh like SD capture-ish
        self.tick_period = 0.03         # scroller tick

        # AV-lock tuning (the version that worked best for you)
        self.slam_count = 8
        self.slam_gap = 0.004
        self.tail_sends = [0.03, 0.08, 0.15, 0.30, 0.55]  # extend if needed
        self.tail_times: List[float] = []
        self.tail_lock = threading.Lock()

        # Source gating: latch TV active for a while (prevents flapping)
        self.tv_latch_seconds = 3.0
        self.last_tv_seen = 0.0
        self.force_show_on_dis = bool(feat.get("force_show_on_dis", False))

        # Stats
        self.send_errs = 0
        self.av_seen = 0

        # prebuilt messages
        self.msg_470 = can.Message(
            arbitration_id=0x470,
            data=[0x80, 0x10, 0x2F, 0x1D, 0x20, 0x00, 0x00, 0x00],
            is_extended_id=False,
        )
        self.msg_602 = can.Message(
            arbitration_id=0x602,
            data=[0x89, 0x12, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00],
            is_extended_id=False,
        )

        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

        # Track last sent payloads to avoid unnecessary traffic
        self.last_sent_363: Optional[bytes] = None
        self.last_sent_365: Optional[bytes] = None

    def shutdown(self, *_):
        self.running = False

    def active(self) -> bool:
        if self.force_show_on_dis:
            return True
        return (time.time() - self.last_tv_seen) < self.tv_latch_seconds

    def _safe_send(self, msg: can.Message) -> bool:
        try:
            self.bus.send(msg)
            return True
        except Exception:
            self.send_errs += 1
            # tiny backoff to help if socket buffer is briefly full
            time.sleep(0.002)
            return False

    def parse_mode(self, mode_str: str, p: dict) -> str:
        title = p.get("title") or p.get("track") or ""
        artist = p.get("artist") or ""
        album = p.get("album") or ""
        source = p.get("source_label") or p.get("source") or ""

        if "-" in mode_str:
            parts = [x.strip() for x in mode_str.split("-")]
            out = []
            for val in parts:
                if val == "1": out.append(title)
                elif val == "2": out.append(artist)
                elif val == "3": out.append(album)
                elif val == "9": out.append(source)
            return " - ".join([x for x in out if x])

        if mode_str == "1": return title
        if mode_str == "2": return artist
        if mode_str == "3": return album
        if mode_str == "9": return source
        return ""

    # -------- Threads --------

    def metadata_listener(self):
        while self.running:
            try:
                parts = self.sub.recv_multipart()
                payload = json.loads(parts[1])
                if self.l1_mode != "0":
                    self.s1.set_text(self.parse_mode(self.l1_mode, payload))
                if self.l2_mode != "0":
                    self.s2.set_text(self.parse_mode(self.l2_mode, payload))
            except Exception:
                time.sleep(0.05)

    def can_rx_thread(self):
        """
        Single CAN RX thread:
        - Update TV latch from 0x661
        - Trigger AV-lock on 0x363 == " AV     "
        """
        while self.running:
            msg = self.bus.recv(timeout=0.5)
            if not msg or msg.is_extended_id:
                continue

            cid = msg.arbitration_id

            # TV / Source latch
            if cid == 0x661 and len(msg.data) >= 4:
                if msg.data[3] == 0x37:  # TV
                    self.last_tv_seen = time.time()

            # AV-lock trigger
            if cid == 0x363 and bytes(msg.data) == self.AV_PAYLOAD and self.active() and self.l1_mode != "0":
                self.av_seen += 1
                self._slam_and_tail()

    def _slam_and_tail(self):
        # snapshot current line1 bytes
        with self.s1.lock:
            payload = bytes(self.s1.current_bytes)

        m363 = can.Message(arbitration_id=0x363, data=payload, is_extended_id=False)

        # immediate slam
        for _ in range(max(1, self.slam_count)):
            if not self._safe_send(m363):
                break
            time.sleep(self.slam_gap)

        # resettable tail schedule
        t0 = time.time()
        with self.tail_lock:
            self.tail_times = [t0 + float(dt) for dt in self.tail_sends]

    def tx_thread(self):
        """
        TX is gentle:
        - optional keepalive 0x470
        - optional 0x602 (disabled by default)
        - periodic refresh of 0x363/0x365 (default 0.8s)
        - tail sends (win after OEM burst ends)
        """
        next470 = 0.0
        next602 = 0.0
        next_refresh = 0.0

        while self.running:
            now = time.time()
            if not self.active():
                time.sleep(0.05)
                continue

            # keepalive 0x470
            if self.enable_470 and now >= next470:
                self._safe_send(self.msg_470)
                next470 = now + self.keepalive_470_period

            # optional 0x602
            if self.enable_602 and now >= next602:
                self._safe_send(self.msg_602)
                next602 = now + self.hb_period

            # periodic refresh (send only if changed, or on timer)
            if now >= next_refresh:
                # line 1
                if self.l1_mode != "0":
                    with self.s1.lock:
                        p363 = bytes(self.s1.current_bytes)
                    if self.last_sent_363 != p363:
                        self._safe_send(can.Message(arbitration_id=0x363, data=p363, is_extended_id=False))
                        self.last_sent_363 = p363
                    else:
                        # still refresh occasionally to stay "sticky"
                        self._safe_send(can.Message(arbitration_id=0x363, data=p363, is_extended_id=False))

                # line 2
                if self.l2_mode != "0":
                    with self.s2.lock:
                        p365 = bytes(self.s2.current_bytes)
                    if self.last_sent_365 != p365:
                        self._safe_send(can.Message(arbitration_id=0x365, data=p365, is_extended_id=False))
                        self.last_sent_365 = p365
                    else:
                        self._safe_send(can.Message(arbitration_id=0x365, data=p365, is_extended_id=False))

                next_refresh = now + self.refresh_period

            # tail schedule (line1)
            with self.tail_lock:
                if self.tail_times and now >= self.tail_times[0]:
                    self.tail_times.pop(0)
                    if self.l1_mode != "0":
                        with self.s1.lock:
                            self._safe_send(can.Message(arbitration_id=0x363, data=self.s1.current_bytes, is_extended_id=False))

            time.sleep(0.002)

    def run(self):
        threading.Thread(target=self.metadata_listener, daemon=True).start()
        threading.Thread(target=self.can_rx_thread, daemon=True).start()
        threading.Thread(target=self.tx_thread, daemon=True).start()

        while self.running:
            if self.active():
                if self.l1_mode != "0":
                    self.s1.tick()
                if self.l2_mode != "0":
                    self.s2.tick()
            time.sleep(self.tick_period)

        try:
            self.bus.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    DISHybridAVLock().run()
