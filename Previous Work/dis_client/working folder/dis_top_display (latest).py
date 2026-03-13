#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import queue
import signal
import sys
import threading
import time

import can
import zmq

CONFIG_PATH = "/home/pi/config.json"

try:
    from icons import audscii_trans
except Exception:
    print("ERROR: icons.py not found or failed to import.")
    sys.exit(1)


def nice_or_ignore():
    try:
        os.nice(-10)
    except Exception:
        pass


class TextScroller:
    """Scrolls an 8-character window over arbitrary-length text."""

    def __init__(self, width=8, speed=0.35, pause=2.0, stagger=0.0):
        self.width         = width
        self.raw_text      = ""
        self.pos           = 0
        self.last_tick     = 0.0
        self.scroll_speed  = float(speed)
        self.pause_time    = float(pause)
        self.stagger_delay = float(stagger)
        self.wait_timer    = 0.0
        self.current_bytes  = [0x20] * width
        self.current_window = " " * width
        self.lock           = threading.Lock()

    def set_text(self, text: str):
        text = (text or "").strip()
        if text == self.raw_text:
            return
        with self.lock:
            self.raw_text       = text
            self.pos            = 0
            now                 = time.monotonic()
            self.last_tick      = now - self.scroll_speed  # first step fires exactly at wait_timer
            self.wait_timer     = now + self.pause_time + self.stagger_delay
            self.current_window = " " * self.width     # FIX: blank immediately so old text
            self.current_bytes  = [0x20] * self.width  # doesn't flash on shorter new text
            if text:
                self._update_bytes()

    def tick(self):
        if not self.raw_text or len(self.raw_text) <= self.width:
            return
        now = time.monotonic()
        if now < self.wait_timer:
            return
        with self.lock:
            if now - self.last_tick <= self.scroll_speed:  # FIX: check inside lock
                return
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
        if len(self.raw_text) <= self.width:
            window = self.raw_text.center(self.width)
        else:
            window = self.raw_text[self.pos:self.pos + self.width].ljust(self.width)
        self.current_window = window
        self.current_bytes = [
            audscii_trans[ord(c)] if ord(c) < len(audscii_trans) else 0x20
            for c in self.current_window
        ]

    def clear(self):
        with self.lock:
            self.raw_text       = ""
            self.pos            = 0
            self.current_window = " " * self.width
            self.current_bytes  = [0x20] * self.width

    def snapshot(self):
        with self.lock:
            return bytes(self.current_bytes)


class LineController:
    """
    Owns everything for one DIS line:
      - TextScroller
      - Slam queue (fed by CANWatcher, never blocks RX)
      - Tail schedule
      - TX thread (direct socketcan for slam/tail, ZMQ for baseline)
    """

    def __init__(self, line_num, can_id, scroller, tx_bus, zmq_push,
                 slam_count, slam_gap, tail_sends, refresh_period, watcher=None):
        self.line_num       = line_num
        self.can_id         = can_id
        self.scroller       = scroller
        self.tx_bus         = tx_bus
        self.push           = zmq_push
        self.slam_count     = slam_count
        self.slam_gap       = slam_gap
        self.tail_sends     = tail_sends
        self.refresh_period = refresh_period
        self._watcher       = watcher

        self._slam_q        = queue.Queue()
        self._tail_times    = []
        self._tail_lock     = threading.Lock()
        self.last_sent      = None
        self._force_refresh = False

    def trigger_slam(self, payload: bytes):
        self._slam_q.put(payload)

    def clear_queue(self):
        """Drain stale slam payloads — called on TV source activation."""
        while not self._slam_q.empty():
            try:
                self._slam_q.get_nowait()
            except queue.Empty:
                break

    def push_text(self):
        """Queue a slam with current scroller bytes so last_sent is updated
        before the next OEM periodic write arrives, closing the flash window."""
        self.trigger_slam(self.scroller.snapshot())

    def force_refresh(self):
        self._force_refresh = True

    def _send_zmq(self, text: str):
        try:
            self.push.send_json({"line": self.line_num, "text": text}, zmq.NOBLOCK)
        except Exception:
            pass

    def run(self):
        next_refresh = 0.0

        while True:
            now = time.monotonic()

            # Drain slam queue — keep only the most recent payload
            payload = None
            while not self._slam_q.empty():
                try:
                    payload = self._slam_q.get_nowait()
                except queue.Empty:
                    break

            if payload is not None and self._watcher and self._watcher.tv_active:
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in range(self.slam_count):
                    try:
                        self.tx_bus.send(msg)
                    except Exception:
                        break
                    time.sleep(self.slam_gap)
                self.last_sent = payload
                with self._tail_lock:
                    self._tail_times = [now + dt for dt in self.tail_sends]

            # Tail sends — use live scroller bytes, update last_sent
            with self._tail_lock:
                due = [t for t in self._tail_times if now >= t]
                self._tail_times = [t for t in self._tail_times if now < t]

            if due and self._watcher and self._watcher.tv_active:
                payload = self.scroller.snapshot()
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in due:
                    try:
                        self.tx_bus.send(msg)
                    except Exception:
                        pass
                self.last_sent = payload  # FIX: keep last_sent current after tail

            # Baseline refresh — ZMQ
            if (now >= next_refresh or self._force_refresh) and self._watcher and self._watcher.tv_active:
                self._force_refresh = False
                with self.scroller.lock:
                    text = self.scroller.current_window
                self._send_zmq(text)
                next_refresh = now + self.refresh_period

            time.sleep(0.002)


class CANWatcher:
    """
    Owns the RX CAN bus. Watches 0x661 to gate activity on TV tuner source.
    Triggers a slam whenever the OEM writes anything to a display line
    that differs from what we last sent.
    """

    TV_SOURCE_BYTE = 0x37

    def __init__(self, bus, id_source, id_line1, id_line2, ctrl_l1, ctrl_l2, dis_ctrl=None):
        self.bus       = bus
        self.id_source = id_source
        self.id_line1  = id_line1
        self.id_line2  = id_line2
        self.ctrl_l1   = ctrl_l1
        self.ctrl_l2   = ctrl_l2
        self.dis_ctrl  = dis_ctrl
        self.tv_active = False

    def run(self):
        while True:
            try:  # FIX: catch CAN errors so RX thread never dies silently
                msg = self.bus.recv(timeout=0.5)
                if not msg or msg.is_extended_id:
                    continue

                cid  = msg.arbitration_id
                data = bytes(msg.data)

                if cid == self.id_source and len(msg.data) >= 4:
                    was_active     = self.tv_active
                    self.tv_active = (msg.data[3] == self.TV_SOURCE_BYTE)
                    if self.tv_active and not was_active:
                        if self.dis_ctrl:
                            self.dis_ctrl._no_media_shown = False
                        for ctrl in (self.ctrl_l1, self.ctrl_l2):
                            if ctrl:
                                ctrl.last_sent = None
                                ctrl.clear_queue()   # FIX: discard stale queued payloads
                                ctrl.push_text()     # FIX: set last_sent before OEM's first write

                elif self.tv_active and self.ctrl_l1 and cid == self.id_line1:
                    if data != self.ctrl_l1.last_sent:
                        self.ctrl_l1.trigger_slam(self.ctrl_l1.scroller.snapshot())

                elif self.tv_active and self.ctrl_l2 and cid == self.id_line2:
                    if data != self.ctrl_l2.last_sent:
                        self.ctrl_l2.trigger_slam(self.ctrl_l2.scroller.snapshot())

            except Exception:
                time.sleep(0.1)  # FIX: brief pause then retry on CAN error


class DISController:
    """
    Thin orchestrator — loads config, wires components together, runs main loop.
    """

    def __init__(self):
        nice_or_ignore()

        with open(CONFIG_PATH, "r") as f:
            cfg = json.load(f)

        feat = cfg.get("features", {}).get("fis_display", {})
        if not feat.get("enabled", False):
            print("fis_display disabled; exiting.")
            sys.exit(0)

        self.running = True

        can_ids   = cfg.get("can_ids", {})
        id_line1  = int(can_ids.get("fis_line1", "0x363"), 16)
        id_line2  = int(can_ids.get("fis_line2", "0x365"), 16)
        id_source = int(can_ids.get("source",    "0x661"), 16)

        iface  = cfg.get("can_interface", "can0")
        rx_bus = can.Bus(interface="socketcan", channel=iface)
        tx_bus = can.Bus(interface="socketcan", channel=iface)

        ctx = zmq.Context()
        self._sub = ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.RCVTIMEO, 1000)  # FIX: don't block forever if hudiy dies
        self._sub.connect(cfg["zmq"]["publish_address"])
        self._sub.connect(cfg["zmq"]["hudiy_publish_address"])
        self._sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")

        def make_push():
            s = ctx.socket(zmq.PUSH)
            s.connect(cfg["zmq"]["dis_draw"])
            return s

        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))

        speed   = feat.get("scroll_speed",   0.35)
        pause   = feat.get("scroll_pause",   2.0)
        stagger = feat.get("stagger_offset", 1.0)
        s1 = TextScroller(8, speed, pause, 0.0)
        s2 = TextScroller(8, speed, pause, stagger)

        self._ctrl_l1 = LineController(
            line_num=1, can_id=id_line1, scroller=s1,
            tx_bus=tx_bus, zmq_push=make_push(),
            slam_count=8,  slam_gap=0.004,
            tail_sends=[0.03, 0.08, 0.15, 0.30, 0.55],
            refresh_period=2.0,
        ) if l1_mode != "0" else None

        self._ctrl_l2 = LineController(
            line_num=2, can_id=id_line2, scroller=s2,
            tx_bus=tx_bus, zmq_push=make_push(),
            slam_count=12, slam_gap=0.003,
            tail_sends=[0.02, 0.05, 0.10, 0.20, 0.40, 0.70],
            refresh_period=2.0,
        ) if l2_mode != "0" else None

        self._watcher = CANWatcher(
            bus=rx_bus,
            id_source=id_source, id_line1=id_line1, id_line2=id_line2,
            ctrl_l1=self._ctrl_l1, ctrl_l2=self._ctrl_l2,
            dis_ctrl=self,
        )
        if self._ctrl_l1: self._ctrl_l1._watcher = self._watcher
        if self._ctrl_l2: self._ctrl_l2._watcher = self._watcher

        self._l1_mode = l1_mode
        self._l2_mode = l2_mode
        self._rx_bus  = rx_bus
        self._tx_bus  = tx_bus

        self.NO_MEDIA_L1 = feat.get("no_media_line1", "No Media")
        self.NO_MEDIA_L2 = feat.get("no_media_line2", "")

        if self._ctrl_l1 and self.NO_MEDIA_L1:
            self._ctrl_l1.scroller.set_text(self.NO_MEDIA_L1)
        if self._ctrl_l2 and self.NO_MEDIA_L2:
            self._ctrl_l2.scroller.set_text(self.NO_MEDIA_L2)

        self._boot_no_media_delay = float(feat.get("boot_no_media_delay", 3.0))
        self._boot_time           = time.monotonic()
        self._hudiy_connected     = False
        self._no_media_shown      = False

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        self.running = False

    def _parse_mode(self, mode_str, p):
        def clean(v):
            s = (v or "").strip()
            if not s:
                return ""
            if not any(c.isalnum() for c in s):
                return ""
            return s

        title  = clean(p.get("title") or p.get("track"))
        artist = clean(p.get("artist"))
        album  = clean(p.get("album"))
        source = clean(p.get("source_label") or p.get("source"))

        if "-" in mode_str:
            parts = [x.strip() for x in mode_str.split("-")]
            out = []
            for val in parts:
                if val == "1": out.append(title)
                elif val == "2": out.append(artist)
                elif val == "3": out.append(album)
                elif val == "9": out.append(source)
            filled = [x for x in out if x]
            return " - ".join(filled) if len(filled) >= 2 else (filled[0] if filled else "")

        if mode_str == "1": return title
        if mode_str == "2": return artist
        if mode_str == "3": return album
        if mode_str == "9": return source
        return ""

    def _show_no_media(self):
        for ctrl, text in ((self._ctrl_l1, self.NO_MEDIA_L1), (self._ctrl_l2, self.NO_MEDIA_L2)):
            if ctrl:
                ctrl.scroller.set_text(text) if text else ctrl.scroller.clear()
                snap = ctrl.scroller.snapshot()
                ctrl.last_sent = snap    # update immediately so CANWatcher sees no mismatch
                ctrl.trigger_slam(snap)  # queue slam to push bytes to display
        self._no_media_shown = True

    def _metadata_listener(self):
        while self.running:
            try:
                parts = self._sub.recv_multipart()
                p = json.loads(parts[1])
                l1_text = self._parse_mode(self._l1_mode, p) if self._ctrl_l1 else ""
                l2_text = self._parse_mode(self._l2_mode, p) if self._ctrl_l2 else ""
                self._hudiy_connected = True
                self._no_media_shown  = False
                if self._watcher.tv_active:
                    for ctrl, text, fallback in (
                        (self._ctrl_l1, l1_text, self.NO_MEDIA_L1),
                        (self._ctrl_l2, l2_text, self.NO_MEDIA_L2),
                    ):
                        if ctrl:
                            ctrl.scroller.set_text(text if text else fallback)
                            snap = ctrl.scroller.snapshot()
                            ctrl.last_sent = snap    # update immediately so CANWatcher sees no mismatch
                            ctrl.trigger_slam(snap)  # queue slam to push new bytes to display
            except zmq.Again:
                pass  # FIX: expected timeout — loop to check self.running
            except Exception:
                time.sleep(0.05)

    def run(self):
        threading.Thread(target=self._metadata_listener, daemon=True).start()
        threading.Thread(target=self._watcher.run,       daemon=True).start()

        if self._ctrl_l1:
            threading.Thread(target=self._ctrl_l1.run, daemon=True).start()
        if self._ctrl_l2:
            threading.Thread(target=self._ctrl_l2.run, daemon=True).start()

        while self.running:
            now = time.monotonic()

            if not self._no_media_shown and not self._hudiy_connected \
                    and (now - self._boot_time) >= self._boot_no_media_delay:
                self._show_no_media()

            if self._ctrl_l1:
                self._ctrl_l1.scroller.tick()
            if self._ctrl_l2:
                self._ctrl_l2.scroller.tick()
            time.sleep(0.03)

        for bus in (self._rx_bus, self._tx_bus):
            try:
                bus.shutdown()
            except Exception:
                pass


if __name__ == "__main__":
    DISController().run()