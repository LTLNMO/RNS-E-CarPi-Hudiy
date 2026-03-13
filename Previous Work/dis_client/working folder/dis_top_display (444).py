#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DIS top display — direct socketcan, no ZMQ, no external dependencies.
All translation and CAN writes happen in-process.
ZMQ metadata listener can be added back once flash-free operation is confirmed.
"""

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

# Precompute full 256-entry translation table as bytes for O(1) branchless lookup
_TRANS = bytes(
    audscii_trans[i] if i < len(audscii_trans) else 0x20
    for i in range(256)
)
_BLANK8 = bytes([0x20] * 8)


def nice_or_ignore():
    try:
        os.nice(-10)
    except Exception:
        pass


class TextScroller:
    """Scrolls an 8-character window over arbitrary-length text."""

    def __init__(self, width=8, speed=0.35, pause=2.0, stagger=0.0):
        self.width         = width
        self.scroll_speed  = float(speed)
        self.pause_time    = float(pause)
        self.stagger_delay = float(stagger)
        self.lock          = threading.Lock()
        self._reset("")

    def _reset(self, text):
        """Set all mutable state. Must be called with lock held (or before threads start)."""
        self.raw_text  = text
        self.raw_len   = len(text)
        self.pos       = 0
        now            = time.monotonic()
        self.last_tick = now - self.scroll_speed
        self.wait_timer = now + self.pause_time + self.stagger_delay
        # _recompute first — current_bytes goes directly from old to new with no
        # intermediate blank state, so snapshot() never returns stale blank bytes
        self._recompute()

    def set_text(self, text: str):
        text = (text or "").strip()
        with self.lock:
            if text == self.raw_text:
                return
            self._reset(text)

    def tick(self, ctrl=None):
        """Advance scroll position if due. Passes ctrl so slam fires immediately."""
        now = time.monotonic()
        with self.lock:
            # All state reads inside lock — raw_len/wait_timer written by set_text
            if self.raw_len <= self.width:
                return
            if now < self.wait_timer:
                return
            if now - self.last_tick <= self.scroll_speed:
                return
            max_pos = self.raw_len - self.width
            self.pos = self.pos + 1 if self.pos < max_pos else 0
            if self.pos == 0 or self.pos == max_pos:
                self.wait_timer = now + self.pause_time
            self._recompute()
            self.last_tick = now
            if ctrl and ctrl._watcher and ctrl._watcher.tv_active:
                ctrl.last_sent = self.current_bytes
                ctrl._slam_q.put(self.current_bytes)

    def _recompute(self):
        """Recompute window text and translated bytes from current pos. Lock must be held."""
        if self.raw_len <= self.width:
            window = self.raw_text.center(self.width)
        else:
            window = self.raw_text[self.pos:self.pos + self.width].ljust(self.width)
        self.current_window = window
        self.current_bytes  = bytes(_TRANS[ord(c) & 0xFF] for c in window)

    def clear(self):
        with self.lock:
            self._reset("")

    def snapshot(self):
        with self.lock:
            return self.current_bytes


class LineController:
    """One DIS display line — all sends are direct socketcan."""

    def __init__(self, can_id, scroller, tx_bus,
                 slam_count, slam_gap, tail_sends, watcher=None):
        self.can_id     = can_id
        self.scroller   = scroller
        self.tx_bus     = tx_bus
        self.slam_count = slam_count
        self.slam_gap   = slam_gap
        self.tail_sends = tail_sends
        self._watcher   = watcher
        self._slam_q    = queue.Queue()
        self._tail_lock = threading.Lock()
        self._tail_times = []
        self.last_sent  = None

    def trigger_slam(self, payload: bytes):
        self._slam_q.put(payload)

    def push_text(self):
        self._slam_q.put(self.scroller.snapshot())

    def clear_queue(self):
        try:
            while True:
                self._slam_q.get_nowait()
        except queue.Empty:
            pass

    def _send(self, msg):
        try:
            self.tx_bus.send(msg)
            return True
        except Exception:
            return False

    def run(self):
        while True:
            # Block up to 2ms for a slam, then service tails regardless
            try:
                payload = self._slam_q.get(timeout=0.002)
                # Drain — only fire the most recent payload
                try:
                    while True:
                        payload = self._slam_q.get_nowait()
                except queue.Empty:
                    pass
            except queue.Empty:
                payload = None

            now = time.monotonic()
            tv  = self._watcher and self._watcher.tv_active

            if payload is not None and tv:
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in range(self.slam_count):
                    if not self._send(msg):
                        break
                    time.sleep(self.slam_gap)
                self.last_sent = payload
                with self._tail_lock:
                    self._tail_times = [now + dt for dt in self.tail_sends]

            # Tail sends — single pass partition
            if self._tail_times and tv:
                with self._tail_lock:
                    old_times, self._tail_times = self._tail_times, []
                    due = []
                    for t in old_times:
                        (due if now >= t else self._tail_times).append(t)
                if due:
                    payload = self.scroller.snapshot()
                    msg = can.Message(arbitration_id=self.can_id,
                                      data=payload, is_extended_id=False)
                    for _ in due:
                        self._send(msg)
                    self.last_sent = payload


class CANWatcher:
    """RX thread — gates on TV source, triggers slams on OEM display overwrites."""

    TV_SOURCE_BYTE = 0x37

    def __init__(self, bus, id_source, id_line1, id_line2,
                 ctrl_l1, ctrl_l2, dis_ctrl=None):
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
            try:
                msg = self.bus.recv(timeout=0.5)
                if not msg or msg.is_extended_id:
                    continue

                cid  = msg.arbitration_id
                data = bytes(msg.data)

                if cid == self.id_source and len(data) >= 4:
                    was_active     = self.tv_active
                    self.tv_active = (data[3] == self.TV_SOURCE_BYTE)
                    if self.tv_active and not was_active:
                        # TV just activated — reset and push current content
                        if self.dis_ctrl:
                            self.dis_ctrl._no_media_shown = False
                        for ctrl in (self.ctrl_l1, self.ctrl_l2):
                            if ctrl:
                                ctrl.last_sent = None
                                ctrl.clear_queue()
                                ctrl.push_text()
                    elif not self.tv_active and was_active:
                        # TV deactivated — allow No Media to show again next time
                        if self.dis_ctrl:
                            self.dis_ctrl._no_media_shown = False

                elif self.tv_active:
                    # OEM wrote to a display line — slam back if it's not our bytes
                    for ctrl, line_id in ((self.ctrl_l1, self.id_line1),
                                          (self.ctrl_l2, self.id_line2)):
                        if ctrl and cid == line_id:
                            snap = ctrl.scroller.snapshot()
                            if data != ctrl.last_sent and data != snap:
                                ctrl.trigger_slam(snap)
                            break

            except Exception:
                time.sleep(0.1)


class DISController:
    """Loads config, wires components, runs main loop."""

    def __init__(self):
        nice_or_ignore()

        with open(CONFIG_PATH) as f:
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

        speed   = feat.get("scroll_speed",   0.35)
        pause   = feat.get("scroll_pause",   2.0)
        stagger = feat.get("stagger_offset", 1.0)

        # ZMQ — subscribe to hudiy metadata for song/source updates
        ctx = zmq.Context()
        self._sub = ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.RCVTIMEO, 1000)
        self._sub.connect(cfg["zmq"]["hudiy_publish_address"])
        self._sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")

        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))

        self._ctrl_l1 = LineController(
            can_id=id_line1,
            scroller=TextScroller(8, speed, pause, 0.0),
            tx_bus=tx_bus,
            slam_count=8,  slam_gap=0.004,
            tail_sends=[0.03, 0.08, 0.15, 0.30, 0.55],
        ) if l1_mode != "0" else None

        self._ctrl_l2 = LineController(
            can_id=id_line2,
            scroller=TextScroller(8, speed, pause, stagger),
            tx_bus=tx_bus,
            slam_count=12, slam_gap=0.003,
            tail_sends=[0.02, 0.05, 0.10, 0.20, 0.40, 0.70],
        ) if l2_mode != "0" else None

        self._watcher = CANWatcher(
            bus=rx_bus,
            id_source=id_source, id_line1=id_line1, id_line2=id_line2,
            ctrl_l1=self._ctrl_l1, ctrl_l2=self._ctrl_l2,
            dis_ctrl=self,
        )
        for ctrl in (self._ctrl_l1, self._ctrl_l2):
            if ctrl:
                ctrl._watcher = self._watcher

        self._l1_mode = l1_mode
        self._l2_mode = l2_mode
        self._rx_bus = rx_bus
        self._tx_bus = tx_bus

        self.NO_MEDIA_L1 = feat.get("no_media_line1", "No Media")
        self.NO_MEDIA_L2 = feat.get("no_media_line2", "")

        # Pre-load scrollers so content is ready before TV activates
        for ctrl, text in ((self._ctrl_l1, self.NO_MEDIA_L1),
                           (self._ctrl_l2, self.NO_MEDIA_L2)):
            if ctrl and text:
                ctrl.scroller.set_text(text)

        self._boot_delay    = float(feat.get("boot_no_media_delay", 3.0))
        self._boot_time     = time.monotonic()
        self._hudiy_connected = False
        self._no_media_shown  = False

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        self.running = False

    def _show_no_media(self):
        for ctrl, text in ((self._ctrl_l1, self.NO_MEDIA_L1),
                           (self._ctrl_l2, self.NO_MEDIA_L2)):
            if not ctrl:
                continue
            ctrl.scroller.set_text(text) if text else ctrl.scroller.clear()
            snap = ctrl.scroller.snapshot()
            ctrl.last_sent = snap
            ctrl.trigger_slam(snap)
        self._no_media_shown = True

    def _parse_mode(self, mode_str, p):
        def clean(v):
            s = (v or "").strip()
            return s if any(c.isalnum() for c in s) else ""
        fields = {
            "1": clean(p.get("title") or p.get("track")),
            "2": clean(p.get("artist")),
            "3": clean(p.get("album")),
            "9": clean(p.get("source_label") or p.get("source")),
        }
        keys  = [k.strip() for k in mode_str.split("-")] if "-" in mode_str else [mode_str]
        parts = [fields[k] for k in keys if k in fields and fields[k]]
        return " - ".join(parts) if len(parts) >= 2 else (parts[0] if parts else "")

    def _apply_metadata(self, l1_text, l2_text):
        for ctrl, text, fallback in (
            (self._ctrl_l1, l1_text, self.NO_MEDIA_L1),
            (self._ctrl_l2, l2_text, self.NO_MEDIA_L2),
        ):
            if not ctrl:
                continue
            effective = text if text else fallback
            # set_text returns early if text unchanged — check if it actually changed
            # by comparing before and after, so we only slam on real content changes
            old_text = ctrl.scroller.raw_text
            ctrl.scroller.set_text(effective)
            if ctrl.scroller.raw_text == old_text:
                continue  # text unchanged — no slam, no scroll reset
            if self._watcher.tv_active:
                with ctrl._tail_lock:
                    ctrl._tail_times = []
                snap = ctrl.scroller.snapshot()
                ctrl.last_sent = snap
                ctrl.trigger_slam(snap)

    def _metadata_listener(self):
        # Debounce: hudiy sends 2-3 rapid messages on song change (transition
        # states), causing old->new->old->new flicker. Buffer the latest and
        # only apply after a short quiet period.
        DEBOUNCE = 0.12
        pending  = None
        deadline = None

        while self.running:
            now = time.monotonic()
            # Commit pending if debounce window elapsed
            if pending is not None and now >= deadline:
                self._apply_metadata(*pending)
                pending = deadline = None

            try:
                parts = self._sub.recv_multipart(flags=zmq.NOBLOCK)
                p       = json.loads(parts[1])
                l1_text = self._parse_mode(self._l1_mode, p) if self._ctrl_l1 else ""
                l2_text = self._parse_mode(self._l2_mode, p) if self._ctrl_l2 else ""
                self._hudiy_connected = True
                self._no_media_shown  = False
                pending  = (l1_text, l2_text)
                deadline = time.monotonic() + DEBOUNCE
            except zmq.Again:
                # No message — sleep until debounce deadline or 50ms
                if deadline is not None:
                    time.sleep(max(0, min(0.05, deadline - time.monotonic())))
                else:
                    time.sleep(0.05)
            except Exception:
                time.sleep(0.05)

    def run(self):
        threading.Thread(target=self._metadata_listener, daemon=True).start()
        threading.Thread(target=self._watcher.run, daemon=True).start()
        for ctrl in (self._ctrl_l1, self._ctrl_l2):
            if ctrl:
                threading.Thread(target=ctrl.run, daemon=True).start()

        while self.running:
            now = time.monotonic()
            if not self._no_media_shown and not self._hudiy_connected \
                    and (now - self._boot_time) >= self._boot_delay:
                self._show_no_media()
            for ctrl in (self._ctrl_l1, self._ctrl_l2):
                if ctrl:
                    ctrl.scroller.tick(ctrl)
            time.sleep(0.03)

        for bus in (self._rx_bus, self._tx_bus):
            try:
                bus.shutdown()
            except Exception:
                pass


if __name__ == "__main__":
    DISController().run()