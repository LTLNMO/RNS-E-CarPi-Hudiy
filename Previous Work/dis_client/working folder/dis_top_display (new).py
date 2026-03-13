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


# ---------------------------------------------------------------------------
# TextScroller
# ---------------------------------------------------------------------------

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

    def set_text(self, text):
        text = (text or "").strip()
        if text == self.raw_text:
            return
        with self.lock:
            self.raw_text   = text
            self.pos        = 0
            self.wait_timer = time.time() + self.pause_time + self.stagger_delay
            self._update_bytes()

    def tick(self):
        if not self.raw_text:
            return
        # Static text — no scrolling needed
        if len(self.raw_text) <= self.width:
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
        self.current_window = self.raw_text[self.pos:self.pos + self.width].ljust(self.width)
        self.current_bytes = [
            audscii_trans[ord(c)] if ord(c) < len(audscii_trans) else 0x20
            for c in self.current_window
        ]

    def snapshot(self):
        """Return immutable snapshot of current bytes. Thread-safe."""
        with self.lock:
            return bytes(self.current_bytes)


# ---------------------------------------------------------------------------
# LineController
# ---------------------------------------------------------------------------

class LineController:
    """
    Owns everything for one DIS line:
      - TextScroller
      - Slam queue (fed by CANWatcher, never blocks RX)
      - Tail schedule
      - TX thread (direct socketcan for slam/tail, ZMQ for baseline)
    """

    def __init__(self, line_num, can_id, scroller, tx_bus, zmq_push,
                 slam_count, slam_gap, tail_sends, refresh_period):
        self.line_num       = line_num
        self.can_id         = can_id
        self.scroller       = scroller
        self.tx_bus         = tx_bus
        self.push           = zmq_push
        self.slam_count     = slam_count
        self.slam_gap       = slam_gap
        self.tail_sends     = tail_sends
        self.refresh_period = refresh_period

        self._slam_q      = queue.Queue()
        self._tail_times  = []
        self._tail_lock   = threading.Lock()

    def trigger_slam(self, payload: bytes):
        """Called by CANWatcher — non-blocking, posts to slam queue."""
        self._slam_q.put(payload)

    def _send_direct(self, payload: bytes):
        try:
            self.tx_bus.send(can.Message(
                arbitration_id=self.can_id,
                data=payload,
                is_extended_id=False,
            ))
        except Exception:
            pass

    def _send_zmq(self, text: str):
        try:
            self.push.send_json({"line": self.line_num, "text": text}, zmq.NOBLOCK)
        except Exception:
            pass

    def run(self):
        """TX thread — handles slam queue, tail schedule, and baseline refresh."""
        next_refresh = 0.0

        while True:
            now = time.time()

            # --- Drain slam queue ---
            while not self._slam_q.empty():
                try:
                    payload = self._slam_q.get_nowait()
                except queue.Empty:
                    break
                # Slam: rapid-fire direct sends using snapshot at trigger time
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in range(self.slam_count):
                    try:
                        self.tx_bus.send(msg)
                    except Exception:
                        break
                    time.sleep(self.slam_gap)
                # Schedule tail — no payload stored, tail always uses live scroller
                with self._tail_lock:
                    self._tail_times = [now + dt for dt in self.tail_sends]

            # --- Tail sends — always use live scroller bytes ---
            with self._tail_lock:
                due = [t for t in self._tail_times if now >= t]
                self._tail_times = [t for t in self._tail_times if now < t]

            if due:
                payload = self.scroller.snapshot()
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in due:
                    try:
                        self.tx_bus.send(msg)
                    except Exception:
                        pass

            # --- Baseline refresh via ZMQ — always use live scroller window ---
            if now >= next_refresh:
                with self.scroller.lock:
                    text = self.scroller.current_window
                self._send_zmq(text)
                next_refresh = now + self.refresh_period

            time.sleep(0.002)


# ---------------------------------------------------------------------------
# CANWatcher
# ---------------------------------------------------------------------------

class CANWatcher:
    """
    Owns the RX CAN bus. Never blocks — on trigger detection it posts
    a payload snapshot to the appropriate LineController's slam queue.
    """

    AV_PAYLOAD = bytes([0x20, 0x41, 0x56, 0x20, 0x20, 0x20, 0x20, 0x20])  # " AV     "
    TV_PAYLOAD = bytes([0x54, 0x56, 0x2F, 0x56, 0x49, 0x44, 0x45, 0x4F])  # "TV/VIDEO"

    def __init__(self, bus, id_source, id_line1, id_line2,
                 ctrl_l1, ctrl_l2, active_fn):
        self.bus        = bus
        self.id_source  = id_source
        self.id_line1   = id_line1
        self.id_line2   = id_line2
        self.ctrl_l1    = ctrl_l1
        self.ctrl_l2    = ctrl_l2
        self.active     = active_fn
        self.last_tv    = 0.0

    def run(self):
        """RX thread — never sleeps, never blocks on slam sends."""
        while True:
            msg = self.bus.recv(timeout=0.5)
            if not msg or msg.is_extended_id:
                continue

            cid  = msg.arbitration_id
            data = bytes(msg.data)

            if cid == self.id_source and len(msg.data) >= 4:
                if msg.data[3] == 0x37:
                    self.last_tv = time.time()

            elif cid == self.id_line1 and data == self.AV_PAYLOAD and self.active():
                payload = self.ctrl_l1.scroller.snapshot()
                self.ctrl_l1.trigger_slam(payload)

            elif cid == self.id_line2 and data == self.TV_PAYLOAD and self.active():
                payload = self.ctrl_l2.scroller.snapshot()
                self.ctrl_l2.trigger_slam(payload)

    def tv_active(self, latch_seconds=3.0):
        return (time.time() - self.last_tv) < latch_seconds


# ---------------------------------------------------------------------------
# DISController
# ---------------------------------------------------------------------------

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

        # CAN IDs
        can_ids = cfg.get("can_ids", {})
        id_line1  = int(can_ids.get("fis_line1", "0x363"), 16)
        id_line2  = int(can_ids.get("fis_line2", "0x365"), 16)
        id_source = int(can_ids.get("source",    "0x661"), 16)

        # CAN buses
        iface   = cfg.get("can_interface", "can0")
        rx_bus  = can.Bus(interface="socketcan", channel=iface)
        tx_bus  = can.Bus(interface="socketcan", channel=iface)

        # ZMQ
        ctx = zmq.Context()
        self._sub = ctx.socket(zmq.SUB)
        self._sub.connect(cfg["zmq"]["publish_address"])
        self._sub.connect(cfg["zmq"]["hudiy_publish_address"])
        self._sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")

        def make_push():
            s = ctx.socket(zmq.PUSH)
            s.connect(cfg["zmq"]["dis_draw"])
            return s

        # Display modes
        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))

        # Scrollers
        speed  = feat.get("scroll_speed", 0.35)
        pause  = feat.get("scroll_pause", 2.0)
        stagger = feat.get("stagger_offset", 1.0)
        s1 = TextScroller(8, speed, pause, 0.0)
        s2 = TextScroller(8, speed, pause, stagger)

        # Source gating
        force = bool(feat.get("force_show_on_dis", True))
        self._watcher_ref = None  # set after watcher created

        def active():
            if force:
                return True
            return self._watcher_ref.tv_active() if self._watcher_ref else False

        # Line controllers
        self._ctrl_l1 = LineController(
            line_num=1, can_id=id_line1, scroller=s1,
            tx_bus=tx_bus, zmq_push=make_push(),
            slam_count=8,  slam_gap=0.004,
            tail_sends=[0.03, 0.08, 0.15, 0.30, 0.55],
            refresh_period=0.05,
        ) if l1_mode != "0" else None

        self._ctrl_l2 = LineController(
            line_num=2, can_id=id_line2, scroller=s2,
            tx_bus=tx_bus, zmq_push=make_push(),
            slam_count=12, slam_gap=0.003,
            tail_sends=[0.02, 0.05, 0.10, 0.20, 0.40, 0.70],
            refresh_period=0.025,
        ) if l2_mode != "0" else None

        # CAN watcher
        self._watcher = CANWatcher(
            bus=rx_bus,
            id_source=id_source, id_line1=id_line1, id_line2=id_line2,
            ctrl_l1=self._ctrl_l1, ctrl_l2=self._ctrl_l2,
            active_fn=active,
        )
        self._watcher_ref = self._watcher

        # Metadata parsing
        self._l1_mode = l1_mode
        self._l2_mode = l2_mode
        self._rx_bus  = rx_bus
        self._tx_bus  = tx_bus

        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, *_):
        self.running = False

    def _parse_mode(self, mode_str, p):
        title  = p.get("title") or p.get("track") or ""
        artist = p.get("artist") or ""
        album  = p.get("album") or ""
        source = p.get("source_label") or p.get("source") or ""

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

    def _metadata_listener(self):
        while self.running:
            try:
                parts = self._sub.recv_multipart()
                p = json.loads(parts[1])
                if self._ctrl_l1:
                    self._ctrl_l1.scroller.set_text(self._parse_mode(self._l1_mode, p))
                if self._ctrl_l2:
                    self._ctrl_l2.scroller.set_text(self._parse_mode(self._l2_mode, p))
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