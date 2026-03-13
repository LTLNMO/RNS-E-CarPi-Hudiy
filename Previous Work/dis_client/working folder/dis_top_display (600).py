#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DIS top display — direct socketcan, optimized for hudiy_data V2.9.
All translation and CAN writes happen in-process.

Self-healing mechanisms:
  1. Watchdog re-assert — if TV is active and a line hasn't sent anything in
     WATCHDOG_STALE seconds, it re-pushes current content automatically.
     Catches any edge case that slipped through all other guards.

  2. OEM contention escalation — CANWatcher counts re-slams per line per second.
     If a line is being fought over (> CONTENTION_THRESHOLD re-slams/sec), that
     line's slam count is temporarily doubled and extra tails are added.
     Resets back to normal after CONTENTION_RESET seconds of calm.

  3. ZMQ socket self-heal — if _metadata_listener hits consecutive non-zmq.Again
     exceptions, it tears down and rebuilds the ZMQ socket automatically.

  4. CAN send failure tracking — consecutive _send() failures are counted and
     logged as warnings so bus issues are visible in logs.

Liveness model (no heartbeat from V2.9):
  - _hudiy_connected = True only when HUDIY_MEDIA arrives with source_id != 0
    AND (playing=True OR title is non-empty)
  - Times out after _MEDIA_TIMEOUT seconds of no qualifying media message
  - Clear packet (source_id=0) immediately drops liveness without waiting for timeout
"""

import json
import logging
import os
import queue
import signal
import sys
import threading
import time

import can
import zmq

CONFIG_PATH = "/home/pi/config.json"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (DIS) %(message)s'
)
logger = logging.getLogger(__name__)

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

# Self-healing tuning
WATCHDOG_STALE      = 2.0   # seconds of silence on an active TV line before re-assert
CONTENTION_THRESHOLD = 3    # re-slams/sec before escalating slam count
CONTENTION_RESET    = 5.0   # seconds of calm before de-escalating
CAN_FAIL_WARN       = 5     # consecutive CAN failures before logging a warning


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
        self.raw_text   = text
        self.raw_len    = len(text)
        self.pos        = 0
        now             = time.monotonic()
        self.last_tick  = now - self.scroll_speed
        self.wait_timer = now + self.pause_time + self.stagger_delay
        self._recompute()

    def set_text(self, text: str):
        text = (text or "").strip()
        with self.lock:
            if text == self.raw_text:
                return
            self._reset(text)

    def tick(self, ctrl=None):
        """Advance scroll position if due. Only queues a slam when TV is active.

        Does NOT set ctrl.last_sent — only run() sets it after actual CAN send.
        """
        now = time.monotonic()
        with self.lock:
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
                ctrl._slam_q.put(self.current_bytes)

    def _recompute(self):
        """Recompute window bytes from current pos. Lock must be held."""
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
    """One DIS display line — all sends are direct socketcan.

    Self-healing features:
      - Watchdog: re-pushes if TV active and nothing sent in WATCHDOG_STALE seconds
      - Contention escalation: boosts slam_count when OEM is fighting persistently
      - CAN failure tracking: logs warnings on repeated send failures
    """

    def __init__(self, can_id, scroller, tx_bus,
                 slam_count, slam_gap, tail_sends, watcher=None, name=""):
        self.can_id         = can_id
        self.scroller       = scroller
        self.tx_bus         = tx_bus
        self._base_slam     = slam_count   # original slam count for de-escalation
        self.slam_count     = slam_count
        self.slam_gap       = slam_gap
        self._base_tails    = list(tail_sends)
        self.tail_sends     = list(tail_sends)
        self._watcher       = watcher
        self._slam_q        = queue.Queue()
        self._tail_lock     = threading.Lock()
        self._tail_times    = []
        self.last_sent      = None         # ONLY set in run() after actual CAN send
        self.last_send_time = 0.0          # monotonic time of last successful send
        self.name           = name         # for logging

        # Contention tracking
        self._reslam_times    = []         # timestamps of recent re-slams
        self._contention_high = False
        self._last_calm_time  = time.monotonic()

        # CAN failure tracking
        self._can_fail_count  = 0

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

    def record_reslam(self):
        """Called by CANWatcher when it triggers a re-slam on this line."""
        now = time.monotonic()
        # Keep only re-slams from the last 1 second
        self._reslam_times = [t for t in self._reslam_times if now - t < 1.0]
        self._reslam_times.append(now)

        if len(self._reslam_times) >= CONTENTION_THRESHOLD and not self._contention_high:
            self._contention_high = True
            # Double slam count and add extra early tails
            self.slam_count = self._base_slam * 2
            self.tail_sends = [0.01, 0.02] + self._base_tails
            logger.warning(f"[{self.name}] OEM contention detected — escalating slam to {self.slam_count}")

        if self._contention_high:
            self._last_calm_time = now  # reset calm timer while fighting

    def _check_de_escalate(self, now):
        """De-escalate if we've had CONTENTION_RESET seconds without contention."""
        if self._contention_high and (now - self._last_calm_time) >= CONTENTION_RESET:
            self._contention_high = False
            self.slam_count = self._base_slam
            self.tail_sends = list(self._base_tails)
            logger.info(f"[{self.name}] OEM contention resolved — slam count restored")

    def _send(self, msg):
        try:
            self.tx_bus.send(msg)
            self._can_fail_count = 0
            return True
        except Exception:
            self._can_fail_count += 1
            if self._can_fail_count == CAN_FAIL_WARN:
                logger.warning(f"[{self.name}] {CAN_FAIL_WARN} consecutive CAN send failures")
            return False

    def run(self):
        while True:
            # Block up to 2ms for a slam, then service watchdog and tails
            try:
                payload = self._slam_q.get(timeout=0.002)
                # Drain — only fire the most recent payload.
                # Rapid song skips: all intermediate titles are discarded,
                # only the final one hits the CAN bus.
                try:
                    while True:
                        payload = self._slam_q.get_nowait()
                except queue.Empty:
                    pass
            except queue.Empty:
                payload = None

            now = time.monotonic()
            tv  = self._watcher and self._watcher.tv_active

            self._check_de_escalate(now)

            # --- Watchdog re-assert ---
            # If TV is active and this line has been silent for too long,
            # re-push the current scroller content unconditionally.
            if tv and self.last_send_time > 0 and (now - self.last_send_time) >= WATCHDOG_STALE:
                watchdog_payload = self.scroller.snapshot()
                if payload is None or payload == watchdog_payload:
                    payload = watchdog_payload
                    logger.debug(f"[{self.name}] Watchdog re-assert fired")

            if payload is not None and tv:
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in range(self.slam_count):
                    if not self._send(msg):
                        break
                    time.sleep(self.slam_gap)
                # last_sent set HERE — after the message actually went out.
                # CANWatcher compares against this so it always reflects the bus.
                self.last_sent      = payload
                self.last_send_time = time.monotonic()
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
                    # snapshot() at fire time — always reflects current scroll pos
                    payload = self.scroller.snapshot()
                    msg = can.Message(arbitration_id=self.can_id,
                                      data=payload, is_extended_id=False)
                    for _ in due:
                        self._send(msg)
                    self.last_sent      = payload
                    self.last_send_time = time.monotonic()


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
                        # TV just activated — clear stale state and push content
                        logger.info("TV source activated")
                        if self.dis_ctrl:
                            self.dis_ctrl._no_media_shown = False
                        for ctrl in (self.ctrl_l1, self.ctrl_l2):
                            if ctrl:
                                ctrl.last_sent      = None
                                ctrl.last_send_time = time.monotonic()
                                ctrl.clear_queue()
                                with ctrl._tail_lock:
                                    ctrl._tail_times = []
                                ctrl.push_text()

                    elif not self.tv_active and was_active:
                        logger.info("TV source deactivated")
                        if self.dis_ctrl:
                            self.dis_ctrl._no_media_shown = False

                elif self.tv_active:
                    # OEM wrote to a display line — slam back if not our bytes.
                    # last_sent is always what's truly on the bus (set post-send),
                    # so this comparison is accurate.
                    for ctrl, line_id in ((self.ctrl_l1, self.id_line1),
                                          (self.ctrl_l2, self.id_line2)):
                        if ctrl and cid == line_id:
                            snap = ctrl.scroller.snapshot()
                            if data != ctrl.last_sent and data != snap:
                                ctrl.record_reslam()   # track for contention escalation
                                ctrl.trigger_slam(snap)
                            break

            except Exception:
                time.sleep(0.1)


class DISController:
    """Loads config, wires components, runs main loop."""

    # No heartbeat from hudiy_data V2.9 — rely on media message recency
    _MEDIA_TIMEOUT = 8.0

    def __init__(self):
        nice_or_ignore()

        with open(CONFIG_PATH) as f:
            cfg = json.load(f)

        feat = cfg.get("features", {}).get("fis_display", {})
        if not feat.get("enabled", False):
            print("fis_display disabled; exiting.")
            sys.exit(0)

        self.running = True
        self._cfg    = cfg   # kept for ZMQ reconnect

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

        self._zmq_ctx  = zmq.Context()
        self._zmq_addr = cfg["zmq"]["hudiy_publish_address"]
        self._sub      = self._make_zmq_sub()

        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))

        self._ctrl_l1 = LineController(
            can_id=id_line1,
            scroller=TextScroller(8, speed, pause, 0.0),
            tx_bus=tx_bus,
            slam_count=8,  slam_gap=0.004,
            tail_sends=[0.03, 0.08, 0.15, 0.30, 0.55],
            name="L1",
        ) if l1_mode != "0" else None

        self._ctrl_l2 = LineController(
            can_id=id_line2,
            scroller=TextScroller(8, speed, pause, stagger),
            tx_bus=tx_bus,
            slam_count=12, slam_gap=0.003,
            tail_sends=[0.02, 0.05, 0.10, 0.20, 0.40, 0.70],
            name="L2",
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
        self._rx_bus  = rx_bus
        self._tx_bus  = tx_bus

        self.NO_MEDIA_L1 = feat.get("no_media_line1", "No Media")
        self.NO_MEDIA_L2 = feat.get("no_media_line2", "")

        for ctrl, text in ((self._ctrl_l1, self.NO_MEDIA_L1),
                           (self._ctrl_l2, self.NO_MEDIA_L2)):
            if ctrl and text:
                ctrl.scroller.set_text(text)

        self._boot_delay     = float(feat.get("boot_no_media_delay", 3.0))
        self._boot_time      = time.monotonic()
        self._last_media_msg = 0.0
        self._hudiy_connected = False
        self._no_media_shown  = False

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _make_zmq_sub(self):
        """Create (or recreate) the ZMQ subscriber socket."""
        sub = self._zmq_ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.RCVTIMEO, 1000)
        sub.connect(self._zmq_addr)
        sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")
        return sub

    def _reconnect_zmq(self):
        """Tear down and rebuild the ZMQ socket — called on repeated exceptions."""
        logger.warning("ZMQ socket error — reconnecting...")
        try:
            self._sub.close()
        except Exception:
            pass
        time.sleep(1.0)
        self._sub = self._make_zmq_sub()
        logger.info("ZMQ socket reconnected")

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
        for ctrl, text in (
            (self._ctrl_l1, l1_text),
            (self._ctrl_l2, l2_text),
        ):
            if not ctrl:
                continue
            # Leave current display untouched if parsed text is empty —
            # don't show NO_MEDIA text while music is actively playing
            if not text:
                continue
            old_text = ctrl.scroller.raw_text
            ctrl.scroller.set_text(text)
            if ctrl.scroller.raw_text == old_text:
                continue
            if self._watcher.tv_active:
                with ctrl._tail_lock:
                    ctrl._tail_times = []
                snap = ctrl.scroller.snapshot()
                ctrl.last_sent = snap
                ctrl.trigger_slam(snap)

    def _metadata_listener(self):
        DEBOUNCE      = 0.18
        pending       = None
        deadline      = None
        zmq_err_count = 0   # consecutive non-zmq.Again exceptions

        while self.running:
            now = time.monotonic()

            if pending is not None and now >= deadline:
                self._apply_metadata(*pending)
                pending = deadline = None

            self._hudiy_connected = (now - self._last_media_msg) < self._MEDIA_TIMEOUT

            try:
                parts = self._sub.recv_multipart(flags=zmq.NOBLOCK)
                topic = parts[0]
                data  = json.loads(parts[1])
                zmq_err_count = 0   # reset on successful receive

                if topic == b"HUDIY_MEDIA":
                    source_id = data.get("source_id", 0)
                    playing   = data.get("playing", False)
                    title     = (data.get("title") or "").strip()

                    if source_id != 0 and (playing or title):
                        self._last_media_msg = time.monotonic()
                        self._no_media_shown = False
                        l1_text = self._parse_mode(self._l1_mode, data) if self._ctrl_l1 else ""
                        l2_text = self._parse_mode(self._l2_mode, data) if self._ctrl_l2 else ""
                        pending  = (l1_text, l2_text)
                        deadline = time.monotonic() + DEBOUNCE
                    else:
                        # Clear packet — drop liveness and any buffered update immediately
                        pending               = None
                        deadline              = None
                        self._last_media_msg  = 0.0
                        self._hudiy_connected = False
                        self._no_media_shown  = False

            except zmq.Again:
                zmq_err_count = 0
                if deadline is not None:
                    time.sleep(max(0, min(0.05, deadline - time.monotonic())))
                else:
                    time.sleep(0.05)
            except Exception:
                zmq_err_count += 1
                if zmq_err_count >= 3:
                    # Three consecutive real errors — socket likely dead, rebuild it
                    self._reconnect_zmq()
                    zmq_err_count = 0
                else:
                    time.sleep(0.05)

    def run(self):
        threading.Thread(target=self._metadata_listener, daemon=True).start()
        threading.Thread(target=self._watcher.run,       daemon=True).start()
        for ctrl in (self._ctrl_l1, self._ctrl_l2):
            if ctrl:
                threading.Thread(target=ctrl.run, daemon=True).start()

        while self.running:
            now = time.monotonic()

            if not self._no_media_shown \
                    and not self._hudiy_connected \
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