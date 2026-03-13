#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DIS top display — direct socketcan, optimized for hudiy_data V2.9.
All translation and CAN writes happen in-process.

Adaptive self-tuning system:

  OEMProfile (per line):
    Tracks OEM write timestamps, computes average write interval, derives
    the minimum tail coverage needed to always land a send before the next
    OEM write. Builds a custom tail schedule per line.

  AdaptiveSlam (per line):
    Ramps slam_count up on contention and bleeds it back down during calm.
    Text-aware ceiling: short static text caps lower, scrolling text higher.

Self-healing:
  1. Watchdog re-assert — silence on active TV line triggers re-push.
  2. CANWatcher immediate send — overwrites OEM content within microseconds.
  3. ZMQ socket self-heal — rebuilds on repeated exceptions.
  4. CAN send failure tracking — warns on repeated failures.

CANWatcher OEM detection (from old stable version):
  Uses only `data != last_sent` — no `data != snap` second condition.
  The snap condition caused missed re-slams: during scroll transitions,
  snapshot() advances to a new position while last_sent still holds the
  previous one. If OEM bytes happened to match the new snapshot position,
  the re-slam was silently skipped and OEM content stayed on the display.

Liveness (no heartbeat from V2.9):
  - _hudiy_connected derived from _last_media_msg age vs _MEDIA_TIMEOUT
  - Clear packet (source_id=0) drops liveness immediately
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

_TRANS = bytes(
    audscii_trans[i] if i < len(audscii_trans) else 0x20
    for i in range(256)
)

# Adaptive tuning bounds
MIN_SLAM         = 2
MAX_SLAM_SHORT   = 8
MAX_SLAM_LONG    = 16
SLAM_GAP         = 0.003
RAMP_UP_RATE     = 2
RAMP_DOWN_RATE   = 1
CALM_TICK        = 1.0

# OEM interval tracking
OEM_WINDOW       = 3.0
OEM_MIN_SAMPLES  = 3
TAIL_COVERAGE    = 1.5
MIN_TAIL_COVER   = 0.55
MAX_TAIL_COVER   = 2.0

# Watchdog
WATCHDOG_STALE   = 2.0

# CAN
CAN_FAIL_WARN    = 5


def nice_or_ignore():
    try:
        os.nice(-10)
    except Exception:
        pass


class OEMProfile:
    """Tracks OEM write timestamps and derives an adaptive tail schedule."""

    def __init__(self):
        self._times = []

    def record(self):
        now = time.monotonic()
        self._times.append(now)
        cutoff = now - OEM_WINDOW
        self._times = [t for t in self._times if t >= cutoff]

    def avg_interval(self):
        if len(self._times) < OEM_MIN_SAMPLES:
            return None
        gaps = [self._times[i+1] - self._times[i]
                for i in range(len(self._times) - 1)]
        return sum(gaps) / len(gaps)

    def tail_schedule(self):
        interval = self.avg_interval()
        if interval is None:
            return [0.03, 0.08, 0.15, 0.30, 0.55]

        cover = min(max(interval * TAIL_COVERAGE, MIN_TAIL_COVER), MAX_TAIL_COVER)
        schedule = []
        t    = interval * 0.4
        step = interval * 0.5
        while t <= cover:
            schedule.append(round(t, 3))
            step = min(step * 1.4, cover * 0.3)
            t   += step

        if not schedule or schedule[0] > 0.05:
            schedule.insert(0, 0.03)
        if schedule[-1] < cover * 0.9:
            schedule.append(round(cover, 3))

        return schedule


class AdaptiveSlam:
    """Smooth slam_count ramp — up on contention, down on calm. Text-aware ceiling."""

    def __init__(self):
        self._count          = MIN_SLAM
        self._last_calm_tick = time.monotonic()

    def on_reslam(self, is_long_text: bool):
        ceiling = MAX_SLAM_LONG if is_long_text else MAX_SLAM_SHORT
        self._count = min(self._count + RAMP_UP_RATE, ceiling)
        self._last_calm_tick = time.monotonic()

    def tick_calm(self, is_long_text: bool):
        now = time.monotonic()
        if (now - self._last_calm_tick) >= CALM_TICK:
            self._last_calm_tick = now
            self._count = max(self._count - RAMP_DOWN_RATE, MIN_SLAM)

    @property
    def count(self):
        return self._count


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

    def is_scrolling(self):
        with self.lock:
            return self.raw_len > self.width

    def tick(self, ctrl=None):
        """Advance scroll if due. UPDATES last_sent to prevent flash."""
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

            # --- THE FIX STARTS HERE ---
            if ctrl:
                # 1. Update the 'Authorized' memory immediately
                ctrl.last_sent = self.current_bytes 
                
                # 2. Queue the high-priority slam
                if ctrl._watcher and ctrl._watcher.tv_active:
                    ctrl._slam_q.put(self.current_bytes)
            # --- THE FIX ENDS HERE ---

    def _recompute(self):
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
    """One DIS display line with adaptive slam and tail behaviour."""

    def __init__(self, can_id, scroller, tx_bus, watcher=None, name=""):
        self.can_id         = can_id
        self.scroller       = scroller
        self.tx_bus         = tx_bus
        self._watcher       = watcher
        self._slam_q        = queue.Queue()
        self._tail_lock     = threading.Lock()
        self._tail_times    = []
        self.last_sent      = None   # ONLY set in run() after actual CAN send
        self.last_send_time = 0.0
        self.name           = name

        self.oem_profile   = OEMProfile()
        self.adaptive_slam = AdaptiveSlam()

        self._can_fail_count = 0

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

    def on_oem_write(self):
        """Called by CANWatcher on every detected OEM write."""
        self.oem_profile.record()
        self.adaptive_slam.on_reslam(self.scroller.is_scrolling())

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
            try:
                payload = self._slam_q.get(timeout=0.002)
                # Drain — only the latest payload fires
                try:
                    while True:
                        payload = self._slam_q.get_nowait()
                except queue.Empty:
                    pass
            except queue.Empty:
                payload = None

            now     = time.monotonic()
            tv      = self._watcher and self._watcher.tv_active
            is_long = self.scroller.is_scrolling()

            self.adaptive_slam.tick_calm(is_long)

            # Watchdog re-assert
            if tv and self.last_send_time > 0 \
                    and (now - self.last_send_time) >= WATCHDOG_STALE:
                if payload is None:
                    payload = self.scroller.snapshot()
                    logger.debug(f"[{self.name}] Watchdog re-assert fired")

            if payload is not None and tv:
                slam_count = self.adaptive_slam.count
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in range(slam_count):
                    if not self._send(msg):
                        break
                    time.sleep(SLAM_GAP)
                self.last_sent      = payload
                self.last_send_time = time.monotonic()

                tail_schedule = self.oem_profile.tail_schedule()
                with self._tail_lock:
                    self._tail_times = [now + dt for dt in tail_schedule]

            # Tail sends
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
                    self.last_sent      = payload
                    self.last_send_time = time.monotonic()


class CANWatcher:
    """RX thread — gates on TV source, triggers slams on OEM display overwrites.

    FIXED: Now uses (data != last_sent AND data != snap) to prevent flashing 
    on static text or during ZMQ baseline refreshes.
    """

    TV_SOURCE_BYTE = 0x37

    def __init__(self, bus, tx_bus, id_source, id_line1, id_line2,
                 ctrl_l1, ctrl_l2, dis_ctrl=None):
        self.bus       = bus
        self.tx_bus    = tx_bus
        self.id_source = id_source
        self.id_line1  = id_line1
        self.id_line2  = id_line2
        self.ctrl_l1   = ctrl_l1
        self.ctrl_l2   = ctrl_l2
        self.dis_ctrl  = dis_ctrl
        self.tv_active = False

    def _immediate_send(self, can_id, payload):
        """Fire one frame instantly — no queue, no delay."""
        try:
            self.tx_bus.send(can.Message(
                arbitration_id=can_id,
                data=payload,
                is_extended_id=False
            ))
        except Exception:
            pass

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

                elif self.tv_active:
                    # CHECK BOTH: Is it different from our last sent AND our current goal?
                    for ctrl, line_id in ((self.ctrl_l1, self.id_line1),
                                          (self.ctrl_l2, self.id_line2)):
                        if ctrl and cid == line_id:
                            # Get the current 'intended' bytes from the scroller
                            snap = ctrl.scroller.snapshot() 
                            
                            # If car data doesn't match our memory OR our current goal:
                            if data != ctrl.last_sent and data != snap:
                                # Overwrite immediately
                                self._immediate_send(line_id, snap)
                                ctrl.on_oem_write()
                                ctrl.trigger_slam(snap)
                            break

            except Exception:
                time.sleep(0.1)


class DISController:
    """Loads config, wires components, runs main loop."""

    _MEDIA_TIMEOUT = 8.0

    def __init__(self):
        nice_or_ignore()

        with open(CONFIG_PATH) as f:
            cfg = json.load(f)

        feat = cfg.get("features", {}).get("fis_display", {})
        if not feat.get("enabled", False):
            print("fis_display disabled; exiting.")
            sys.exit(0)

        self.running   = True
        self._cfg      = cfg
        self._zmq_addr = cfg["zmq"]["hudiy_publish_address"]
        self._zmq_ctx  = zmq.Context()
        self._sub      = self._make_zmq_sub()

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

        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))

        self._ctrl_l1 = LineController(
            can_id=id_line1,
            scroller=TextScroller(8, speed, pause, 0.0),
            tx_bus=tx_bus,
            name="L1",
        ) if l1_mode != "0" else None

        self._ctrl_l2 = LineController(
            can_id=id_line2,
            scroller=TextScroller(8, speed, pause, stagger),
            tx_bus=tx_bus,
            name="L2",
        ) if l2_mode != "0" else None

        self._watcher = CANWatcher(
            bus=rx_bus,
            tx_bus=tx_bus,
            id_source=id_source,
            id_line1=id_line1,
            id_line2=id_line2,
            ctrl_l1=self._ctrl_l1,
            ctrl_l2=self._ctrl_l2,
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

        self._boot_delay      = float(feat.get("boot_no_media_delay", 3.0))
        self._boot_time       = time.monotonic()
        self._last_media_msg  = 0.0
        self._hudiy_connected = False
        self._no_media_shown  = False

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _make_zmq_sub(self):
        sub = self._zmq_ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.RCVTIMEO, 1000)
        sub.connect(self._zmq_addr)
        sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")
        return sub

    def _reconnect_zmq(self):
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
            if not ctrl or not text:
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
        zmq_err_count = 0

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
                zmq_err_count = 0

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

            # UPDATE THESE TWO LINES RIGHT HERE:
            if self._ctrl_l1:
                self._ctrl_l1.scroller.tick(self._ctrl_l1) # Pass the controller
            if self._ctrl_l2:
                self._ctrl_l2.scroller.tick(self._ctrl_l2) # Pass the controller
                
            time.sleep(0.03)

        for bus in (self._rx_bus, self._tx_bus):
            try:
                bus.shutdown()
            except Exception:
                pass


if __name__ == "__main__":
    DISController().run()