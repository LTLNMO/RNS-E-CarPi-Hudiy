#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DIS top display — direct socketcan, optimized for hudiy_data V2.9.

Goal: behave like a factory cluster information display.
Text updates promptly on song change. Scrolling is smooth and continuous.
OEM display writes are suppressed immediately. Everything is stable and quiet.

Architecture:
  TextScroller   — 8-char scrolling window. set_text() resets to pos 0 so
                   every new song starts from the beginning with a full pause.
  LineController — per-line CAN TX thread. Adaptive slam count and tail
                   schedule tune themselves to observed OEM write cadence.
                   Slam queue carries (payload, reset_tails) so scroll-position
                   ticks never blow away OEM-response tail schedules.
  CANWatcher     — RX thread. Gates on TV source byte. Fires 3 immediate
                   CAN sends the instant an OEM write is detected, then queues
                   full adaptive slam for sustained coverage.
  DISController  — orchestrator. ZMQ metadata listener with debounce that
                   only resets on genuine content changes (not repeat packets).

Text transitions:
  When a song changes mid-scroll, slam_with_blank() sends 2 blank frames
  directly before queuing the new content slam. The blank is imperceptible
  but completely hides the positional jump of the old scrolling text.

Adaptive tuning (per line, independent):
  OEMProfile    — tracks OEM write timestamps, derives adaptive tail schedule
                  so tails always cover the OEM's observed write interval.
  AdaptiveSlam  — ramps slam count up on OEM contention, bleeds down on calm.
                  Text-aware ceiling: scrolling text gets higher ceiling.
                  Fully reset on TV source deactivation.

Self-healing:
  - Watchdog re-assert if line goes silent while TV active.
  - ZMQ socket rebuilt on 3 consecutive non-timeout exceptions.
  - CAN send failure logged after 5 consecutive failures.

Liveness (no heartbeat from V2.9):
  - _hudiy_connected derived from _last_media_msg age vs MEDIA_TIMEOUT.
  - Clear packet (source_id=0) drops liveness immediately.
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
    format="%(asctime)s [%(levelname)s] (DIS) %(message)s",
)
logger = logging.getLogger(__name__)

try:
    from icons import audscii_trans
except Exception:
    print("ERROR: icons.py not found or failed to import.")
    sys.exit(1)

# Pre-built 256-entry translation table for fast character mapping
_TRANS = bytes(
    audscii_trans[i] if i < len(audscii_trans) else 0x20
    for i in range(256)
)
_BLANK_BYTE = _TRANS[0x20]  # translated space character


# ---------------------------------------------------------------------------
# Tuning constants — all in one place for easy adjustment
# ---------------------------------------------------------------------------

# Adaptive slam
MIN_SLAM       = 2      # minimum slams per trigger
MAX_SLAM_SHORT = 8      # ceiling for static / short text
MAX_SLAM_LONG  = 16     # ceiling for scrolling text
SLAM_GAP       = 0.003  # seconds between slams
RAMP_UP        = 2      # slams added per OEM write event
RAMP_DOWN      = 1      # slams removed per calm tick
CALM_TICK      = 1.0    # seconds between calm ramp-down ticks

# OEM profile
OEM_WINDOW      = 3.0   # seconds of OEM write history to keep
OEM_MIN_SAMPLES = 3     # minimum samples before trusting interval
TAIL_COVERAGE   = 1.5   # cover (interval × this) with tails
MIN_TAIL_COVER  = 0.55  # minimum tail coverage (seconds)
MAX_TAIL_COVER  = 2.0   # maximum tail coverage (seconds)

# Blank transition
BLANK_TRANSITION = 0.08  # seconds of blank between songs — masks scroll jump

# Watchdog
WATCHDOG_STALE = 2.0    # seconds of silence before re-assert

# Debounce
DEBOUNCE = 0.18         # seconds — absorbs rapid song skips

# Liveness
MEDIA_TIMEOUT = 8.0     # seconds before hudiy considered disconnected

# CAN
CAN_FAIL_WARN = 5       # consecutive failures before warning


# ---------------------------------------------------------------------------

def _nice() -> None:
    try:
        os.nice(-10)
    except Exception:
        pass


class OEMProfile:
    """Tracks OEM write timestamps per line and derives an adaptive tail schedule.

    Learns the OEM's write cadence so tails always land before the next
    expected OEM write. Falls back to a safe default until enough data
    accumulates or if the observed interval is implausibly fast.
    """

    def __init__(self):
        self._times: list[float] = []

    def record(self) -> None:
        now = time.monotonic()
        self._times.append(now)
        cutoff = now - OEM_WINDOW
        self._times = [t for t in self._times if t >= cutoff]

    def _avg_interval(self) -> float | None:
        if len(self._times) < OEM_MIN_SAMPLES:
            return None
        gaps = [self._times[i + 1] - self._times[i]
                for i in range(len(self._times) - 1)]
        return sum(gaps) / len(gaps)

    def tail_schedule(self) -> list[float]:
        interval = self._avg_interval()
        if interval is None or interval < 0.01:
            return [0.03, 0.08, 0.15, 0.30, 0.55]

        cover = min(max(interval * TAIL_COVERAGE, MIN_TAIL_COVER), MAX_TAIL_COVER)
        schedule: list[float] = []
        t    = interval * 0.4
        step = max(interval * 0.5, 0.02)   # floor prevents degenerate loop
        while t <= cover:
            schedule.append(round(t, 3))
            step = min(step * 1.4, cover * 0.3)
            t   += step

        if not schedule or schedule[0] > 0.05:
            schedule.insert(0, 0.03)
        if schedule[-1] < cover * 0.9:
            schedule.append(round(cover, 3))

        return schedule

    def clear(self) -> None:
        self._times = []


class AdaptiveSlam:
    """Per-line slam count — ramps up on OEM contention, bleeds down on calm.

    Text-aware ceiling: scrolling text gets a higher cap because there are
    more distinct payloads to protect. Short static text needs less coverage.
    """

    def __init__(self):
        self._count:      int   = MIN_SLAM
        self._last_calm:  float = time.monotonic()

    def on_write(self, is_scrolling: bool) -> None:
        ceiling         = MAX_SLAM_LONG if is_scrolling else MAX_SLAM_SHORT
        self._count     = min(self._count + RAMP_UP, ceiling)
        self._last_calm = time.monotonic()

    def tick_calm(self, is_scrolling: bool) -> None:
        now = time.monotonic()
        if now - self._last_calm >= CALM_TICK:
            self._last_calm = now
            self._count     = max(self._count - RAMP_DOWN, MIN_SLAM)

    def reset(self) -> None:
        self._count     = MIN_SLAM
        self._last_calm = time.monotonic()

    @property
    def count(self) -> int:
        return self._count


class TextScroller:
    """Scrolls an 8-character window over arbitrary-length text.

    set_text() always resets pos to 0 and restarts the pause timer so every
    new song starts from the beginning — just like a factory display.

    clear() and set_text() are the only public mutators. Both hold the lock.
    snapshot() and snapshot_info() are safe to call from any thread.
    tick() is called from the main loop and queues (payload, reset_tails=False)
    so scroll-position advances never reset the OEM tail schedule.
    """

    def __init__(self, width: int = 8, speed: float = 0.35,
                 pause: float = 2.0, stagger: float = 0.0):
        self.width        = width
        self.scroll_speed = speed
        self.pause_time   = pause
        self.lock         = threading.Lock()
        self._reset("")
        # Stagger applied once at init to offset L2 from L1 at startup
        if stagger:
            self.wait_timer += stagger

    def _reset(self, text: str) -> None:
        """Full reset. Must be called with lock held (or before lock exists)."""
        self.raw_text   = text
        self.raw_len    = len(text)
        self.pos        = 0
        now             = time.monotonic()
        self.last_tick  = now - self.scroll_speed
        self.wait_timer = now + self.pause_time
        self._recompute()

    def set_text(self, text: str) -> bool:
        """Set new text. Resets pos to 0 and restarts pause timer.
        Returns True if content changed."""
        text = (text or "").strip()
        with self.lock:
            if text == self.raw_text:
                return False
            self._reset(text)
            return True

    def clear(self) -> bool:
        """Clear to blank. Returns True if content changed."""
        with self.lock:
            if not self.raw_text:
                return False
            self._reset("")
            return True

    def is_scrolling(self) -> bool:
        with self.lock:
            return self.raw_len > self.width

    def is_mid_scroll(self) -> bool:
        """True only when actively scrolling between the two pause points.
        False when text fits, or when paused at start (pos=0) or end (pos=max)."""
        with self.lock:
            if self.raw_len <= self.width:
                return False
            max_pos = self.raw_len - self.width
            return 0 < self.pos < max_pos

    def snapshot(self) -> bytes:
        with self.lock:
            return self.current_bytes

    def snapshot_info(self) -> tuple[bytes, bool]:
        """Return (current_bytes, is_scrolling) under a single lock acquire."""
        with self.lock:
            return self.current_bytes, self.raw_len > self.width

    def tick(self, ctrl: "LineController | None" = None) -> None:
        """Advance scroll position if due.
        Queues (payload, reset_tails=False) — scroll ticks must not reset
        OEM-response tail schedules."""
        now = time.monotonic()
        with self.lock:
            if self.raw_len <= self.width:
                return
            if now < self.wait_timer:
                return
            if now - self.last_tick <= self.scroll_speed:
                return
            max_pos   = self.raw_len - self.width
            self.pos  = self.pos + 1 if self.pos < max_pos else 0
            if self.pos == 0 or self.pos == max_pos:
                self.wait_timer = now + self.pause_time
            self._recompute()
            self.last_tick = now
            if ctrl and ctrl._watcher and ctrl._watcher.tv_active:
                ctrl._slam_q.put((self.current_bytes, False))

    def _recompute(self) -> None:
        """Rebuild current_bytes from raw_text at pos. Lock must be held."""
        if self.raw_len <= self.width:
            window = self.raw_text.center(self.width)
        else:
            window = self.raw_text[self.pos:self.pos + self.width].ljust(self.width)
        self.current_window = window
        self.current_bytes  = bytes(_TRANS[ord(c) & 0xFF] for c in window)


class LineController:
    """Owns one DIS display line: TX queue, adaptive slam, tail schedule.

    Slam queue carries (payload: bytes, reset_tails: bool).
      reset_tails=True  — OEM/content/watchdog slam: rebuild tail schedule.
      reset_tails=False — scroll-position tick: leave tails untouched.

    During queue drain, reset_tails flags are OR'd so a True from an OEM
    slam is never lost to a trailing scroll-position tick.
    """

    def __init__(self, can_id: int, scroller: TextScroller,
                 tx_bus: can.Bus, name: str = "", tx_lock: threading.Lock | None = None):
        self.can_id          = can_id
        self.scroller        = scroller
        self.tx_bus          = tx_bus
        self._tx_lock        = tx_lock or threading.Lock()
        self._watcher: "CANWatcher | None" = None
        self._slam_q:        queue.Queue       = queue.Queue()
        self._tail_lock:     threading.Lock    = threading.Lock()
        self._tail_times:    list[float]       = []
        self.last_sent:      bytes | None      = None
        self.last_send_time: float             = 0.0
        self.name            = name
        self.oem_profile     = OEMProfile()
        self.adaptive_slam   = AdaptiveSlam()
        self._can_fail_count = 0

    # --- queue helpers ---

    def trigger_slam(self, payload: bytes) -> None:
        """Queue a content slam — will reset tail schedule."""
        self._slam_q.put((payload, True))

    def push_text(self) -> None:
        """Queue current scroller text as a content slam."""
        self._slam_q.put((self.scroller.snapshot(), True))

    def slam_with_blank(self, payload: bytes) -> None:
        """Send a brief blank burst directly to the bus, then queue the slam.

        The blank is sent synchronously from the calling thread (metadata
        listener) so the TX thread stays fully responsive throughout —
        OEM writes are handled normally during the blank window.
        80ms is enough to break visual continuity without looking deliberate."""
        blank = bytes([_BLANK_BYTE] * self.scroller.width)
        bmsg  = can.Message(arbitration_id=self.can_id,
                            data=blank, is_extended_id=False)
        deadline = time.monotonic() + BLANK_TRANSITION
        while time.monotonic() < deadline:
            try:
                with self._tx_lock:
                    self.tx_bus.send(bmsg)
                    self.tx_bus.send(bmsg)
                    self.tx_bus.send(bmsg)
            except Exception:
                break
            time.sleep(0.015)
        with self._tail_lock:
            self._tail_times = []
        self._slam_q.put((payload, True))

    def clear_queue(self) -> None:
        try:
            while True:
                self._slam_q.get_nowait()
        except queue.Empty:
            pass

    def on_oem_write(self, is_scrolling: bool) -> None:
        self.oem_profile.record()
        self.adaptive_slam.on_write(is_scrolling)

    def reset_adaptive(self) -> None:
        """Called on TV deactivation — start next session fresh."""
        self.oem_profile.clear()
        self.adaptive_slam.reset()

    # --- CAN send ---

    def _send(self, msg: can.Message) -> bool:
        try:
            with self._tx_lock:
                self.tx_bus.send(msg)
            self._can_fail_count = 0
            return True
        except Exception:
            self._can_fail_count += 1
            if self._can_fail_count == CAN_FAIL_WARN:
                logger.warning("[%s] %d consecutive CAN send failures",
                               self.name, CAN_FAIL_WARN)
            return False

    # --- TX thread ---

    def run(self) -> None:
        due: list[float] = []

        while True:
            # Block up to 2ms waiting for a slam entry
            try:
                payload, reset_tails = self._slam_q.get(timeout=0.002)
                # Drain — keep latest payload; OR reset_tails so a True from
                # an OEM slam is never lost to a trailing tick False
                try:
                    while True:
                        p, rt        = self._slam_q.get_nowait()
                        payload      = p
                        reset_tails  = reset_tails or rt
                except queue.Empty:
                    pass
            except queue.Empty:
                payload, reset_tails = None, False

            now          = time.monotonic()
            tv           = self._watcher and self._watcher.tv_active
            is_scrolling = self.scroller.is_scrolling()

            self.adaptive_slam.tick_calm(is_scrolling)

            # Watchdog — re-push if the line has gone silent
            if tv and self.last_send_time > 0 \
                    and (now - self.last_send_time) >= WATCHDOG_STALE:
                if payload is None:
                    payload      = self.scroller.snapshot()
                    reset_tails  = True
                    logger.debug("[%s] Watchdog re-assert", self.name)

            # Slam
            if payload is not None and tv:
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                for _ in range(self.adaptive_slam.count):
                    if not self._send(msg):
                        break
                    time.sleep(SLAM_GAP)
                self.last_sent      = payload
                slam_done           = time.monotonic()
                self.last_send_time = slam_done

                # Only content/OEM slams rebuild the tail schedule.
                # Scroll-position ticks (reset_tails=False) leave it alone
                # so OEM-response tails survive across scroll steps.
                if reset_tails:
                    new_tails = [slam_done + dt
                                 for dt in self.oem_profile.tail_schedule()]
                    with self._tail_lock:
                        self._tail_times = new_tails

            # Tail sends — check and partition fully under lock to avoid TOCTOU
            due.clear()
            if tv:
                now = time.monotonic()
                with self._tail_lock:
                    if self._tail_times:
                        remaining = []
                        for t in self._tail_times:
                            (due if now >= t else remaining).append(t)
                        self._tail_times = remaining

            if due:
                snap = self.scroller.snapshot()
                msg  = can.Message(arbitration_id=self.can_id,
                                   data=snap, is_extended_id=False)
                for _ in due:
                    self._send(msg)
                self.last_sent      = snap
                self.last_send_time = time.monotonic()


class CANWatcher:
    """RX thread — monitors TV source byte and OEM display writes.

    On OEM write detected:
      1. Fires 3 immediate direct CAN sends — overwrites OEM content before
         the queue drain delay (2ms) and slam startup (~6-9ms) elapse.
      2. Updates last_sent immediately so the next OEM write comparison
         is accurate.
      3. Queues full adaptive slam (reset_tails=True) for burst + tail coverage.

    OEM detection: data != last_sent only. The previous `data != snap`
    secondary condition caused missed re-slams during scroll transitions
    and has been permanently removed.
    """

    TV_SOURCE_BYTE = 0x37

    def __init__(self, rx_bus: can.Bus, tx_bus: can.Bus,
                 id_source: int, id_line1: int, id_line2: int,
                 ctrl_l1: "LineController | None",
                 ctrl_l2: "LineController | None",
                 dis_ctrl: "DISController | None" = None,
                 tx_lock: threading.Lock | None = None):
        self.rx_bus    = rx_bus
        self.tx_bus    = tx_bus
        self._tx_lock  = tx_lock or threading.Lock()
        self.id_source = id_source
        self.id_line1  = id_line1
        self.id_line2  = id_line2
        self.ctrl_l1   = ctrl_l1
        self.ctrl_l2   = ctrl_l2
        self.dis_ctrl  = dis_ctrl
        self.tv_active = False

    def _isend(self, msg: can.Message) -> None:
        """Immediate direct send — bypasses queue entirely."""
        try:
            with self._tx_lock:
                self.tx_bus.send(msg)
        except Exception as e:
            logger.debug("CANWatcher immediate send failed: %s", e)

    def run(self) -> None:
        while True:
            try:
                msg = self.rx_bus.recv(timeout=0.5)
                if not msg or msg.is_extended_id:
                    continue

                cid  = msg.arbitration_id
                data = bytes(msg.data)

                # --- Source gate ---
                if cid == self.id_source and len(data) >= 4:
                    was_active     = self.tv_active
                    self.tv_active = (data[3] == self.TV_SOURCE_BYTE)

                    if self.tv_active and not was_active:
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
                        for ctrl in (self.ctrl_l1, self.ctrl_l2):
                            if ctrl:
                                ctrl.reset_adaptive()

                # --- OEM write detection ---
                elif self.tv_active:
                    for ctrl, line_id in ((self.ctrl_l1, self.id_line1),
                                          (self.ctrl_l2, self.id_line2)):
                        if ctrl and cid == line_id:
                            if data != ctrl.last_sent:
                                snap, is_scrolling = ctrl.scroller.snapshot_info()
                                # One Message object, 3 immediate sends —
                                # closes the gap before the slam thread wakes
                                imsg = can.Message(arbitration_id=line_id,
                                                   data=snap,
                                                   is_extended_id=False)
                                self._isend(imsg)
                                self._isend(imsg)
                                self._isend(imsg)
                                ctrl.last_sent = snap
                                ctrl.on_oem_write(is_scrolling)
                                ctrl.trigger_slam(snap)
                            break

            except Exception as e:
                logger.warning("CANWatcher exception: %s", e)
                time.sleep(0.1)


class DISController:
    """Orchestrator — loads config, wires components, runs main loop."""

    def __init__(self):
        _nice()

        with open(CONFIG_PATH) as f:
            cfg = json.load(f)

        feat = cfg.get("features", {}).get("fis_display", {})
        if not feat.get("enabled", False):
            print("fis_display disabled; exiting.")
            sys.exit(0)

        self.running = True

        # ZMQ
        zmq_addr       = cfg["zmq"]["hudiy_publish_address"]
        self._zmq_ctx  = zmq.Context()
        self._sub      = self._make_sub(zmq_addr)
        self._zmq_addr = zmq_addr

        # CAN IDs
        can_ids   = cfg.get("can_ids", {})
        id_line1  = int(can_ids.get("fis_line1", "0x363"), 16)
        id_line2  = int(can_ids.get("fis_line2", "0x365"), 16)
        id_source = int(can_ids.get("source",    "0x661"), 16)

        # CAN buses (separate rx/tx so RX never blocks TX)
        iface  = cfg.get("can_interface", "can0")
        rx_bus = can.Bus(interface="socketcan", channel=iface)
        tx_bus = can.Bus(interface="socketcan", channel=iface)

        # Config
        speed   = float(feat.get("scroll_speed",   0.35))
        pause   = float(feat.get("scroll_pause",   2.0))
        stagger = float(feat.get("stagger_offset", 1.0))

        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))

        # Shared TX lock — all threads that write to tx_bus must hold this.
        # Prevents blank burst (metadata thread), slam (TX thread), and
        # immediate sends (CANWatcher RX thread) from interleaving on the bus.
        tx_lock = threading.Lock()

        self._ctrl_l1 = LineController(
            can_id=id_line1,
            scroller=TextScroller(8, speed, pause, 0.0),
            tx_bus=tx_bus,
            name="L1",
            tx_lock=tx_lock,
        ) if l1_mode != "0" else None

        self._ctrl_l2 = LineController(
            can_id=id_line2,
            scroller=TextScroller(8, speed, pause, stagger),
            tx_bus=tx_bus,
            name="L2",
            tx_lock=tx_lock,
        ) if l2_mode != "0" else None

        self._watcher = CANWatcher(
            rx_bus=rx_bus, tx_bus=tx_bus,
            id_source=id_source, id_line1=id_line1, id_line2=id_line2,
            ctrl_l1=self._ctrl_l1, ctrl_l2=self._ctrl_l2,
            dis_ctrl=self,
            tx_lock=tx_lock,
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

        # Set initial scroller content
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

    # --- ZMQ ---

    def _make_sub(self, addr: str) -> zmq.Socket:
        sub = self._zmq_ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.RCVTIMEO, 1000)
        sub.connect(addr)
        sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")
        return sub

    def _reconnect_zmq(self) -> None:
        logger.warning("ZMQ socket error — reconnecting")
        try:
            self._sub.close()
        except Exception:
            pass
        time.sleep(1.0)
        self._sub = self._make_sub(self._zmq_addr)
        logger.info("ZMQ reconnected")

    # --- Lifecycle ---

    def _shutdown(self, *_) -> None:
        self.running = False

    # --- Display helpers ---

    def _show_no_media(self) -> None:
        for ctrl, text in ((self._ctrl_l1, self.NO_MEDIA_L1),
                           (self._ctrl_l2, self.NO_MEDIA_L2)):
            if not ctrl:
                continue
            if text:
                ctrl.scroller.set_text(text)
            else:
                ctrl.scroller.clear()
            snap = ctrl.scroller.snapshot()
            # No blank transition here — this is initial state, not a song
            # change. Text should appear immediately.
            with ctrl._tail_lock:
                ctrl._tail_times = []
            ctrl.trigger_slam(snap)
        self._no_media_shown = True

    def _parse_mode(self, mode_str: str, p: dict) -> str:
        def clean(v) -> str:
            s = (v or "").strip()
            return s if any(c.isalnum() for c in s) else ""

        fields = {
            "1": clean(p.get("title") or p.get("track")),
            "2": clean(p.get("artist")),
            "3": clean(p.get("album")),
            "9": clean(p.get("source_label") or p.get("source")),
        }
        keys  = [k.strip() for k in mode_str.split("-")] if "-" in mode_str \
                else [mode_str]
        parts = [fields[k] for k in keys if k in fields and fields[k]]
        return " - ".join(parts) if len(parts) >= 2 else (parts[0] if parts else "")

    def _apply_metadata(self, l1_text: str, l2_text: str) -> None:
        for ctrl, text in ((self._ctrl_l1, l1_text),
                           (self._ctrl_l2, l2_text)):
            if not ctrl:
                continue
            was_mid_scroll = ctrl.scroller.is_mid_scroll()
            if text:
                changed = ctrl.scroller.set_text(text)
            else:
                changed = ctrl.scroller.clear()
            if not changed:
                continue
            if self._watcher.tv_active:
                # Flush stale scroll-tick payloads before the transition
                ctrl.clear_queue()
                snap = ctrl.scroller.snapshot()
                if was_mid_scroll:
                    # Was mid-scroll — blank the line first so the old
                    # scroll position doesn't flash before new text appears
                    ctrl.slam_with_blank(snap)
                else:
                    # Was at a pause point or static — no jump possible,
                    # go straight to new content
                    ctrl.trigger_slam(snap)

    # --- Metadata listener thread ---

    def _metadata_listener(self) -> None:
        pending:       tuple | None = None
        deadline:      float | None = None
        zmq_err_count: int          = 0

        while self.running:
            now = time.monotonic()

            # Fire pending update when debounce expires
            if pending is not None and now >= deadline:
                self._apply_metadata(*pending)
                pending = deadline = None

            self._hudiy_connected = (
                self._last_media_msg > 0 and
                (now - self._last_media_msg) < MEDIA_TIMEOUT
            )

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
                        l1 = self._parse_mode(self._l1_mode, data) \
                             if self._ctrl_l1 else ""
                        l2 = self._parse_mode(self._l2_mode, data) \
                             if self._ctrl_l2 else ""
                        new_pending = (l1, l2)
                        # Only reset debounce deadline when content changes.
                        # Repeat packets (position updates etc.) let it fire.
                        if new_pending != pending:
                            pending  = new_pending
                            deadline = time.monotonic() + DEBOUNCE
                    else:
                        # Clear packet or no source — drop liveness immediately
                        pending              = None
                        deadline             = None
                        self._last_media_msg = 0.0
                        self._no_media_shown = False

            except zmq.Again:
                zmq_err_count = 0
                sleep_for = max(0.0, min(0.05, deadline - time.monotonic())) \
                            if deadline is not None else 0.05
                time.sleep(sleep_for)

            except Exception as e:
                logger.warning("ZMQ exception: %s", e)
                zmq_err_count += 1
                if zmq_err_count >= 3:
                    self._reconnect_zmq()
                    zmq_err_count = 0
                else:
                    time.sleep(0.05)

    # --- Main run ---

    def run(self) -> None:
        threading.Thread(target=self._metadata_listener,
                         daemon=True, name="meta").start()
        threading.Thread(target=self._watcher.run,
                         daemon=True, name="rx").start()
        for ctrl in (self._ctrl_l1, self._ctrl_l2):
            if ctrl:
                threading.Thread(target=ctrl.run,
                                 daemon=True, name=f"tx-{ctrl.name}").start()

        while self.running:
            now = time.monotonic()

            # Show No Media after boot delay if hudiy not connected
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