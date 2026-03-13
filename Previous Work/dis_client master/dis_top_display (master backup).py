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
  When a song changes the scroller resets to pos 0 and the new text is
  slammed immediately — no blank phase, scroll just stops and restarts.

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
_BLANK_BYTE = audscii_trans[ord(" ")]  # identical to v0.9.6


# ---------------------------------------------------------------------------
# Tuning constants — all in one place for easy adjustment
# ---------------------------------------------------------------------------

# Adaptive slam
MIN_SLAM       = 4      # minimum slams per trigger
MAX_SLAM_SHORT = 8      # ceiling for static / short text
MAX_SLAM_LONG  = 16     # ceiling for scrolling text
SLAM_GAP       = 0.003  # seconds between slams
RAMP_UP        = 3      # slams added per OEM write event
RAMP_DOWN      = 1      # slams removed per calm tick
CALM_TICK      = 1.0    # seconds between calm ramp-down ticks

# OEM profile
OEM_WINDOW      = 3.0   # seconds of OEM write history to keep
OEM_MIN_SAMPLES = 3     # minimum samples before trusting interval
TAIL_COVERAGE   = 1.5   # cover (interval × this) with tails
MIN_TAIL_COVER  = 0.55  # minimum tail coverage (seconds)
MAX_TAIL_COVER  = 2.0   # maximum tail coverage (seconds)

# Watchdog
WATCHDOG_STALE = 2.0    # seconds of silence before re-assert

# Debounce
DEBOUNCE          = 0.18   # seconds — absorbs rapid song skips
NO_MEDIA_DEBOUNCE = 0.5    # seconds grace before showing No Media after clear

# Liveness
MEDIA_TIMEOUT = 8.0     # seconds before hudiy considered disconnected

# CAN
CAN_FAIL_WARN = 5       # consecutive failures before warning

# OEM heartbeat
OEM_HEARTBEAT_INTERVAL = 0.3   # re-assert every 300ms while OEM active
OEM_HEARTBEAT_WINDOW   = 5.0   # stay in heartbeat mode for 5s after last OEM write


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
        self._lock  = threading.Lock()

    def record(self) -> None:
        now = time.monotonic()
        with self._lock:
            self._times.append(now)
            cutoff = now - OEM_WINDOW
            self._times = [t for t in self._times if t >= cutoff]

    def _avg_interval(self) -> float | None:
        # caller must hold lock
        if len(self._times) < OEM_MIN_SAMPLES:
            return None
        gaps = [self._times[i + 1] - self._times[i]
                for i in range(len(self._times) - 1)]
        return sum(gaps) / len(gaps)

    def tail_schedule(self) -> list[float]:
        with self._lock:
            interval = self._avg_interval()
        if interval is None or interval < 0.01:
            return [0.03, 0.10, 0.25, 0.50]
        cover = min(max(interval * TAIL_COVERAGE, MIN_TAIL_COVER), MAX_TAIL_COVER)
        step = max(interval * 0.4, 0.05)
        schedule = []
        t = step
        while t <= cover:
            schedule.append(round(t, 3))
            t += step
        if not schedule or schedule[0] > 0.05:
            schedule.insert(0, 0.03)
        if not schedule or schedule[-1] < cover * 0.9:
            schedule.append(round(cover, 3))
        return schedule

    def clear(self) -> None:
        with self._lock:
            self._times = []


class AdaptiveSlam:
    """Per-line slam count — ramps up on OEM contention, bleeds down on calm.

    Text-aware ceiling: scrolling text gets a higher cap because there are
    more distinct payloads to protect. Short static text needs less coverage.
    """

    def __init__(self):
        self._count:      int   = MIN_SLAM
        self._last_calm:  float = time.monotonic()
        self._lock        = threading.Lock()

    def on_write(self, is_scrolling: bool) -> None:
        ceiling = MAX_SLAM_LONG if is_scrolling else MAX_SLAM_SHORT
        with self._lock:
            self._count     = min(self._count + RAMP_UP, ceiling)
            self._last_calm = time.monotonic()

    def tick_calm(self, is_scrolling: bool) -> None:
        now = time.monotonic()
        with self._lock:
            if now - self._last_calm >= CALM_TICK:
                self._last_calm = now
                self._count     = max(self._count - RAMP_DOWN, MIN_SLAM)

    def reset(self) -> None:
        with self._lock:
            self._count     = MIN_SLAM
            self._last_calm = time.monotonic()

    @property
    def count(self) -> int:
        with self._lock:
            return self._count


class TextScroller:
    """Scrolls an 8-character window over arbitrary-length text.

    scroll_type="oem"     — bounce. start→end→start, pauses at each end.
                            Factory display behaviour.
    scroll_type="marquee" — continuous left-wrap. No reverse, no pause.
                            Text repeats with marquee_gap blank chars between.

    set_text() always resets pos to 0 so every new song starts from the
    beginning. clear() and set_text() are the only public mutators.
    tick() is called from the main loop and queues (payload, reset_tails=False)
    so scroll-position advances never reset the OEM tail schedule.
    """

    def __init__(self, width: int = 8, speed: float = 0.35,
                 pause: float = 2.0, stagger: float = 0.0,
                 scroll_type: str = "oem", marquee_gap: int = 2):
        self.width        = width
        self.scroll_speed = speed
        self.pause_time   = pause
        self.scroll_type  = scroll_type.lower()
        self.marquee_gap  = max(0, int(marquee_gap))
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
        # Both modes pause at start — marquee then wraps continuously, OEM bounces
        self.wait_timer = now + self.pause_time
        if self.scroll_type == "marquee":
            gap_len          = self.marquee_gap + 1
            txt              = bytes(
                0x65 if c == " " else audscii_trans[ord(c)] & 0xFF
                for c in text
            )
            gap              = bytes([0x65] * gap_len)
            self._stream     = (txt + gap) if text else gap
            self._stream_len = max(1, len(self._stream))
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

    def restart(self) -> None:
        """Reset scroll position to 0 and restart pause — text unchanged.
        Called on TV deactivation so re-activation always starts cleanly."""
        with self.lock:
            if self.raw_len <= self.width:
                return
            self.pos        = 0
            now             = time.monotonic()
            self.last_tick  = now - self.scroll_speed
            self.wait_timer = now + self.pause_time
            if self.scroll_type == "marquee":
                # Rebuild stream in case it was mid-loop
                gap_len          = self.marquee_gap + 1
                txt              = bytes(
                    0x65 if c == " " else audscii_trans[ord(c)] & 0xFF
                    for c in self.raw_text
                )
                self._stream     = txt + bytes([0x65] * gap_len)
                self._stream_len = max(1, len(self._stream))
            self._recompute()

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
            if now - self.last_tick <= self.scroll_speed:
                return

            if self.scroll_type == "marquee":
                if now < self.wait_timer:
                    return
                self.pos = (self.pos + 1) % self._stream_len
            else:
                if now < self.wait_timer:
                    return
                max_pos  = self.raw_len - self.width
                self.pos = self.pos + 1 if self.pos < max_pos else 0
                if self.pos == 0 or self.pos == max_pos:
                    self.wait_timer = now + self.pause_time

            self._recompute()
            self.last_tick = now
            new_bytes = self.current_bytes

        # Queue outside scroller lock — avoids nested lock ordering issues
        if ctrl and ctrl._watcher and ctrl._watcher.tv_active:
            ctrl._slam_q.put((new_bytes, False))

    def _recompute(self) -> None:
        """Rebuild current_bytes from raw_text at pos. Lock must be held."""
        if self.raw_len <= self.width:
            txt   = bytes(_TRANS[ord(c) & 0xFF] for c in self.raw_text)
            pad   = self.width - self.raw_len
            left  = pad // 2
            right = pad - left
            blank = _BLANK_BYTE
            self.current_bytes  = (bytes([blank] * left)
                                   + txt
                                   + bytes([blank] * right))
            self.current_window = self.raw_text.center(self.width)
        elif self.scroll_type == "marquee":
            self.current_bytes  = bytes(
                self._stream[(self.pos + i) % self._stream_len]
                for i in range(self.width)
            )
            self.current_window = self.current_bytes.decode("latin-1")
        else:
            window              = self.raw_text[self.pos:self.pos + self.width]
            self.current_window = window.ljust(self.width)
            txt                 = bytes(_TRANS[ord(c) & 0xFF] for c in window)
            self.current_bytes  = txt + bytes([_BLANK_BYTE] * (self.width - len(txt)))


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
        self.last_oem_write: float             = 0.0
        self.name            = name
        self._sent_cache:     dict[bytes, float] = {}
        self._sent_cache_lock = threading.Lock()
        self._SENT_TTL        = 2.0
        self.oem_profile     = OEMProfile()
        self.adaptive_slam   = AdaptiveSlam()
        self._can_fail_count = 0

    # --- queue helpers ---

    def trigger_slam(self, payload: bytes) -> None:
        self._slam_q.put((payload, True))

    def push_text(self) -> None:
        self._slam_q.put((self.scroller.snapshot(), True))

    def clear_queue(self) -> None:
        try:
            while True:
                self._slam_q.get_nowait()
        except queue.Empty:
            pass

    def on_oem_write(self, is_scrolling: bool) -> None:
        self.last_oem_write = time.monotonic()
        self.oem_profile.record()
        self.adaptive_slam.on_write(is_scrolling)

    def reset_adaptive(self) -> None:
        self.oem_profile.clear()
        self.adaptive_slam.reset()
        self.last_oem_write = 0.0

    # --- CAN send ---

    def _register_sent(self, payload: bytes) -> None:
        now = time.monotonic()
        with self._sent_cache_lock:
            self._sent_cache[payload] = now
            if len(self._sent_cache) > 64:
                cutoff = now - self._SENT_TTL
                self._sent_cache = {k: v for k, v in self._sent_cache.items()
                                    if v >= cutoff}

    def _send(self, msg: can.Message) -> bool:
        try:
            with self._tx_lock:
                self.tx_bus.send(msg)
            self._register_sent(bytes(msg.data))
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
        next_heartbeat: float = 0.0

        while True:
            try:
                payload, reset_tails = self._slam_q.get(timeout=0.002)
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

            # Watchdog
            if tv and self.last_send_time > 0 \
                    and (now - self.last_send_time) >= WATCHDOG_STALE:
                if payload is None:
                    payload     = self.scroller.snapshot()
                    reset_tails = True
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

                if reset_tails:
                    new_tails = [slam_done + dt
                                 for dt in self.oem_profile.tail_schedule()]
                    with self._tail_lock:
                        self._tail_times = new_tails

            # Tail sends
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

            # Heartbeat
            if tv:
                now = time.monotonic()
                oem_active = (now - self.last_oem_write) < OEM_HEARTBEAT_WINDOW
                if oem_active and now >= next_heartbeat:
                    snap = self.scroller.snapshot()
                    msg  = can.Message(arbitration_id=self.can_id,
                                       data=snap, is_extended_id=False)
                    self._send(msg)
                    self.last_sent      = snap
                    self.last_send_time = time.monotonic()
                    next_heartbeat      = now + OEM_HEARTBEAT_INTERVAL


class CANWatcher:
    """RX thread — monitors TV source byte and OEM display writes."""

    TV_SOURCE_BYTE  = 0x37
    OEM_COOLDOWN    = 0.05

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
        self._last_oem_time: dict[int, float] = {}

    def _isend(self, msg: can.Message) -> None:
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
                                ctrl.last_sent      = ctrl.scroller.snapshot()
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
                                ctrl.scroller.restart()
                                ctrl.reset_adaptive()

                # --- OEM write detection ---
                elif self.tv_active:
                    for ctrl, line_id in ((self.ctrl_l1, self.id_line1),
                                          (self.ctrl_l2, self.id_line2)):
                        if ctrl and cid == line_id:
                            snap, is_scrolling = ctrl.scroller.snapshot_info()
                            if data != snap:
                                now = time.monotonic()
                                with ctrl._sent_cache_lock:
                                    ts = ctrl._sent_cache.get(data)
                                    is_echo = ts is not None and (now - ts) < ctrl._SENT_TTL
                                if is_echo:
                                    logger.debug(
                                        "[%s] TX echo ignored — rx=%s",
                                        ctrl.name, data.hex(),
                                    )
                                    break

                                last = self._last_oem_time.get(line_id, 0.0)
                                in_cooldown = (now - last) < self.OEM_COOLDOWN

                                # If OEM is writing after a long gap the menu
                                # just cycled back to this line — restart scroll
                                # so it always reappears from the beginning.
                                if (now - last) > 2.0:
                                    ctrl.scroller.restart()
                                    snap = ctrl.scroller.snapshot()

                                logger.warning(
                                    "[%s] OEM write — rx=%s snap=%s "
                                    "scrolling=%s cooldown=%s",
                                    ctrl.name, data.hex(), snap.hex(),
                                    is_scrolling, in_cooldown,
                                )

                                ctrl._register_sent(snap)
                                ctrl.last_sent = snap

                                imsg = can.Message(arbitration_id=line_id,
                                                   data=snap,
                                                   is_extended_id=False)
                                self._isend(imsg)
                                self._isend(imsg)
                                self._isend(imsg)
                                self._isend(imsg)
                                self._isend(imsg)

                                if not in_cooldown:
                                    self._last_oem_time[line_id] = now
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

        zmq_addr       = cfg["zmq"]["hudiy_publish_address"]
        self._zmq_ctx  = zmq.Context()
        self._sub      = self._make_sub(zmq_addr)
        self._zmq_addr = zmq_addr

        can_ids   = cfg.get("can_ids", {})
        id_line1  = int(can_ids.get("fis_line1", "0x363"), 16)
        id_line2  = int(can_ids.get("fis_line2", "0x365"), 16)
        id_source = int(can_ids.get("source",    "0x661"), 16)

        iface  = cfg.get("can_interface", "can0")
        rx_bus = can.Bus(interface="socketcan", channel=iface,
                         receive_own_messages=False)
        tx_bus = can.Bus(interface="socketcan", channel=iface)

        _speed_raw  = float(feat.get("scroll_speed", 5))
        _speed_raw  = max(1.0, min(10.0, _speed_raw))
        speed       = round(0.7 - (_speed_raw - 1) * (0.6 / 9), 3)
        pause       = float(feat.get("scroll_pause",   2.0))
        stagger     = float(feat.get("stagger_offset", 1.0))
        scroll_type = str(feat.get("scroll_type",      "oem")).lower()
        marquee_gap = int(feat.get("marquee_gap",      0))

        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))

        tx_lock = threading.Lock()

        self._ctrl_l1 = LineController(
            can_id=id_line1,
            scroller=TextScroller(8, speed, pause, 0.0,
                                  scroll_type=scroll_type,
                                  marquee_gap=marquee_gap),
            tx_bus=tx_bus,
            name="L1",
            tx_lock=tx_lock,
        ) if l1_mode != "0" else None

        self._ctrl_l2 = LineController(
            can_id=id_line2,
            scroller=TextScroller(8, speed, pause, stagger,
                                  scroll_type=scroll_type,
                                  marquee_gap=marquee_gap),
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

        for ctrl, text in ((self._ctrl_l1, self.NO_MEDIA_L1),
                           (self._ctrl_l2, self.NO_MEDIA_L2)):
            if ctrl and text:
                ctrl.scroller.set_text(text)

        self._boot_delay      = float(feat.get("boot_no_media_delay", 3.0))
        self._boot_time       = time.monotonic()
        self._last_media_msg  = 0.0
        self._hudiy_connected = False
        self._no_media_shown  = False
        self._no_media_grace  = 0.0   # deadline before No Media is allowed to show

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

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

    def _shutdown(self, *_) -> None:
        self.running = False

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
            if text:
                changed = ctrl.scroller.set_text(text)
            else:
                changed = ctrl.scroller.clear()
            if not changed:
                continue
            if self._watcher.tv_active:
                ctrl.clear_queue()
                ctrl.trigger_slam(ctrl.scroller.snapshot())

    def _metadata_listener(self) -> None:
        pending:       tuple | None = None
        deadline:      float | None = None
        zmq_err_count: int          = 0

        while self.running:
            now = time.monotonic()

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
                        self._last_media_msg  = time.monotonic()
                        self._no_media_shown  = False
                        self._no_media_grace  = 0.0  # real source arrived — cancel grace
                        l1 = self._parse_mode(self._l1_mode, data) \
                             if self._ctrl_l1 else ""
                        l2 = self._parse_mode(self._l2_mode, data) \
                             if self._ctrl_l2 else ""
                        new_pending = (l1, l2)
                        if new_pending != pending:
                            pending  = new_pending
                            deadline = time.monotonic() + DEBOUNCE
                    else:
                        pending              = None
                        deadline             = None
                        self._last_media_msg = 0.0
                        self._no_media_shown = False
                        # Grace period: don't show No Media until source has
                        # been absent for a full NO_MEDIA_DEBOUNCE interval.
                        # This prevents No Media flashing during rapid source
                        # switches where source_id briefly hits 0 between tracks.
                        self._no_media_grace = time.monotonic() + NO_MEDIA_DEBOUNCE

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

            if not self._no_media_shown and not self._hudiy_connected \
                    and (now - self._boot_time) >= self._boot_delay \
                    and now >= self._no_media_grace:
                self._show_no_media()

            for ctrl in (self._ctrl_l1, self._ctrl_l2):
                if ctrl:
                    ctrl.scroller.tick(ctrl)

            next_tick = min(
                (ctrl.scroller.last_tick + ctrl.scroller.scroll_speed
                 for ctrl in (self._ctrl_l1, self._ctrl_l2) if ctrl),
                default=time.monotonic() + 0.03,
            )
            sleep_for = max(0.002, next_tick - time.monotonic())
            time.sleep(sleep_for)

        for bus in (self._rx_bus, self._tx_bus):
            try:
                bus.shutdown()
            except Exception:
                pass


if __name__ == "__main__":
    DISController().run()