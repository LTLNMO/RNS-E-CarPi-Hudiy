#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DIS top display — socketcan, hudiy_data V2.9.

Priority (high→low): Phone call > Media > No Media.

Line mode keys:
  media:  title, artist, album, source       dash-joined: "title-artist"
  phone:  caller, state, name, connection, battery, signal

Config (fis_display):
  line1_mode, line2_mode             "0" = line disabled
  phone_line1_mode, phone_line2_mode
  line1_scroll_speed, line2_scroll_speed  0=off  1=slow … 10=fast
  scroll_pause, stagger_offset, scroll_type, marquee_gap
  no_media_line1, no_media_line2, boot_no_media_delay
"""

import bisect, json, logging, os, queue, signal, sys, threading, time
import can, zmq

CONFIG_PATH = "/home/pi/config.json"
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] (DIS) %(message)s")
logger = logging.getLogger(__name__)
_fh = logging.FileHandler('/tmp/dis_top.log')
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] (DIS) %(message)s"))
logger.addHandler(_fh)

try:
    from icons import audscii_trans
except Exception:
    sys.exit("ERROR: icons.py not found or failed to import.")

_TRANS = bytes(audscii_trans[i] if i < len(audscii_trans) else 0x20 for i in range(256))
_BLANK = audscii_trans[ord(" ")]

# ── Tuning ───────────────────────────────────────────────────────────────────
MIN_SLAM, MAX_SLAM_SHORT, MAX_SLAM_LONG = 4, 8, 16
SLAM_GAP        = 0.003
RAMP_UP         = 3
RAMP_DOWN       = 1
CALM_TICK       = 1.0
OEM_WINDOW      = 3.0
OEM_MIN_SAMPLES = 3
TAIL_COVERAGE   = 1.5
MIN_TAIL_COVER  = 0.55
MAX_TAIL_COVER  = 2.0
WATCHDOG_STALE  = 2.0
DEBOUNCE             = 0.18
SOURCE_CHANGE_DEBOUNCE = 1.0  # absorb brief src=0 gap during BT→CarPlay handoff
CAN_FAIL_WARN   = 5
HB_INTERVAL     = 0.3
HB_WINDOW       = 5.0
SENT_TTL        = 2.0
OEM_COOLDOWN    = 0.05
OEM_RESTART_GAP = 2.0   # seconds gap in OEM writes before restarting scroll
OEM_RESPONSE    = 5     # immediate response sends when OEM write detected

CALL_ACTIVE = {"INCOMING", "ALERTING", "ACTIVE"}
CALL_LABELS = {"INCOMING": "Incoming", "ALERTING": "Calling",
               "ACTIVE": "Active", "IDLE": "Idle"}

# ─────────────────────────────────────────────────────────────────────────────


def _nice():
    try: os.nice(-10)
    except Exception: pass


class LineController:
    """
    One DIS line end-to-end: scroll, TX, adaptive slam, tails, watchdog, heartbeat.
    Runs its own thread. External interface: set_text / snapshot / snapshot_info /
    trigger_slam / push_text / clear_queue / on_oem_write / reset_adaptive / restart.
    """
    W = 8  # display width in characters

    def __init__(self, can_id, tx_bus, tx_lock, name,
                 speed, pause, stagger, scroll_type, marquee_gap, no_scroll):
        self.can_id    = can_id
        self._tx_bus   = tx_bus
        self._tx_lock  = tx_lock
        self.name      = name
        self._watcher  = None  # injected after construction

        # Scroll config
        self._speed   = speed
        self._pause   = pause
        self._stagger = stagger
        self._stype   = scroll_type
        self._mgap    = max(0, int(marquee_gap))
        self.no_scroll = no_scroll

        # Scroll + display state (protected by self._lock)
        self._lock       = threading.Lock()
        self._raw        = ""
        self._raw_len    = 0
        self._pos        = 0
        self._stream     = b""
        self._stream_len = 1
        self._cur        = bytes([_BLANK] * self.W)
        now              = time.monotonic()
        self._last_tick  = now - speed
        self._wait_timer = now + pause + stagger

        # TX
        self._slam_q     = queue.Queue()
        self._tail_lock  = threading.Lock()
        self._tail_times: list[float] = []
        self.last_send_t = 0.0
        self.last_oem_t  = 0.0

        # Sent-echo cache
        self._sent_cache: dict[bytes, float] = {}
        self._cache_lock = threading.Lock()
        self._fail_count = 0

        # Adaptive slam — OEM write history inlined
        self._oem_t:     list[float] = []
        self._oem_lock   = threading.Lock()
        self._slam_lock  = threading.Lock()
        self._slam_count = MIN_SLAM
        self._calm_t     = now

        self._build_stream(self._raw)
        self._recompute()

    # ── public text interface ────────────────────────────────────────────────

    def set_text(self, text: str) -> bool:
        text = (text or "").strip()
        with self._lock:
            if text == self._raw: return False
            self._reset(text); return True

    def snapshot(self) -> bytes:
        with self._lock: return self._cur

    def snapshot_info(self) -> tuple[bytes, bool]:
        with self._lock:
            return self._cur, (not self.no_scroll) and self._raw_len > self.W

    def restart(self):
        with self._lock:
            if self.no_scroll or self._raw_len <= self.W: return
            self._pos        = 0
            now              = time.monotonic()
            self._last_tick  = now - self._speed
            self._wait_timer = now + self._pause + self._stagger
            self._recompute()

    # ── TX queue ─────────────────────────────────────────────────────────────

    def trigger_slam(self, payload: bytes):
        self._slam_q.put((payload, True))

    def push_text(self):
        self._slam_q.put((self.snapshot(), True))

    def clear_queue(self):
        try:
            while True: self._slam_q.get_nowait()
        except queue.Empty:
            pass

    # ── adaptive ─────────────────────────────────────────────────────────────

    def on_oem_write(self, scrolling: bool):
        now = time.monotonic()
        self.last_oem_t = now
        with self._oem_lock:
            self._oem_t.append(now)
            idx = bisect.bisect_left(self._oem_t, now - OEM_WINDOW)
            if idx: del self._oem_t[:idx]
        ceiling = MAX_SLAM_LONG if scrolling else MAX_SLAM_SHORT
        with self._slam_lock:
            self._slam_count = min(self._slam_count + RAMP_UP, ceiling)
            self._calm_t     = now

    def reset_adaptive(self):
        with self._oem_lock: self._oem_t.clear()
        self.last_oem_t = 0.0
        with self._slam_lock:
            self._slam_count = MIN_SLAM
            self._calm_t     = time.monotonic()

    # ── TX thread ────────────────────────────────────────────────────────────

    def run(self):
        due: list[float] = []
        next_hb = 0.0

        while True:
            # Drain slam queue
            payload, reset_tails = None, False
            try:
                payload, reset_tails = self._slam_q.get(timeout=0.002)
                try:
                    while True:
                        p, rt       = self._slam_q.get_nowait()
                        payload     = p
                        reset_tails = reset_tails or rt
                except queue.Empty:
                    pass
            except queue.Empty:
                pass

            now = time.monotonic()
            tv  = self._watcher.tv_active

            # Calm ramp-down
            with self._slam_lock:
                if now - self._calm_t >= CALM_TICK:
                    self._calm_t     = now
                    self._slam_count = max(self._slam_count - RAMP_DOWN, MIN_SLAM)

            # Scroll tick — self-contained, no main loop needed
            if tv and not self.no_scroll:
                advanced, new_bytes = self._tick(now)
                if advanced:
                    self._slam_q.put((new_bytes, False))

            # Watchdog
            if tv and self.last_send_t > 0 and (now - self.last_send_t) >= WATCHDOG_STALE:
                if payload is None:
                    payload, reset_tails = self.snapshot(), True
                    logger.debug("[%s] watchdog re-assert", self.name)

            # Slam
            if payload is not None and tv:
                msg = can.Message(arbitration_id=self.can_id,
                                  data=payload, is_extended_id=False)
                with self._slam_lock: count = self._slam_count
                for _ in range(count):
                    if not self._send(msg, now): break
                    time.sleep(SLAM_GAP)
                done             = time.monotonic()
                self.last_send_t = done
                if reset_tails:
                    with self._oem_lock:
                        t = list(self._oem_t)
                    if len(t) < OEM_MIN_SAMPLES:
                        sched = [0.03, 0.10, 0.25, 0.50]
                    else:
                        gaps = [b - a for a, b in zip(t, t[1:])]
                        interval = sum(gaps) / len(gaps)
                        if interval < 0.01:
                            sched = [0.03, 0.10, 0.25, 0.50]
                        else:
                            cover = min(max(interval * TAIL_COVERAGE, MIN_TAIL_COVER), MAX_TAIL_COVER)
                            step  = max(interval * 0.4, 0.05)
                            sched, v = [], step
                            while v <= cover:
                                sched.append(round(v, 3)); v += step
                            if not sched or sched[0] > 0.05:  sched.insert(0, 0.03)
                            if not sched or sched[-1] < cover * 0.9: sched.append(round(cover, 3))
                    with self._tail_lock:
                        self._tail_times = [done + dt for dt in sched]

            # Tails
            due.clear()
            if tv:
                with self._tail_lock:
                    if self._tail_times:
                        now = time.monotonic()
                        remaining = []
                        for t in self._tail_times:
                            (due if now >= t else remaining).append(t)
                        self._tail_times = remaining
            if due:
                now  = time.monotonic()
                snap = self.snapshot()
                msg  = can.Message(arbitration_id=self.can_id,
                                   data=snap, is_extended_id=False)
                for _ in due: self._send(msg, now)
                self.last_send_t = now

            # Heartbeat
            now = time.monotonic()
            if tv and (now - self.last_oem_t) < HB_WINDOW and now >= next_hb:
                snap = self.snapshot()
                msg  = can.Message(arbitration_id=self.can_id,
                                   data=snap, is_extended_id=False)
                self._send(msg, now)
                self.last_send_t = now
                next_hb          = now + HB_INTERVAL

    # ── scroll internals ─────────────────────────────────────────────────────

    def _reset(self, text: str):
        """Full reset. Must be called with self._lock held."""
        self._raw        = text
        self._raw_len    = len(text)
        self._pos        = 0
        now              = time.monotonic()
        self._last_tick  = now - self._speed
        self._wait_timer = now + self._pause + self._stagger
        self._build_stream(text)
        self._recompute()

    def _build_stream(self, text: str):
        """Rebuild marquee stream. Must hold self._lock or be at init."""
        if self._stype == "marquee":
            gap              = bytes([0x65] * (self._mgap + 1))
            txt              = bytes(0x65 if c == " " else audscii_trans[ord(c) & 0xFF] & 0xFF
                                     for c in text)
            self._stream     = (txt + gap) if text else gap
            self._stream_len = max(1, len(self._stream))

    def _tick(self, now: float) -> tuple[bool, bytes]:
        """Advance scroll position if due. Returns (advanced, new_bytes)."""
        with self._lock:
            if (self._raw_len <= self.W
                    or (now - self._last_tick) <= self._speed
                    or now < self._wait_timer):
                return False, b""
            if self._stype == "marquee":
                self._pos = (self._pos + 1) % self._stream_len
            else:
                max_pos   = self._raw_len - self.W
                self._pos = self._pos + 1 if self._pos < max_pos else 0
                if self._pos == 0 or self._pos == max_pos:
                    self._wait_timer = now + self._pause
            self._recompute()
            self._last_tick = now
            return True, self._cur

    def _recompute(self):
        """Rebuild _cur from current scroll state. Must hold self._lock."""
        if self._raw_len <= self.W:
            txt        = bytes(_TRANS[ord(c) & 0xFF] for c in self._raw)
            pad        = self.W - self._raw_len
            left       = pad // 2
            self._cur  = bytes([_BLANK]*left) + txt + bytes([_BLANK]*(pad-left))
        elif self._stype == "marquee":
            self._cur  = bytes(self._stream[(self._pos+i) % self._stream_len]
                               for i in range(self.W))
        else:
            w          = self._raw[self._pos:self._pos + self.W]
            txt        = bytes(_TRANS[ord(c) & 0xFF] for c in w)
            self._cur  = txt + bytes([_BLANK] * (self.W - len(txt)))

    # ── CAN send ─────────────────────────────────────────────────────────────

    def _reg_sent(self, payload: bytes, now: float):
        with self._cache_lock:
            self._sent_cache[payload] = now
            if len(self._sent_cache) > 64:
                cutoff = now - SENT_TTL
                self._sent_cache = {k: v for k, v in self._sent_cache.items()
                                    if v >= cutoff}

    def _send(self, msg: can.Message, now: float) -> bool:
        try:
            with self._tx_lock: self._tx_bus.send(msg)
            self._reg_sent(bytes(msg.data), now)
            self._fail_count = 0
            return True
        except Exception:
            self._fail_count += 1
            if self._fail_count == CAN_FAIL_WARN:
                logger.warning("[%s] %d consecutive CAN send failures",
                               self.name, CAN_FAIL_WARN)
            return False


class CANWatcher:
    """RX thread — gates on TV source byte, suppresses OEM display writes."""

    def __init__(self, rx_bus, tx_bus, tx_lock,
                 id_source, id_line1, id_line2,
                 ctrl_l1, ctrl_l2, dis_ctrl, tv_byte=0x37):
        self._rx       = rx_bus
        self._tx       = tx_bus
        self._txlock   = tx_lock
        self._id_src   = id_source
        self._lines    = {k: v for k, v in {id_line1: ctrl_l1, id_line2: ctrl_l2}.items() if v}
        self._dis      = dis_ctrl
        self.TV_BYTE   = tv_byte
        self.tv_active = False
        self._last_oem: dict[int, float] = {}

    def run(self):
        while True:
            try:
                msg = self._rx.recv(timeout=0.5)
                if not msg or msg.is_extended_id:
                    continue
                cid  = msg.arbitration_id
                data = bytes(msg.data)

                # Source gate
                if cid == self._id_src and len(data) >= 4:
                    was            = self.tv_active
                    self.tv_active = data[3] == self.TV_BYTE

                    if self.tv_active and not was:
                        logger.info("TV source activated")
                        self._dis._media_last_t = 0.0
                        for ctrl in self._lines.values():
                            ctrl.last_send_t = time.monotonic()
                            ctrl.clear_queue()
                            with ctrl._tail_lock: ctrl._tail_times = []
                            ctrl.push_text()

                    elif not self.tv_active and was:
                        logger.info("TV source deactivated")
                        for ctrl in self._lines.values():
                            ctrl.restart()
                            ctrl.reset_adaptive()

                # OEM write detection
                elif self.tv_active and cid in self._lines:
                    ctrl = self._lines[cid]
                    snap, scrolling = ctrl.snapshot_info()
                    if data == snap: continue

                    with ctrl._cache_lock:
                        ts = ctrl._sent_cache.get(data)
                    now = time.monotonic()
                    if ts is not None and (now - ts) < SENT_TTL:
                        logger.debug("[%s] TX echo ignored", ctrl.name)
                        continue

                    last = self._last_oem.get(cid, 0.0)
                    if (now - last) > OEM_RESTART_GAP:
                        ctrl.restart()
                        snap = ctrl.snapshot()

                    logger.debug("[%s] OEM write rx=%s snap=%s",
                                   ctrl.name, data.hex(), snap.hex())
                    ctrl._reg_sent(snap, now)
                    imsg = can.Message(arbitration_id=cid, data=snap,
                                       is_extended_id=False)
                    for _ in range(OEM_RESPONSE):
                        try:
                            with self._txlock: self._tx.send(imsg)
                        except Exception as e:
                            logger.debug("CANWatcher send failed: %s", e)
                            break

                    if (now - last) >= OEM_COOLDOWN:
                        self._last_oem[cid] = now
                        ctrl.on_oem_write(scrolling)
                        ctrl.trigger_slam(snap)

            except Exception as e:
                logger.warning("CANWatcher: %s", e)
                time.sleep(0.1)


class DISController:
    """Orchestrator — config, wiring, ZMQ listener, priority resolution."""

    def __init__(self):
        _nice()
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)

        feat = cfg.get("features", {}).get("fis_display", {})
        if not feat.get("enabled", False):
            sys.exit("fis_display disabled.")

        self.running = True

        interfaces = cfg.get("interfaces", {})
        can_cfg = interfaces.get("can", {})
        zmq_cfg = interfaces.get("zmq", {})
        input_maps = cfg.get("input_mappings", {})
        source_cfg = input_maps.get("source", {})

        zmq_addr = zmq_cfg.get("metric_stream", "ipc:///run/rnse_control/hudiy_stream.ipc")
        self._zmq_ctx = zmq.Context()
        self._sub = self._make_sub(zmq_addr)
        self._zmq_addr = zmq_addr

        def _hex(val, default):
            if isinstance(val, int):
                return val
            try:
                return int(val, 16)
            except (ValueError, TypeError):
                return default

        can_ids = cfg.get("can_ids", {})
        id_l1 = _hex(can_ids.get("fis_line1", "0x363"), 0x363)
        id_l2 = _hex(can_ids.get("fis_line2", "0x365"), 0x365)
        id_src = _hex(can_ids.get("source", "0x661"), 0x661)

        iface = can_cfg.get("infotainment", "can0")
        rx_bus = can.Bus(interface="socketcan", channel=iface,
                         receive_own_messages=False)
        tx_bus = can.Bus(interface="socketcan", channel=iface)
        tx_lock = threading.Lock()

        def speed_cfg(raw):
            v = max(0.0, min(10.0, float(raw)))
            if v == 0:
                return (0.35, True)

            min_cps = float(feat.get("scroll_min_cps", 1.25))
            max_cps = float(feat.get("scroll_max_cps", 8.0))

            if max_cps < min_cps:
                max_cps = min_cps

            cps = min_cps + (v - 1.0) * ((max_cps - min_cps) / 9.0)
            return (round(1.0 / cps, 3), False)

        pause = float(feat.get("scroll_pause", 2.0))
        stagger = float(feat.get("stagger_offset", 1.0))
        stype = str(feat.get("scroll_type", "oem")).lower()
        mgap = int(feat.get("marquee_gap", 0))

        def make_ctrl(can_id, name, raw_speed, stag, mode):
            if mode == "0":
                return None
            spd, noscr = speed_cfg(raw_speed)
            return LineController(can_id, tx_bus, tx_lock, name,
                                  spd, pause, stag, stype, mgap, noscr)

        l1_mode = str(feat.get("line1_mode", "0"))
        l2_mode = str(feat.get("line2_mode", "0"))
        raw_l1 = feat.get("line1_scroll_speed", feat.get("scroll_speed", 5))
        raw_l2 = feat.get("line2_scroll_speed", feat.get("scroll_speed", 5))

        self._ctrl_l1 = make_ctrl(id_l1, "L1", raw_l1, 0.0, l1_mode)
        self._ctrl_l2 = make_ctrl(id_l2, "L2", raw_l2, stagger, l2_mode)
        self._ctrls = [c for c in (self._ctrl_l1, self._ctrl_l2) if c]

        _tv_raw = source_cfg.get("tv_mode_identifier", "0x37")
        tv_byte = _hex(_tv_raw, 0x37)

        self._watcher = CANWatcher(rx_bus, tx_bus, tx_lock, id_src,
                                   id_l1, id_l2,
                                   self._ctrl_l1, self._ctrl_l2, self,
                                   tv_byte=tv_byte)
        for c in self._ctrls:
            c._watcher = self._watcher

        self._l1_mode    = l1_mode
        self._l2_mode    = l2_mode
        self._ph_l1_mode = str(feat.get("phone_line1_mode", "caller"))
        self._ph_l2_mode = str(feat.get("phone_line2_mode", "state"))
        self._no_media   = (feat.get("no_media_line1", "No Media"),
                            feat.get("no_media_line2", ""))
        self._boot_delay = float(feat.get("boot_no_media_delay", 3.0))
        self._boot_time  = time.monotonic()
        self._rx_bus     = rx_bus
        self._tx_bus     = tx_bus

        # Media state
        self._media_active    = False
        self._media_last_t    = 0.0     # time media last went inactive (for No Media gate)
        self._media_texts     = ("", "")
        self._last_good_texts = ("", "")
        self._call_active     = False
        self._no_media_pushed = False

        for ctrl, text in ((self._ctrl_l1, self._no_media[0]),
                           (self._ctrl_l2, self._no_media[1])):
            if ctrl and text: ctrl.set_text(text)

        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    # ── ZMQ ──────────────────────────────────────────────────────────────────

    def _make_sub(self, addr):
        sub = self._zmq_ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.RCVTIMEO, 1000)
        sub.connect(addr)
        for t in (b"HUDIY_MEDIA", b"HUDIY_PHONE"):
            sub.setsockopt(zmq.SUBSCRIBE, t)
        return sub

    def _reconnect_zmq(self):
        logger.warning("ZMQ reconnecting")
        try: self._sub.close()
        except Exception: pass
        time.sleep(1.0)
        self._sub = self._make_sub(self._zmq_addr)

    def _shutdown(self, *_):
        self.running = False

    # ── display ───────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_mode(mode_str: str, fields: dict) -> str:
        def clean(v):
            s = str(v).strip() if v is not None else ""
            return s if any(c.isalnum() for c in s) else ""
        keys  = [k.strip() for k in mode_str.split("-")]
        parts = [v for k in keys if (v := clean(fields.get(k, "")))]
        return " - ".join(parts) if len(parts) >= 2 else (parts[0] if parts else "")

    def _to_lines(self, fields: dict, m1: str, m2: str) -> tuple[str, str]:
        return (self._parse_mode(m1, fields) if self._ctrl_l1 else "",
                self._parse_mode(m2, fields) if self._ctrl_l2 else "")

    def _media_fields(self, d: dict) -> tuple[str, str]:
        f = {"title":  d.get("title") or d.get("track", ""),
             "artist": d.get("artist", ""),
             "album":  d.get("album", ""),
             "source": d.get("source_label", "")}
        return self._to_lines(f, self._l1_mode, self._l2_mode)

    def _phone_fields(self, d: dict) -> tuple[str, str]:
        conn = d.get("connection_state", "")
        f = {"caller":     d.get("caller_name") or d.get("caller_id") or "Call",
             "state":      CALL_LABELS.get(d.get("state", ""), d.get("state", "")),
             "name":       d.get("name", ""),
             "connection": "Connected" if conn == "CONNECTED" else
                           "Disconnected" if conn == "DISCONNECTED" else conn,
             "battery":    str(d.get("battery") or ""),
             "signal":     str(d.get("signal")  or "")}
        return self._to_lines(f, self._ph_l1_mode, self._ph_l2_mode)

    def _push(self, l1: str, l2: str):
        """Push text to both lines and slam if TV is active."""
        for ctrl, text in ((self._ctrl_l1, l1), (self._ctrl_l2, l2)):
            if not ctrl: continue
            if ctrl.set_text(text) and self._watcher.tv_active:
                ctrl.clear_queue()
                ctrl.trigger_slam(ctrl.snapshot())

    def _resolve(self):
        """Re-push the correct content for current state."""
        if self._media_active:
            self._push(*self._media_texts)
        else:
            self._push(*self._no_media)
            self._no_media_pushed = True

    # ── listener ─────────────────────────────────────────────────────────────

    def _listener(self):
        pending      = None
        deadline     = None
        pending_src  = 0
        err_count    = 0

        while self.running:
            now = time.monotonic()

            # Fire debounced media update
            if pending and now >= deadline:
                l1, l2 = pending
                # Hold last known good if title clears briefly during track gap
                if self._media_active:
                    if not l1: l1 = self._last_good_texts[0]
                    if not l2: l2 = self._last_good_texts[1]
                resolved = (l1, l2)
                self._media_texts = resolved
                if any(resolved): self._last_good_texts = resolved
                if not self._call_active:
                    self._push(*resolved)
                pending = deadline = None

            # No Media — only after boot delay and once media has gone inactive
            if (not self._call_active
                    and not self._media_active
                    and not self._no_media_pushed
                    and (now - self._boot_time) >= self._boot_delay):
                self._push(*self._no_media)
                self._no_media_pushed = True
                logger.info("No Media")

            try:
                parts = self._sub.recv_multipart(flags=zmq.NOBLOCK)
                if len(parts) < 2:
                    continue
                topic, data = parts[0], json.loads(parts[1])
                err_count = 0

                if topic == b"HUDIY_MEDIA":
                    src   = data.get("source_id", 0)
                    state = data.get("media_state", "NONE")

                    if state in ("PLAYING", "PAUSED", "IDLE"):
                        self._media_active    = True
                        self._no_media_pushed = False
                        new = self._media_fields(data)
                        if new != pending:
                            logger.info("MEDIA %s src=%s — lines: %r", state, src, new)
                            db          = SOURCE_CHANGE_DEBOUNCE if src != pending_src else DEBOUNCE
                            pending     = new
                            pending_src = src
                            deadline    = now + db
                    else:
                        # NONE — hudiy explicitly says nothing connected
                        if self._media_active or pending:
                            logger.info("MEDIA NONE — no source")
                        self._media_active = False
                        self._media_last_t = now
                        pending            = None
                        deadline           = None
                        pending_src        = 0

                elif topic == b"HUDIY_PHONE":
                    state = data.get("state", "IDLE")
                    was   = self._call_active
                    self._call_active = state in CALL_ACTIVE

                    if self._call_active:
                        if not was:
                            logger.info("Call started (%s) — %s", state,
                                        {k: v for k, v in data.items()
                                         if k in ("state", "caller_name", "caller_id",
                                                   "name", "connection_state", "battery", "signal")})
                        self._push(*self._phone_fields(data))
                    elif was:
                        logger.info("Call ended — restoring display")
                        if pending:
                            self._media_texts     = pending
                            self._last_good_texts = pending
                            pending = deadline = pending_src = None
                        self._resolve()

            except zmq.Again:
                _now  = time.monotonic()
                sleep = max(0.0, min(0.05, deadline - _now)) if deadline else 0.05
                time.sleep(sleep)
            except Exception as e:
                logger.warning("ZMQ: %s", e)
                err_count += 1
                if err_count >= 3:
                    self._reconnect_zmq()
                    err_count = 0
                else:
                    time.sleep(0.05)

    # ── run ───────────────────────────────────────────────────────────────────

    def run(self):
        threading.Thread(target=self._listener,    daemon=True, name="meta").start()
        threading.Thread(target=self._watcher.run, daemon=True, name="rx").start()
        for c in self._ctrls:
            threading.Thread(target=c.run, daemon=True, name=f"tx-{c.name}").start()

        try:
            while self.running:
                time.sleep(0.5)
        except KeyboardInterrupt:
            self.running = False

        for bus in (self._rx_bus, self._tx_bus):
            try: bus.shutdown()
            except Exception: pass


if __name__ == "__main__":
    DISController().run()
