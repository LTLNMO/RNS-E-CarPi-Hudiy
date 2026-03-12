#!/usr/bin/env python3
"""
CAN Detective -- server.py
Reads every frame on the bus. No ID filtering.
Labels known IDs from config.json.
Decodes with DBC when available (latin-1 encoded, German signal names kept as-is).
Writes current_state.json every N seconds.
Run: python server.py
"""
import can, json, time, os, threading, sys
from collections import defaultdict, deque
from datetime import datetime

BASE  = os.path.dirname(os.path.abspath(__file__))
CFG   = json.load(open(os.path.join(BASE, "config.json")))

CHANNEL      = CFG["can_interface"]
DBC_FILE     = CFG.get("dbc_file", "")
DBC_ENC      = CFG.get("dbc_encoding", "latin-1")
CAPTURE_FILE = CFG["capture_file"]
STATE_FILE   = CFG["state_file"]
INTERVAL     = CFG.get("state_interval", 2)
HISTORY      = CFG.get("frame_history", 100)

os.makedirs(os.path.dirname(CAPTURE_FILE), exist_ok=True)

# Label map: int_id -> name
LABELS = {}
for name, val in CFG.get("can_ids", {}).items():
    try:
        LABELS[int(val, 16) if isinstance(val, str) else int(val)] = name
    except Exception:
        pass

# TV-protocol frame IDs we flag specially
def _id(key, default):
    try:
        return int(CFG["can_ids"].get(key, default), 16)
    except Exception:
        return int(default, 16)

TV_IDS = {
    _id("tv_presence",     "0x602"),
    _id("tv_nwm",          "0x437"),
    _id("tv_tp_to_tuner_1","0x4F8"),
    _id("tv_tp_to_tuner_2","0x4F9"),
    _id("tv_bap",          "0x6D3"),
    _id("tv_iso_req",      "0x76D"),
    _id("tv_iso_resp",     "0x7D7"),
    _id("source",          "0x661"),
    _id("fis_line1",       "0x363"),
    _id("fis_line2",       "0x365"),
}

SOURCE_ID  = _id("source", "0x661")
SOURCE_HEX = f"{SOURCE_ID:03X}"
SOURCE_MODES = {int(k, 16): v for k, v in CFG.get("source_modes", {}).items()}
current_source: dict = {"byte": None, "name": "unknown"}

# Load DBC with latin-1 encoding
db = None
if DBC_FILE and os.path.exists(DBC_FILE):
    try:
        import cantools
        raw = open(DBC_FILE, encoding=DBC_ENC, errors="replace").read()
        db  = cantools.database.load_string(raw)
        tv_msgs = [m for m in db.messages if "TV" in m.name.upper()]
        print(f"[server] DBC loaded: {len(db.messages)} messages")
        for m in tv_msgs[:5]:
            print(f"  [{m.frame_id:03X}] {m.name}  sigs={[s.name for s in m.signals[:4]]}")
    except Exception as e:
        print(f"[server] DBC load failed: {e} -- raw mode")
        db = None
else:
    print("[server] No DBC configured -- raw mode")

state = defaultdict(lambda: {
    "frames": deque(maxlen=HISTORY), "decoded": deque(maxlen=HISTORY),
    "count": 0, "first_seen": None
})
lock = threading.Lock()

def safe_val(v):
    if isinstance(v, (int, float, bool, str)): return v
    if isinstance(v, bytes): return v.hex()
    try: return float(v)
    except Exception: return str(v)

def save_state_loop():
    while True:
        time.sleep(INTERVAL)
        snap = {}
        with lock:
            for hex_id, s in state.items():
                frames = list(s["frames"])
                if not frames: continue
                ts_list = [f["ts"] for f in frames]
                span    = (ts_list[-1] - ts_list[0]) / 1000 if len(ts_list) >= 2 else 0
                freq    = round((len(ts_list)-1)/span, 2) if span > 0 else 0
                latest  = frames[-1]["data"]
                recent  = [f["data"] for f in frames[-10:]]
                changing = [i for i in range(min(len(d) for d in recent))
                            if len(set(d[i] for d in recent if i < len(d))) > 1] if len(recent) >= 2 else []
                int_id  = int(hex_id, 16)
                snap[hex_id] = {
                    "label":          LABELS.get(int_id, ""),
                    "is_tv_related":  int_id in TV_IDS,
                    "count":          s["count"],
                    "freq_hz":        freq,
                    "dlc":            frames[-1]["len"],
                    "latest_hex":     bytes(latest).hex(),
                    "bytes":          latest,
                    "ascii_hint":     "".join(chr(b) if 32<=b<127 else "." for b in latest),
                    "changing_bytes": changing,
                    "payload_stable": len(set(bytes(f["data"]).hex() for f in frames[-10:])) == 1,
                    "has_text":       any(32<=b<127 for b in latest),
                    "decoded":        list(s["decoded"])[-1] if s["decoded"] else None,
                    "first_seen_ms":  s["first_seen"],
                }
        src = current_source.copy()
        with open(STATE_FILE, "w") as f:
            json.dump({"updated": datetime.now().isoformat(),
                       "total_ids": len(snap),
                       "total_frames": sum(v["count"] for v in snap.values()),
                       "tv_ids_seen": [h for h,v in snap.items() if v["is_tv_related"]],
                       "current_source": src,
                       "ids": snap}, f, indent=2)

threading.Thread(target=save_state_loop, daemon=True).start()

try:
    bus = can.interface.Bus(channel=CHANNEL, interface="socketcan")
except Exception as e:
    sys.exit(f"[server] Cannot open {CHANNEL}: {e}\n  Try: sudo ip link set {CHANNEL} up type can bitrate 100000")

f_out = open(CAPTURE_FILE, "a")
print(f"[server] Listening on ALL IDs -- {CHANNEL}")
print(f"[server] {len(LABELS)} labelled IDs  |  {len(TV_IDS)} TV-protocol IDs flagged")
print(f"[server] State -> {STATE_FILE}")
print(f"[server] Log   -> {CAPTURE_FILE}")
print("[server] Running -- Ctrl+C to stop\n")

for msg in bus:
    hex_id = f"{msg.arbitration_id:03X}"
    data   = list(msg.data)
    ts     = msg.timestamp * 1000
    decoded = None
    if db:
        try:
            raw     = db.decode_message(msg.arbitration_id, msg.data, decode_choices=True)
            decoded = {k: safe_val(v) for k, v in raw.items()}
        except Exception:
            pass
    frame = {"id_hex": hex_id, "id": msg.arbitration_id,
             "data": data, "len": msg.dlc, "ts": ts, "decoded": decoded}
    with lock:
        s = state[hex_id]
        if s["first_seen"] is None: s["first_seen"] = ts
        s["count"] += 1
        s["frames"].append(frame)
        if decoded: s["decoded"].append(decoded)
        # Track current source from 0x661 byte[3]
        if msg.arbitration_id == SOURCE_ID and msg.dlc >= 4:
            sb = data[3]
            if sb != current_source["byte"]:
                current_source["byte"] = sb
                current_source["name"] = SOURCE_MODES.get(sb, f"0x{sb:02X}")
    f_out.write(json.dumps(frame) + "\n")
    f_out.flush()
