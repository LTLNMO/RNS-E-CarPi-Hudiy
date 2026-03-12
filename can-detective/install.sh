#!/usr/bin/env bash
# =============================================================================
#  CAN Detective — Installer for Raspberry Pi (RNS-E 193F TV Tuner Hunter)
#  Put this file + the DBC in the same folder, then:
#    chmod +x install.sh && ./install.sh
# =============================================================================

set -e

INSTALL_DIR="$HOME/can-detective"
VENV_DIR="$INSTALL_DIR/.venv"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ask() writes prompt to /dev/tty so it shows even inside $() subshells
ask() {
    printf "\n%s " "$1" > /dev/tty
    read -r REPLY < /dev/tty
    echo "$REPLY"
}

echo ""
echo "================================================"
echo "  CAN Detective -- RNS-E 193F TV Tuner Hunter"
echo "================================================"
echo ""

# =============================================================================
# Questions
# =============================================================================
HUDIY_CONFIG=$(ask "Path to your hudiy config.json? [default: /home/pi/config.json]")
HUDIY_CONFIG=${HUDIY_CONFIG:-/home/pi/config.json}

NET_IFACE=$(ask "Network interface for SavvyCAN bridge? [default: eth0]")
NET_IFACE=${NET_IFACE:-eth0}

echo ""
echo "  hudiy config : $HUDIY_CONFIG"
echo "  net iface    : $NET_IFACE"
echo "  install dir  : $INSTALL_DIR"
echo ""

# =============================================================================
# 1. System packages
# =============================================================================
echo "[1/7] Installing system packages..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
    python3 python3-pip python3-venv \
    can-utils git autoconf net-tools jq 2>/dev/null || true
echo "  done"

# =============================================================================
# 2. socketcand
# =============================================================================
echo "[2/7] Installing socketcand..."
if command -v socketcand &>/dev/null; then
    echo "  already installed"
elif sudo apt-get install -y -qq socketcand 2>/dev/null; then
    echo "  installed via apt"
else
    echo "  building from source..."
    cd /tmp && rm -rf socketcand
    git clone --quiet https://github.com/linux-can/socketcand
    cd socketcand && autoreconf -fi -q && ./configure -q && make -s && sudo make install -s
    cd "$OLDPWD"
    echo "  built ok"
fi

# =============================================================================
# 3. Directories
# =============================================================================
echo "[3/7] Creating directories..."
mkdir -p "$INSTALL_DIR"/{snapshots,diffs,logs}
echo "  $INSTALL_DIR/{snapshots,diffs,logs}"

# =============================================================================
# 4. Python venv
# =============================================================================
echo "[4/7] Creating Python venv..."
python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --quiet --upgrade pip
"$VENV_DIR/bin/pip" install --quiet python-can cantools pyzmq
echo "  done"

# =============================================================================
# 5. Copy DBC if present next to installer
# =============================================================================
echo "[5/7] Looking for DBC..."
DBC_PATH=""
for f in "$SCRIPT_DIR"/*.dbc; do
    if [ -f "$f" ]; then
        cp "$f" "$INSTALL_DIR/"
        DBC_PATH="$INSTALL_DIR/$(basename "$f")"
        echo "  copied: $DBC_PATH"
        break
    fi
done
if [ -z "$DBC_PATH" ]; then
    echo "  no DBC found next to install.sh -- add path to config.json later"
fi

# =============================================================================
# 6. Read CAN interface from hudiy config
# =============================================================================
echo "[6/7] Reading hudiy config..."
CAN_IFACE="can0"
if [ -f "$HUDIY_CONFIG" ]; then
    # Try new nested format first, fall back to flat key
    IFACE_NEW=$(jq -r '.interfaces.can.infotainment // empty' "$HUDIY_CONFIG" 2>/dev/null || true)
    IFACE_OLD=$(jq -r '.can_interface // empty'               "$HUDIY_CONFIG" 2>/dev/null || true)
    CAN_IFACE="${IFACE_NEW:-${IFACE_OLD:-can0}}"
    echo "  CAN interface: $CAN_IFACE"
else
    echo "  WARNING: $HUDIY_CONFIG not found -- defaulting to can0"
fi

# =============================================================================
# 7. Write all project files via Python
#    (avoids heredoc quoting issues with special chars in paths/values)
# =============================================================================
echo "[7/7] Writing project files..."

"$VENV_DIR/bin/python3" - <<PYEOF
import json, os, stat

BASE         = os.path.expanduser("~/can-detective")
VENV         = os.path.join(BASE, ".venv")
CAN_IFACE    = "${CAN_IFACE}"
NET_IFACE    = "${NET_IFACE}"
DBC_PATH     = "${DBC_PATH}"
HUDIY_CONFIG = "${HUDIY_CONFIG}"

# ── Merge CAN IDs from hudiy config ──────────────────────────────────────────
hudiy_ids = {}
if os.path.exists(HUDIY_CONFIG):
    try:
        hudiy_ids = json.load(open(HUDIY_CONFIG)).get("can_ids", {})
    except Exception as e:
        print(f"  warning: could not read hudiy can_ids: {e}")

# Full ID map from DBC analysis -- DBC-derived entries are filled in,
# hudiy config values override anything here if they differ
dbc_ids = {
    # DIS text lines
    "fis_line1":             "0x363",  # mRadio_1_neu  Zeichen_1..8
    "fis_line2":             "0x365",  # mRadio_2_neu  Zeichen_9..16
    # Source/mode frame
    "source":                "0x661",  # mRadio_4  byte[1]=RA4_Radio_Mode (0x37=TV)
    # TV tuner protocol
    "tv_presence":           "0x602",  # mTV_A_1   TV_hybrid heartbeat
    "tv_nwm":                "0x437",  # NWM_TV    TV ring management
    "tv_tp_to_tuner_1":      "0x4F8",  # mTP_Dyn_TV_1  gateway->tuner TP
    "tv_tp_to_tuner_2":      "0x4F9",  # mTP_Dyn_TV_2  gateway->tuner TP
    "tv_bap":                "0x6D3",  # BAP_TV_Tuner
    "tv_iso_req":            "0x76D",  # ISO_TV_Tuner_Req
    "tv_iso_resp":           "0x7D7",  # ISO_TV_Tuner_Resp
    "tv_tp_error":           "0x257",  # mTP_Er_TV_Tuner
    # Ring management (NWM) - all known nodes
    "nwm_rns":               "0x436",  # NWM_RNS  (hudiy already uses this)
    "nwm_radio":             "0x439",  # NWM_Radio
    "nwm_mdi":               "0x435",  # NWM_MDI
    "nwm_dsp":               "0x433",  # NWM_DSP
    "nwm_rearview":          "0x429",  # NWM_Rearview
    "nwm_telefon":           "0x43A",  # NWM_Telefon
    # Companion frames
    "radio_3":               "0x361",  # mRadio_3  station presets + address
    "mmi":                   "0x461",  # mRadio_MUX_1
    "mfsw":                  "0x5C3",  # mMFL_Cmd_
    "light_status":          "0x635",  # mDimmung
    "time_data":             "0x623",  # mKombi_K2
    "ignition_status":       "0x2C3",  # mZAS_Status
    "media_status":          "0x6C1",  # mTp_KOM_NSL
    "nav_status":            "0x6C0",  # mTp_NSL_KOM
}

# hudiy values win on conflict, but skip blank entries
merged_ids = {**dbc_ids}
for k, v in hudiy_ids.items():
    if v:  # skip empty strings like tv_presence was in old config
        merged_ids[k] = v

print(f"  {len(merged_ids)} CAN IDs in config ({len(hudiy_ids)} from hudiy, {len(dbc_ids)} from DBC)")

# ── config.json ──────────────────────────────────────────────────────────────
config = {
    "_notes": {
        "0x661_byte1_modes": "RA4_Radio_Mode: 0x37=TV, other values=FM/AM/CD/AUX (no VAL_ table in DBC)",
        "0x602_key_signals": "TV1_TVA_On=bit0 (must=1), TV1_TVA_Mode=byte1, TV1_TVA_Para1-6=bytes2-7",
        "0x437_purpose":     "NWM_TV ring join -- emulator must send this to be seen as TV_hybrid",
        "dbc_encoding":      "latin-1 (not UTF-8) -- has German special chars",
        "listen_only_note":  "hudiy listen_only_mode=true -- tuner emulator must run as separate process"
    },
    "hudiy_config":      HUDIY_CONFIG,
    "can_interface":     CAN_IFACE,
    "net_interface":     NET_IFACE,
    "socketcand_port":   29536,
    "dbc_file":          DBC_PATH,
    "dbc_encoding":      "latin-1",
    "state_interval":    2,
    "frame_history":     100,
    "capture_file":      os.path.join(BASE, "logs", "capture.jsonl"),
    "state_file":        os.path.join(BASE, "current_state.json"),
    "snapshots_dir":     os.path.join(BASE, "snapshots"),
    "diffs_dir":         os.path.join(BASE, "diffs"),
    "tv_source_mode_byte": "0x37",
    "can_ids":           merged_ids,
}
with open(os.path.join(BASE, "config.json"), "w") as f:
    json.dump(config, f, indent=4)
print(f"  config.json written")

# ── server.py ────────────────────────────────────────────────────────────────
server_py = r'''#!/usr/bin/env python3
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
        with open(STATE_FILE, "w") as f:
            json.dump({"updated": datetime.now().isoformat(),
                       "total_ids": len(snap),
                       "total_frames": sum(v["count"] for v in snap.values()),
                       "tv_ids_seen": [h for h,v in snap.items() if v["is_tv_related"]],
                       "ids": snap}, f, indent=2)

threading.Thread(target=save_state_loop, daemon=True).start()

try:
    bus = can.interface.Bus(channel=CHANNEL, bustype="socketcan")
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
    f_out.write(json.dumps(frame) + "\n")
    f_out.flush()
'''

# ── snapshot.py ──────────────────────────────────────────────────────────────
snapshot_py = r'''#!/usr/bin/env python3
"""
CAN Detective -- snapshot.py

  save <n>       capture current bus state
  diff <a> <b>   compare two snapshots -- highlights TV-related changes
  tv             show only TV-protocol frames and 0x661 source byte
  ascii          frames carrying ASCII text
  ids            all active frame IDs
  unknown        frames NOT in config (potential undocumented poll frames)
  summary        quick overview
  watch <id>     live-tail one frame (e.g. 602 or 0x602)
"""
import json, sys, os, time
from datetime import datetime

BASE       = os.path.dirname(os.path.abspath(__file__))
CFG        = json.load(open(os.path.join(BASE, "config.json")))
STATE_FILE = CFG["state_file"]
SNAP_DIR   = CFG["snapshots_dir"]
DIFF_DIR   = CFG["diffs_dir"]
os.makedirs(SNAP_DIR, exist_ok=True)
os.makedirs(DIFF_DIR, exist_ok=True)

LABELS = {}
for name, val in CFG.get("can_ids", {}).items():
    try:
        LABELS[int(val,16) if isinstance(val,str) else int(val)] = name
    except Exception:
        pass

TV_SOURCE_BYTE = int(CFG.get("tv_source_mode_byte","0x37"),16)

def load_state():
    with open(STATE_FILE) as f: return json.load(f)

def fmt(hex_id):
    try:
        lbl = LABELS.get(int(hex_id,16),"")
        return f"0x{hex_id}" + (f" ({lbl})" if lbl else "")
    except Exception: return hex_id

def save(name):
    s = load_state()
    out = os.path.join(SNAP_DIR, f"{name}.json")
    with open(out,"w") as f: json.dump(s,f,indent=2)
    print(f"[snap] '{name}' -- {len(s['ids'])} IDs  ({s['updated']})")

def diff(na, nb):
    a = json.load(open(os.path.join(SNAP_DIR,f"{na}.json")))["ids"]
    b = json.load(open(os.path.join(SNAP_DIR,f"{nb}.json")))["ids"]
    changed, appeared, disappeared = {}, {}, {}
    for id_ in set(a)|set(b):
        in_a,in_b = id_ in a, id_ in b
        if in_a and in_b:
            if (a[id_]["latest_hex"] != b[id_]["latest_hex"] or
                    abs(a[id_]["freq_hz"]-b[id_]["freq_hz"]) > 1.0):
                changed[id_] = {
                    "hex_before": a[id_]["latest_hex"], "hex_after": b[id_]["latest_hex"],
                    "ascii_before": a[id_]["ascii_hint"], "ascii_after": b[id_]["ascii_hint"],
                    "freq_before": a[id_]["freq_hz"], "freq_after": b[id_]["freq_hz"],
                    "decoded_before": a[id_].get("decoded"), "decoded_after": b[id_].get("decoded"),
                    "is_tv": b[id_].get("is_tv_related",False),
                    "changing_bytes": b[id_].get("changing_bytes",[]),
                }
        elif in_b: appeared[id_] = b[id_]
        else:      disappeared[id_] = a[id_]
    result = {"diff":f"{na}->{nb}","timestamp":datetime.now().isoformat(),
              "changed":changed,"appeared":appeared,"disappeared":disappeared}
    out = os.path.join(DIFF_DIR,f"{na}_vs_{nb}.json")
    with open(out,"w") as f: json.dump(result,f,indent=2)
    print(f"\n  Changed:     {len(changed)}")
    print(f"  Appeared:    {len(appeared)}")
    print(f"  Disappeared: {len(disappeared)}")
    print(f"  -> {out}\n")
    if changed:
        print("Changed IDs:")
        for id_,d in sorted(changed.items()):
            tv = "  *** TV ***" if d["is_tv"] else ""
            print(f"  {fmt(id_)}{tv}")
            print(f"    hex:   {d['hex_before']}  ->  {d['hex_after']}")
            if d["ascii_before"].replace(".","").strip() or d["ascii_after"].replace(".","").strip():
                print(f"    ascii: '{d['ascii_before']}' -> '{d['ascii_after']}'")
            if d["freq_before"] != d["freq_after"]:
                print(f"    freq:  {d['freq_before']}Hz -> {d['freq_after']}Hz")
            if d.get("decoded_before") or d.get("decoded_after"):
                print(f"    dec <- {d['decoded_before']}")
                print(f"    dec -> {d['decoded_after']}")
            if d["changing_bytes"]: print(f"    changing bytes: {d['changing_bytes']}")
            print()
    if appeared:
        print("New IDs (only in second snapshot):")
        for id_,d in sorted(appeared.items()):
            tv = "  *** TV ***" if d.get("is_tv_related") else ""
            print(f"  {fmt(id_)}{tv}  {d['latest_hex']}  ascii='{d['ascii_hint']}'  {d['freq_hz']}Hz")
    if disappeared:
        print("\nDisappeared IDs:")
        for id_,d in sorted(disappeared.items()):
            print(f"  {fmt(id_)}  {d['latest_hex']}  {d['freq_hz']}Hz")

def show_tv():
    s = load_state()
    tv_ids = s.get("tv_ids_seen",[])
    print(f"\nTV-Protocol Frames  ({s['updated']})\n")
    if not tv_ids:
        print("  None seen yet.")
        return
    for hex_id in sorted(tv_ids):
        d = s["ids"].get(hex_id,{})
        if not d: continue
        print(f"  {fmt(hex_id)}")
        print(f"    hex:     {d['latest_hex']}")
        print(f"    bytes:   {d.get('bytes',[])}")
        print(f"    ascii:   '{d['ascii_hint']}'")
        print(f"    freq:    {d['freq_hz']}Hz")
        if d.get("decoded"): print(f"    decoded: {d['decoded']}")
        if d.get("changing_bytes"): print(f"    changing bytes: {d['changing_bytes']}")
        print()
    # 0x661 source byte interpretation
    src_int = int(CFG["can_ids"].get("source","0x661"),16)
    src_hex = f"{src_int:03X}"
    if src_hex in s["ids"]:
        raw = s["ids"][src_hex].get("bytes",[])
        mode = raw[1] if len(raw)>1 else None
        if mode is not None:
            tv = mode == TV_SOURCE_BYTE
            print(f"  0x661 RA4_Radio_Mode = 0x{mode:02X}  {'*** TV SOURCE ACTIVE ***' if tv else '(not TV source)'}")

def show_ascii():
    s = load_state()
    print(f"\nFrames with ASCII text  ({s['updated']})\n")
    hits = [(id_,d) for id_,d in s["ids"].items()
            if len(d["ascii_hint"].replace(".","").strip())>=2]
    for id_,d in sorted(hits):
        tv = "  ***TV***" if d.get("is_tv_related") else ""
        dec = f"  {d['decoded']}" if d.get("decoded") else ""
        print(f"  {fmt(id_)}{tv}  {d['latest_hex']}  ascii='{d['ascii_hint']}'  {d['freq_hz']}Hz{dec}")

def show_ids():
    s = load_state()
    print(f"\nAll active IDs  ({s['total_ids']} total)\n")
    for id_,d in sorted(s["ids"].items()):
        tv = " ***TV***" if d.get("is_tv_related") else ""
        chg = f"  chg={d['changing_bytes']}" if d["changing_bytes"] else ""
        dec = f"  {d['decoded']}" if d.get("decoded") else ""
        print(f"  {fmt(id_)}{tv}  {d['latest_hex']}  {d['freq_hz']}Hz  '{d['ascii_hint']}'{chg}{dec}")

def show_unknown():
    s = load_state()
    known = set(LABELS.keys())
    unk = [(id_,d) for id_,d in s["ids"].items() if int(id_,16) not in known]
    print(f"\nUnknown frames (not in config can_ids) -- {len(unk)} found\n")
    if not unk:
        print("  All frames are labelled.")
        return
    for id_,d in sorted(unk):
        print(f"  [0x{id_}]  {d['latest_hex']}  {d['freq_hz']}Hz  "
              f"ascii='{d['ascii_hint']}'  dlc={d['dlc']}"
              + (f"  dec={d['decoded']}" if d.get("decoded") else ""))

def summary():
    s = load_state()
    ids = s["ids"]
    print(f"\nBus Summary  ({s['updated']})")
    print(f"  Active IDs:         {s['total_ids']}")
    print(f"  Total frames:       {s['total_frames']}")
    print(f"  High-freq (>5Hz):   {sum(1 for d in ids.values() if d['freq_hz']>5)}")
    print(f"  Has text:           {sum(1 for d in ids.values() if d['has_text'])}")
    print(f"  Unknown IDs:        {sum(1 for id_,d in ids.items() if int(id_,16) not in LABELS)}")
    print(f"\n  TV Protocol IDs:")
    tv_map = {
        "0x602":"mTV_A_1       tuner heartbeat (TV1_TVA_On must=1)",
        "0x437":"NWM_TV        ring join for TV_hybrid",
        "0x4F8":"mTP_Dyn_TV_1  gateway->tuner TP data",
        "0x4F9":"mTP_Dyn_TV_2  gateway->tuner TP data",
        "0x661":"mRadio_4      byte[1]=0x37 means TV source active",
        "0x363":"mRadio_1_neu  DIS line1 (chars 1-8)",
        "0x365":"mRadio_2_neu  DIS line2 (chars 9-16)",
    }
    for hex_str,desc in tv_map.items():
        int_id = int(hex_str,16)
        hid    = f"{int_id:03X}"
        seen   = hid in ids
        status = f"{ids[hid]['latest_hex']}  {ids[hid]['freq_hz']}Hz" if seen else "NOT SEEN"
        print(f"    [{hex_str}] {desc:<50} {status}")

def watch(frame_id):
    frame_id = frame_id.upper().lstrip("0X").lstrip("X").zfill(3)
    print(f"Watching {fmt(frame_id)} -- Ctrl+C to stop\n")
    last = None
    while True:
        try:
            s = load_state()
            entry = s["ids"].get(frame_id)
            if entry and entry["latest_hex"] != last:
                last = entry["latest_hex"]
                dec = f"  dec={entry['decoded']}" if entry.get("decoded") else ""
                print(f"  [{datetime.now().strftime('%H:%M:%S.%f')[:-3]}]  "
                      f"{entry['latest_hex']}  ascii='{entry['ascii_hint']}'  "
                      f"{entry['freq_hz']}Hz{dec}")
            time.sleep(0.3)
        except KeyboardInterrupt:
            print("\nStopped.")
            break

cmd = sys.argv[1] if len(sys.argv)>1 else ""
if   cmd=="save"    and len(sys.argv)==3: save(sys.argv[2])
elif cmd=="diff"    and len(sys.argv)==4: diff(sys.argv[2],sys.argv[3])
elif cmd=="tv":                           show_tv()
elif cmd=="ascii":                        show_ascii()
elif cmd=="ids":                          show_ids()
elif cmd=="unknown":                      show_unknown()
elif cmd=="summary":                      summary()
elif cmd=="watch"   and len(sys.argv)==3: watch(sys.argv[2])
else: print(__doc__)
'''

# ── CLAUDE.md ────────────────────────────────────────────────────────────────
claude_md = """# CAN Detective -- Claude Code Context

## Goal
Emulate the VAG TV tuner so the RNS-E 193F writes real channel names
to DIS lines 1 and 2 instead of the fallback "AV" / "TV/Video".

## TV Tuner Protocol (from DBC PQ35_46_ICAN_V3_6_9_F_20081104_ASR_V1_2)

### Frames the real tuner sends -- we must emulate these:
0x437  NWM_TV       Ring join heartbeat. Sender=TV_hybrid.
                    NWM_TV_Receiver=byte0, NWM_TV_CmdAlive=bit9, NWM_TV_CmdRing=bit8
0x602  mTV_A_1      Tuner status. Sender=TV_hybrid -> RNS_MFD2.
                    TV1_TVA_On=bit0 (MUST=1 for RNS-E to see tuner as present)
                    TV1_TVA_Mode=byte1, TV1_TVA_Para1..6=bytes2-7

### Frames RNS-E sends to tuner -- watch these for channel requests:
0x4F8  mTP_Dyn_TV_1   Gateway_PQ35 -> TV_hybrid  (TP channel data)
0x4F9  mTP_Dyn_TV_2   Gateway_PQ35 -> TV_hybrid  (TP channel data)

### DIS text -- RNS-E writes these when tuner is present:
0x363  mRadio_1_neu   Signals RA1_DSP_Zeichen_1..8  = DIS chars 1-8  (raw bytes)
0x365  mRadio_2_neu   Signals RA2_DSP_Zeichen_9..16 = DIS chars 9-16 (raw bytes)
Text is 16 chars total across both frames.

### Source frame:
0x661  mRadio_4       byte[1] = RA4_Radio_Mode
                      0x37 = TV source active (empirical -- no VAL_ table in DBC)
                      TV_hybrid is a RECEIVER of this frame -- real tuner watched it

## Sequence When TV Source Selected
1. User selects TV -> RNS-E sets 0x661 byte[1]=0x37
2. Real tuner sees 0x661, joins ring (0x437), sends heartbeat 0x602 with TVA_On=1
3. RNS-E sees 0x602 TVA_On=1, knows tuner is alive, writes channel text to 0x363/0x365
4. Without step 2-3 -> RNS-E falls back to static "AV" / "TV/Video"

## Key Investigation Files
current_state.json  -- live bus state, refreshes every 2s
capture.jsonl       -- raw frame log
snapshots/*.json    -- named captures
diffs/*.json        -- what changed between two captures

## Snapshot Commands
python snapshot.py summary              quick overview
python snapshot.py tv                   TV protocol frames only + 0x661 source byte
python snapshot.py save radio           capture baseline
python snapshot.py save tv_active       capture after switching to TV source
python snapshot.py diff radio tv_active what changed?
python snapshot.py unknown              frames not in config (find unknown poll IDs)
python snapshot.py watch 602            is the tuner heartbeat present?
python snapshot.py watch 661            watch source mode byte change

## Existing HUDIY System
Config: """ + HUDIY_CONFIG + """
listen_only_mode = true  -- tuner emulator must be a SEPARATE process, not part of hudiy
ZMQ sockets use IPC paths under /run/rnse_control/
Ring joined via 0x436 (NWM_RNS)

## DBC Notes
File: PQ35_46_ICAN_V3_6_9_F_20081104_ASR_V1_2.dbc
Encoding: latin-1 (not UTF-8)
Key German terms: Zeichen=character, Befehl=command, Ziel=target,
  Ueberhitzung=overheating, Sender=sender, Empfaenger=receiver
"""

# ── run.sh ───────────────────────────────────────────────────────────────────
run_sh = f"""#!/usr/bin/env bash
# Start a CAN Detective session.
# Open a second terminal for snapshot commands while this runs.

INSTALL_DIR="{BASE}"
CAN_IFACE="{CAN_IFACE}"
NET_IFACE="{NET_IFACE}"
PORT=29536

echo ""
echo "=== CAN Detective ==="
echo ""

# Bring up CAN interface
sudo ip link set $CAN_IFACE down 2>/dev/null || true
sudo ip link set $CAN_IFACE up type can bitrate 100000 2>/dev/null || true
sudo ip link set $CAN_IFACE up

ip link show $CAN_IFACE | grep -q "UP" && echo "OK $CAN_IFACE up" || echo "WARNING: $CAN_IFACE not up -- check hardware"

# Start socketcand for SavvyCAN
sudo socketcand -i $CAN_IFACE -l $NET_IFACE -p $PORT &
SCAND_PID=$!
PI_IP=$(hostname -I | awk '{{print $1}}')
echo "OK socketcand running on port $PORT"
echo "   SavvyCAN: Remote SocketCAN -> $PI_IP:$PORT"

echo ""
echo "Snapshot commands (run in a second terminal):"
echo "  python snapshot.py summary"
echo "  python snapshot.py tv"
echo "  python snapshot.py save <name>"
echo "  python snapshot.py diff <a> <b>"
echo "  python snapshot.py unknown"
echo "  python snapshot.py watch 602"
echo ""

cd "$INSTALL_DIR"
"{VENV}/bin/python" server.py

kill $SCAND_PID 2>/dev/null || true
"""

# Write all files
files = {
    "server.py":   server_py,
    "snapshot.py": snapshot_py,
    "CLAUDE.md":   claude_md,
    "run.sh":      run_sh,
}
for fname, content in files.items():
    path = os.path.join(BASE, fname)
    with open(path, "w") as f:
        f.write(content)
    if fname.endswith(".sh"):
        os.chmod(path, os.stat(path).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    print(f"  {fname}")

print("")
print("All files written.")
PYEOF

# =============================================================================
# systemd service
# =============================================================================
sudo tee /etc/systemd/system/can-detective.service > /dev/null <<EOF
[Unit]
Description=CAN Detective Server
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$INSTALL_DIR
ExecStart=$VENV_DIR/bin/python $INSTALL_DIR/server.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload

# =============================================================================
# Done
# =============================================================================
PI_IP=$(hostname -I | awk '{print $1}')

echo ""
echo "================================================"
echo "  Done!"
echo "================================================"
echo ""
echo "  Project : $INSTALL_DIR"
echo "  Pi IP   : $PI_IP"
if [ -n "$DBC_PATH" ]; then
echo "  DBC     : $DBC_PATH"
else
echo "  DBC     : not found -- copy DBC next to install.sh and re-run, or set manually in config.json"
fi
echo ""
echo "Start a session:"
echo "  cd $INSTALL_DIR && bash run.sh"
echo ""
echo "First thing to check once running:"
echo "  python snapshot.py summary"
echo "  python snapshot.py tv"
echo "  python snapshot.py watch 602"
echo ""