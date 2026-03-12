#!/usr/bin/env python3
"""
CAN Detective -- snapshot.py

  save <n>       capture current bus state as snapshots/<n>.json
  source         show current source, DIS lines, and all source-mode frames
  sources        save a snapshot named after the current source (auto-detected)
  diff <a> <b>   compare two snapshots -- highlights changes per source
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
SOURCE_MODES   = {int(k,16): v for k,v in CFG.get("source_modes",{}).items()}

def load_state():
    for _ in range(5):
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError):
            time.sleep(0.05)
    with open(STATE_FILE) as f:
        return json.load(f)

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
    # 0x661 source byte[3] = RA4_Radio_Para2 (TV=0x37)
    src_int = int(CFG["can_ids"].get("source","0x661"),16)
    src_hex = f"{src_int:03X}"
    if src_hex in s["ids"]:
        raw = s["ids"][src_hex].get("bytes",[])
        mode = raw[3] if len(raw)>3 else None
        if mode is not None:
            name = SOURCE_MODES.get(mode, f"0x{mode:02X}")
            tv = mode == TV_SOURCE_BYTE
            print(f"  0x661 byte[3]=0x{mode:02X} ({name})  {'*** TV SOURCE ACTIVE ***' if tv else '(not TV source)'}")

def show_source():
    """Show current source mode, DIS content, and all source-related frames."""
    s = load_state()
    ids = s["ids"]

    # Current source from server (if available) or derive from 0x661
    src = s.get("current_source", {})
    src_byte = src.get("byte")
    src_name = src.get("name", "unknown")
    if src_byte is None:
        raw = ids.get("661", {}).get("bytes", [])
        src_byte = raw[3] if len(raw) > 3 else None
        src_name = SOURCE_MODES.get(src_byte, f"0x{src_byte:02X}") if src_byte is not None else "unknown"

    print(f"\nCurrent source: {src_name}  (byte=0x{src_byte:02X})" if src_byte is not None
          else "\nCurrent source: unknown (0x661 not seen)")

    # DIS lines
    for hex_id, label in [("363","DIS line 1"), ("365","DIS line 2")]:
        d = ids.get(hex_id)
        if d:
            print(f"\n  0x{hex_id} {label}:  '{d['ascii_hint']}'  ({d['latest_hex']})  {d['freq_hz']}Hz")

    # Source-related frames: 0x661, 0x602, 0x437, 0x461, 0x463
    print()
    for key, default in [("source","0x661"),("tv_presence","0x602"),("tv_nwm","0x437"),
                         ("mmi","0x461")]:
        int_id = int(CFG["can_ids"].get(key, default), 16)
        hex_id = f"{int_id:03X}"
        d = ids.get(hex_id)
        label = CFG["can_ids"].get(key, default)
        if d:
            print(f"  {label:<12} 0x{hex_id}  {d['latest_hex']}  {d['freq_hz']}Hz  '{d['ascii_hint']}'")
        else:
            print(f"  {label:<12} 0x{hex_id}  NOT SEEN")

    # 0x463 (tuner response — not in config, check manually)
    d463 = ids.get("463")
    if d463:
        print(f"  {'mmi_resp':<12} 0x463  {d463['latest_hex']}  {d463['freq_hz']}Hz  '{d463['ascii_hint']}'")
    else:
        print(f"  {'mmi_resp':<12} 0x463  NOT SEEN")
    print()


def save_source():
    """Save a snapshot automatically named after the current source."""
    s = load_state()
    src = s.get("current_source", {})
    src_byte = src.get("byte")
    if src_byte is None:
        raw = s["ids"].get("661", {}).get("bytes", [])
        src_byte = raw[3] if len(raw) > 3 else None
    name = SOURCE_MODES.get(src_byte, f"source_0x{src_byte:02X}") if src_byte is not None else "source_unknown"
    out = os.path.join(SNAP_DIR, f"{name}.json")
    with open(out, "w") as f:
        json.dump(s, f, indent=2)
    src_str = f"0x{src_byte:02X}" if src_byte is not None else "0x??"
    print(f"[snap] '{name}' saved -- source={src_str}  {len(s['ids'])} IDs  ({s['updated']})")


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
elif cmd=="source":                       show_source()
elif cmd=="sources":                      save_source()
elif cmd=="diff"    and len(sys.argv)==4: diff(sys.argv[2],sys.argv[3])
elif cmd=="tv":                           show_tv()
elif cmd=="ascii":                        show_ascii()
elif cmd=="ids":                          show_ids()
elif cmd=="unknown":                      show_unknown()
elif cmd=="summary":                      summary()
elif cmd=="watch"   and len(sys.argv)==3: watch(sys.argv[2])
else: print(__doc__)
