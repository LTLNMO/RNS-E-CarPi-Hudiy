#!/usr/bin/env python3
"""
tuner_emulator.py -- VAG TV tuner emulator for RNS-E (PQ35)

Protocol findings so far (0x461 mMMI, RA6 signals):

  cmd=0x70  op0=0x01  -- WAKE/INIT: sent when TV source selected
  cmd=0x30  op0=0x01  -- PROPERTY GET/SET, prop 1 (channel data)
            op0=0x04  -- PROPERTY GET/SET, prop 4 (mode/band)
            byte[4]   -- control value (0x40=up, 0x20=down, 0x02=query)
            byte[5]   -- state/step
  cmd=0x7F  op0=0x00  -- OFFLINE: sent when source deselected

  RNS-E briefly writes real channel-list bytes to 0x363 when it gets
  our 0x463 responses. The 4s revert loop was caused by 0x602 byte[0]=0x81
  which set TV1_TVA_Error_flag (bit7) -- RNS-E kept re-waking the "faulted" tuner.
  Fixed: byte[0]=0x09 (TVA_On + 50Hz, no error flags).

Response strategy (0x463):
  cmd=0x70  -> wake OK x2, then immediately proactive channel-locked status:
               [0x36, 0x00, 0x00, 0x00, 0x00, 0x00]  (generic OK)
               [0x36, 0x70, 0x00, 0x00, 0x00, 0x00]  (echo-cmd OK)
               [0x36, 0x01, 0x03, 0x00, 0x01, 0x00]  (ch1 locked, proactive)
  cmd=0x30  -> property ACK with lock status:
               prop 0x01: [0x36, 0x01, 0x03, 0x00, 0x01, 0x00]  (ch1 locked)
               prop 0x04: [0x36, 0x04, 0x03, 0x00, 0x00, 0x00]  (mode locked)
  cmd=0x7F  -> offline ACK: [0x36, 0x7F, 0x00, 0x00, 0x00, 0x00]

  byte[2] in responses: 0x01=data valid, 0x03=valid+locked (bit0=valid, bit1=locked)

Sends:
  0x437  NWM heartbeat      200 ms
  0x602  TVA_On=1           500 ms
  0x363  DIS line 1         250 ms  (TV source only, unless --no-dis)
  0x365  DIS line 2         250 ms  (TV source only, unless --no-dis)

Usage:
  python tuner_emulator.py --no-dis          # observe only
  python tuner_emulator.py                   # send DIS text too
  python tuner_emulator.py --line1 "BBC ONE "
"""

import argparse
import json
import os
import signal
import sys
import time
import threading

import can

BASE = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# Frame builders
# ---------------------------------------------------------------------------

def _nwm_join(predecessor: int = 0x36) -> can.Message:
    return can.Message(arbitration_id=0x437,
                       data=[predecessor, 0x03], is_extended_id=False)

def _nwm_alive(predecessor: int = 0x36) -> can.Message:
    return can.Message(arbitration_id=0x437,
                       data=[predecessor, 0x02], is_extended_id=False)

def _tv_status() -> can.Message:
    """
    0x602 mTV_A_1:
      byte[0] = 0x09 = 0b00001001
        bit0 = TV1_TVA_On        = 1  (tuner active, MUST=1)
        bit3 = TV1_TVA_50Hz      = 1  (50 Hz PAL signal locked)
        bit6 = TV1_TVA_Error     = 0  (no error)
        bit7 = TV1_TVA_Error_flag= 0  (no fault -- was 0x81 which wrongly set bit7)
      byte[1] = TV1_TVA_Mode = 0x01
    """
    return can.Message(arbitration_id=0x602,
                       data=[0x09, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
                       is_extended_id=False)

def _dis(arb_id: int, text: str) -> can.Message:
    raw = text.encode("ascii", errors="replace")[:8].ljust(8)
    return can.Message(arbitration_id=arb_id, data=list(raw),
                       is_extended_id=False)

def _resp_463(data: list) -> can.Message:
    """Send a 6-byte response on 0x463 (tuner -> RNS-E reply frame)."""
    return can.Message(arbitration_id=0x463,
                       data=data[:6], is_extended_id=False)


# ---------------------------------------------------------------------------
# Response dispatch
# ---------------------------------------------------------------------------

def _handle_461(bus: can.Bus, data: bytes) -> None:
    """
    Respond to 0x461 mMMI poll from RNS-E.
    Different response per command type.
    """
    cmd  = data[1]
    op0  = data[2] if len(data) > 2 else 0
    b4   = data[4] if len(data) > 4 else 0

    if cmd == 0x70:
        # WAKE/INIT — tuner was just selected as source.
        # Two OK variants + immediate proactive "channel 1 locked" status.
        # byte[2]=0x03: bit0=valid, bit1=locked. byte[4]=0x01: channel index 1.
        bus.send(_resp_463([0x36, 0x00, 0x00, 0x00, 0x00, 0x00]))
        bus.send(_resp_463([0x36, 0x70, 0x00, 0x00, 0x00, 0x00]))
        bus.send(_resp_463([0x36, 0x01, 0x03, 0x00, 0x01, 0x00]))
        print(f"  -> 0x463 WAKE-OK x2 + CH1-LOCKED")

    elif cmd == 0x30:
        # PROPERTY GET/SET — respond "locked to channel 1".
        # byte[2]=0x03 (valid+locked), byte[4]=0x01 (channel 1) for prop=0x01.
        # prop=0x04 (mode/band): same lock status, value=0x00.
        if op0 == 0x01:
            bus.send(_resp_463([0x36, 0x01, 0x03, 0x00, 0x01, 0x00]))
            print(f"  -> 0x463 PROP-ACK  prop=0x01 locked ch=1  (req b4=0x{b4:02X})")
        else:
            bus.send(_resp_463([0x36, op0, 0x03, 0x00, 0x00, 0x00]))
            print(f"  -> 0x463 PROP-ACK  prop=0x{op0:02X} locked b4=0x{b4:02X}")

    elif cmd == 0x7F:
        # OFFLINE — source deselected.
        bus.send(_resp_463([0x36, 0x7F, 0x00, 0x00, 0x00, 0x00]))
        print(f"  -> 0x463 OFFLINE-ACK")

    else:
        # Unknown command — generic OK.
        bus.send(_resp_463([0x36, cmd, 0x00, 0x00, 0x00, 0x00]))
        print(f"  -> 0x463 GENERIC-ACK  cmd=0x{cmd:02X}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="VAG TV tuner emulator (PQ35 RNS-E)")
    parser.add_argument("--interface", default=None)
    parser.add_argument("--config", default=os.path.join(BASE, "config.json"))
    parser.add_argument("--line1", default="BBC ONE ", metavar="8CHARS")
    parser.add_argument("--line2", default="        ", metavar="8CHARS")
    parser.add_argument("--no-dis", action="store_true",
                        help="Suppress 0x363/0x365 — observe 0x461 only")
    args = parser.parse_args()

    cfg: dict = {}
    if os.path.exists(args.config):
        with open(args.config) as f:
            cfg = json.load(f)

    iface          = args.interface or cfg.get("can_interface", "can0")
    tv_source_byte = int(cfg.get("tv_source_mode_byte", "0x37"), 16)

    print(f"[tuner_emulator] Opening {iface}")
    try:
        bus = can.interface.Bus(channel=iface, interface="socketcan")
    except Exception as exc:
        sys.exit(f"[tuner_emulator] Cannot open {iface}: {exc}")

    stop      = threading.Event()
    tv_active = threading.Event()

    def _on_sig(_s, _f):
        print("\n[tuner_emulator] Stopping.")
        stop.set()
    signal.signal(signal.SIGINT,  _on_sig)
    signal.signal(signal.SIGTERM, _on_sig)

    # -----------------------------------------------------------------------
    # Listener
    # -----------------------------------------------------------------------
    def _listener():
        seen_payloads: set = set()
        reader   = can.BufferedReader()
        notifier = can.Notifier(bus, [reader])

        while not stop.is_set():
            msg = reader.get_message(timeout=0.1)
            if msg is None:
                continue
            aid = msg.arbitration_id

            if aid == 0x661 and msg.dlc >= 4:
                if msg.data[3] == tv_source_byte:
                    if not tv_active.is_set():
                        tv_active.set()
                        print("[0x661] TV source ACTIVE")
                else:
                    if tv_active.is_set():
                        tv_active.clear()
                        print(f"[0x661] TV source inactive (byte[3]=0x{msg.data[3]:02X})")

            elif aid == 0x461 and msg.dlc >= 2 and msg.data[0] == 0x37:
                key  = bytes(msg.data)
                new  = key not in seen_payloads
                seen_payloads.add(key)
                cmd  = msg.data[1]
                op0  = msg.data[2] if msg.dlc > 2 else 0
                tag  = " (NEW)" if new else ""
                print(f"[0x461] cmd=0x{cmd:02X} op0=0x{op0:02X} "
                      f"hex={msg.data.hex()}{tag}")
                _handle_461(bus, msg.data)

            elif aid == 0x6D3:
                print(f"[0x6D3] BAP  {msg.data.hex()}")

            elif aid in (0x4F8, 0x4F9):
                print(f"[0x{aid:03X}] TP   {msg.data.hex()}")

        notifier.stop()

    threading.Thread(target=_listener, daemon=True).start()

    # -----------------------------------------------------------------------
    # Ring join
    # -----------------------------------------------------------------------
    print("[tuner_emulator] Ring-join burst...")
    join = _nwm_join()
    bus.send(join); time.sleep(0.050); bus.send(join)

    alive  = _nwm_alive()
    status = _tv_status()
    dis1   = _dis(0x363, args.line1)
    dis2   = _dis(0x365, args.line2)

    if args.no_dis:
        print("[tuner_emulator] --no-dis: 0x363/0x365 suppressed")
    else:
        print(f"[tuner_emulator] DIS: '{args.line1[:8]}' / '{args.line2[:8]}'")
    print("[tuner_emulator] Running -- watching for 0x461 polls...")

    last_nwm    = time.monotonic()
    last_status = time.monotonic()
    last_dis    = time.monotonic()

    while not stop.is_set():
        now = time.monotonic()

        if now - last_nwm >= 0.200:
            bus.send(alive)
            last_nwm = now

        if now - last_status >= 0.500:
            bus.send(status)
            last_status = now

        if not args.no_dis and tv_active.is_set() and now - last_dis >= 0.250:
            bus.send(dis1)
            bus.send(dis2)
            last_dis = now

        time.sleep(0.005)

    bus.shutdown()
    print("[tuner_emulator] Done.")


if __name__ == "__main__":
    main()
