# CAN Detective -- Claude Code Context

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
Config: /home/pi/config.json
listen_only_mode = true  -- tuner emulator must be a SEPARATE process, not part of hudiy
ZMQ sockets use IPC paths under /run/rnse_control/
Ring joined via 0x436 (NWM_RNS)

## DBC Notes
File: PQ35_46_ICAN_V3_6_9_F_20081104_ASR_V1_2.dbc
Encoding: latin-1 (not UTF-8)
Key German terms: Zeichen=character, Befehl=command, Ziel=target,
  Ueberhitzung=overheating, Sender=sender, Empfaenger=receiver
