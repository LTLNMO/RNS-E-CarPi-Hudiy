import zmq
import json
import time
import sys


def load_config():
    with open('/home/pi/config.json', 'r') as f: return json.load(f)

cfg = load_config()
ctx = zmq.Context()
push = ctx.socket(zmq.PUSH)
push.connect(cfg['zmq']['send_address'])

# TV_ID 0x602 identifies the Pi as a valid 'TV' source to the RNS-E
TV_ID = str(int(cfg['can_ids'].get('tv_presence', '0x602'), 0)).encode()
TV_PAYLOAD = "8912304341525049" # Standard TV Identity string

def run():
    print("Heartbeat Active: Sending TV Presence (0x602)...")
    while True:
        try:
            push.send_multipart([TV_ID, TV_PAYLOAD.encode()])
            time.sleep(1.0)
        except Exception as e:
            print(f"Heartbeat Error: {e}")
            time.sleep(5)

if __name__ == "__main__": run()