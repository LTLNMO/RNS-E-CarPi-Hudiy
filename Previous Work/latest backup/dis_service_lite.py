import zmq
import json
import time
from icons import audscii_trans

class DisServiceLite:
    def __init__(self):
        with open('/home/pi/config.json') as f:
            self.cfg = json.load(f)
        self.ctx = zmq.Context()
        self.pull = self.ctx.socket(zmq.PULL)
        self.pull.bind(self.cfg['zmq']['dis_draw'])
        self.push = self.ctx.socket(zmq.PUSH)
        self.push.connect(self.cfg['zmq']['send_address'])
        
        # Lock to 0x363 and 0x665 only. NO 0x365.
        self.id_363 = b"867" 
        self.id_665 = b"1637"

    def translate(self, text):
        text = text.ljust(8)[:8]
        return ''.join(format(audscii_trans[ord(char) % 256], '02x') for char in text)

    def run(self):
        while True:
            try:
                msg = self.pull.recv_json()
                # High-speed back-to-back delivery
                if 'claim' in msg:
                    self.push.send_multipart([self.id_665, msg['claim'].encode('utf-8')])
                if 'text' in msg:
                    hex_str = self.translate(msg['text'])
                    self.push.send_multipart([self.id_363, hex_str.encode('utf-8')])
            except Exception:
                time.sleep(0.01)

if __name__ == "__main__":
    DisServiceLite().run()