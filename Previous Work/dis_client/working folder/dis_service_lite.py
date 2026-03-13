import zmq
import json
import time
import logging
import sys
from icons import audscii_trans

logging.basicConfig(level=logging.INFO, format='%(asctime)s [DIS-LITE] %(message)s')
logger = logging.getLogger(__name__)

class DisServiceLite:
    def __init__(self):
        try:
            with open('/home/pi/config.json') as f:
                self.cfg = json.load(f)
        except Exception as e:
            logger.error(f"Config load failed: {e}")
            sys.exit(1)
        
        self.ctx = zmq.Context()
        
        # 1. PULL: Receives text from dis_top_display
        self.pull = self.ctx.socket(zmq.PULL)
        self.pull.bind(self.cfg['zmq']['dis_draw'])
        
        # 2. PUSH: Sends hex to can_base_function
        self.push = self.ctx.socket(zmq.PUSH)
        self.push.connect(self.cfg['zmq']['send_address'])

    def translate(self, text):
        # CHANGED: Use all 8 characters of space.
        # We no longer prepend 0x00. This matches the 'KMA     ' format in your log.
        text = text.ljust(8)[:8]
        return [audscii_trans[ord(char) % 256] for char in text]

    def run(self):
        # 1. Get the ID (e.g., '0x363') from config
        raw_id = self.cfg['can_ids'].get('fis_line1', '0x363')
        
        # 2. Convert it to a decimal string for the handler
        fis_id_dec = str(int(raw_id, 0)).encode('utf-8')
        
        logger.info(f"DIS Lite Active. ID {raw_id} (8-char mode) mapped to decimal {fis_id_dec.decode()}")
        
        while True:
            try:
                msg = self.pull.recv_json()
                if 'text' in msg:
                    # 'text' will now be mapped to all 8 bytes of the CAN frame
                    hex_data = self.translate(msg['text'])
                    hex_str = ''.join(format(x, '02x') for x in hex_data)
                    
                    logger.info(f"Sending to CAN: {msg['text']} -> {hex_str}")
                    
                    # SEND MULTIPART [Decimal ID, DATA]
                    self.push.send_multipart([fis_id_dec, hex_str.encode('utf-8')])
                    
            except Exception as e:
                logger.error(f"Run loop error: {e}")
                time.sleep(1)

if __name__ == "__main__":
    DisServiceLite().run()