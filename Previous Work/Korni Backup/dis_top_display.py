#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import zmq
import json
import time
import logging
import sys

# --- Configuration ---
CONFIG_PATH = '/home/pi/config.json'
LOG_FORMAT = '%(asctime)s [%(levelname)s] (TopDIS) %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

class TextScroller:
    def __init__(self, width=8):
        self.width = width
        self.raw_text = ""
        self.pos = 0
        self.last_tick = 0
        self.scroll_speed = 0.40  # OEM Speed
        self.start_dwell = 3.0    
        self.end_dwell = 2.0      
        self.wait_timer = 0

    def set_text(self, text):
        if text != self.raw_text:
            self.raw_text = text
            self.pos = 0
            self.wait_timer = time.time() + self.start_dwell

    def tick(self):
        if len(self.raw_text) <= self.width: return
        now = time.time()
        if now < self.wait_timer: return

        if now - self.last_tick > self.scroll_speed:
            self.last_tick = now
            self.pos += 1
            max_pos = len(self.raw_text) - self.width
            if self.pos > max_pos:
                self.pos = 0
                self.wait_timer = now + self.end_dwell + self.start_dwell
            elif self.pos == max_pos:
                self.wait_timer = now + self.end_dwell

    def get_window(self):
        if len(self.raw_text) <= self.width:
            return self.raw_text.ljust(self.width)
        return self.raw_text[self.pos : self.pos + self.width]

class TopDisplayService:
    def __init__(self):
        self.load_config()
        self.ctx = zmq.Context()
        
        # Subscriber for Media and Mode changes
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(self.cfg['zmq']['publish_address'])        
        self.sub.connect(self.cfg['zmq']['hudiy_publish_address'])  
        self.sub.subscribe(b"CAN_661")
        self.sub.subscribe(b"HUDIY_MEDIA")
        
        # PUSH to DisService (High Priority Nav Channel)
        self.push = self.ctx.socket(zmq.PUSH)
        self.push.connect("tcp://127.0.0.1:5555") 
        
        self.TV_MODE_ID = int(self.cfg['source_data'].get('tv_mode_identifier', '0x37'), 16)
        self.scroller = TextScroller(width=8)
        self.tv_mode_active = False
        self.last_draw_time = 0

    def load_config(self):
        with open(CONFIG_PATH) as f:
            self.cfg = json.load(f)

    def draw_line_one(self, text):
        """Sends command to DisService to draw ONLY on the top line."""
        try:
            msg = {
                'command': 'draw_text',
                'text': text,
                'y': 0,      # Coordinate 0 is the start of Line 1
                'flags': 0   
            }
            self.push.send_json(msg)
        except Exception as e:
            logger.error(f"DDP Send Error: {e}")

    def handle_messages(self):
        try:
            while True:
                parts = self.sub.recv_multipart(flags=zmq.NOBLOCK)
                topic = parts[0]
                payload = json.loads(parts[1])

                if topic == b"CAN_661":
                    data = bytes.fromhex(payload.get('data_hex', ''))
                    if len(data) >= 4:
                        self.tv_mode_active = (data[3] == self.TV_MODE_ID)

                elif topic == b"HUDIY_MEDIA":
                    # Title ONLY
                    title = payload.get('title', 'Media')
                    self.scroller.set_text(title if title else "Media")
        except zmq.Again:
            pass 

    def run(self):
        logger.info("Line 1 Nav-Priority Service Active")
        while True:
            self.handle_messages()
            
            if self.tv_mode_active:
                now = time.time()
                self.scroller.tick()
                
                # Update every 150ms to maintain the hardware lock
                if now - self.last_draw_time > 0.15:
                    self.draw_line_one(self.scroller.get_window())
                    self.last_draw_time = now
            
            time.sleep(0.01)

if __name__ == "__main__":
    TopDisplayService().run()