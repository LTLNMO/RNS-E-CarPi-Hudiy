#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import zmq
import json
import time
import sys
import can 
import threading
import signal
import os

try:
    from icons import audscii_trans
except ImportError:
    sys.exit(1)

CONFIG_PATH = '/home/pi/config.json'

try:
    os.nice(-10) 
except:
    pass 

class TextScroller:
    def __init__(self, width=8, speed=0.35, pause=2.0, stagger=0.0):
        self.width = width
        self.raw_text = ""
        self.pos = 0
        self.last_tick = 0
        self.scroll_speed = speed
        self.pause_time = pause
        self.stagger_delay = stagger
        self.wait_timer = 0
        self.current_bytes = [0x20] * 8
        self.lock = threading.Lock()

    def set_text(self, text):
        text = text.strip() if text else ""
        if text != self.raw_text:
            with self.lock:
                self.raw_text = text
                self.pos = 0
                self.wait_timer = time.time() + self.pause_time + self.stagger_delay
                self._update_bytes()

    def tick(self):
        if not self.raw_text or len(self.raw_text) <= self.width:
            if self.pos != -1:
                with self.lock:
                    self.pos = -1
                    self._update_bytes()
            return
        now = time.time()
        if now < self.wait_timer: return
        if now - self.last_tick > self.scroll_speed:
            with self.lock:
                max_pos = len(self.raw_text) - self.width
                if self.pos < max_pos:
                    self.pos += 1
                    if self.pos == max_pos: self.wait_timer = now + self.pause_time
                else:
                    self.pos = 0
                    self.wait_timer = now + self.pause_time
                self._update_bytes()
                self.last_tick = now

    def _update_bytes(self):
        start = 0 if self.pos == -1 else self.pos
        window = self.raw_text[start : start + self.width].ljust(self.width)
        self.current_bytes = [audscii_trans[ord(c)] if ord(c) < len(audscii_trans) else 0x20 for c in window]

class DirectDisplay:
    def __init__(self):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)
        
        feat = cfg.get('features', {}).get('fis_display', {})
        if not feat.get('enabled', False): sys.exit(0)

        self.bus = can.Bus(interface='socketcan', channel=cfg.get('can_interface', 'can0'))
        
        self.ctx = zmq.Context()
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(cfg['zmq']['publish_address'])
        self.sub.connect(cfg['zmq']['hudiy_publish_address'])
        self.sub.setsockopt(zmq.SUBSCRIBE, b"HUDIY_MEDIA")
        
        self.l1_mode = str(feat.get('line1_mode', 0))
        self.l2_mode = str(feat.get('line2_mode', 0))
        
        self.s1 = TextScroller(8, feat.get('scroll_speed', 0.35), feat.get('scroll_pause', 2.0), 0)
        self.s2 = TextScroller(8, feat.get('scroll_speed', 0.35), feat.get('scroll_pause', 2.0), feat.get('stagger_offset', 1.0))
        
        # --- NEW FLAG ---
        self.show_on_dis = False 
        self.running = True
        signal.signal(signal.SIGINT, self.shutdown)

    def shutdown(self, signum, frame):
        self.running = False
        sys.exit(0)

    def source_monitor(self):
        """ Watches RNS-E and only enables DIS output if on AUX/TV source (0x37) """
        while self.running:
            msg = self.bus.recv(0.5)
            if msg and msg.arbitration_id == 0x661:
                # Byte index 3 is our source byte
                if len(msg.data) >= 4:
                    # Enable DIS if source is 0x37 (AUX/TV)
                    self.show_on_dis = (msg.data[3] == 0x37)

    def parse_mode(self, mode_str, p):
        title = p.get('title') or p.get('track') or ""
        artist = p.get('artist') or ""
        album = p.get('album') or ""
        source = p.get('source_label') or p.get('source') or ""

        if "-" in mode_str:
            parts = mode_str.split("-")
            results = []
            for val in [part.strip() for part in parts]:
                if val == "1": results.append(title)
                elif val == "2": results.append(artist)
                elif val == "3": results.append(album)
                elif val == "9": results.append(source)
            return " - ".join([r for r in results if r])
        
        if mode_str == "1": return title
        if mode_str == "2": return artist
        if mode_str == "3": return album
        if mode_str == "9": return source
        return ""

    def metadata_listener(self):
        while self.running:
            try:
                parts = self.sub.recv_multipart()
                payload = json.loads(parts[1])
                if self.l1_mode != "0":
                    self.s1.set_text(self.parse_mode(self.l1_mode, payload))
                if self.l2_mode != "0":
                    self.s2.set_text(self.parse_mode(self.l2_mode, payload))
            except:
                time.sleep(0.1)

    def burst_thread(self):
        hb_msg = can.Message(arbitration_id=0x602, data=[0x89, 0x12, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00], is_extended_id=False)
        while self.running:
            try:
                # ONLY SEND IF WE ARE ON THE RIGHT SOURCE
                if self.show_on_dis:
                    self.bus.send(hb_msg)
                    if self.l1_mode != "0":
                        with self.s1.lock:
                            self.bus.send(can.Message(arbitration_id=0x363, data=self.s1.current_bytes, is_extended_id=False))
                    if self.l2_mode != "0":
                        with self.s2.lock:
                            self.bus.send(can.Message(arbitration_id=0x365, data=self.s2.current_bytes, is_extended_id=False))
                    time.sleep(0.005)
                else:
                    # Quietly wait until source changes back to 0x37
                    time.sleep(0.1)
            except:
                time.sleep(0.01)

    def run(self):
        threading.Thread(target=self.metadata_listener, daemon=True).start()
        threading.Thread(target=self.burst_thread, daemon=True).start()
        threading.Thread(target=self.source_monitor, daemon=True).start()
        while self.running:
            if self.show_on_dis:
                if self.l1_mode != "0": self.s1.tick()
                if self.l2_mode != "0": self.s2.tick()
            time.sleep(0.05)

if __name__ == "__main__":
    DirectDisplay().run()