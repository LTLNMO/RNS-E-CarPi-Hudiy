#!/usr/bin/env python3
import time
import threading
import can

# Load your translation table
try:
    from icons import audscii_trans
except ImportError:
    audscii_trans = [i for i in range(256)]

# --- CONFIGURATION ---
CAN_IFACE = "can0"
ID_CLAIM = 0x661
ID_LINE1 = 0x363
WIDTH = 8
# v0.9.6 blanking byte (translated space)
BLANK_BYTE = audscii_trans[ord(" ")] 

TEXT = "DAFT PUNK - AROUND THE WORLD / HARDER BETTER FASTER STRONGER (LIVE 2007)".upper()

class v096FullEngine:
    def __init__(self, text):
        self.bus = can.interface.Bus(channel=CAN_IFACE, bustype="socketcan")
        self.text = text
        self.running = True
        self.speed = 6     # How fast it steps
        self.smooth = 10   # How many CAN repeats (Liquid effect)
        
        # --- v0.9.6 STATE VARIABLES ---
        self.begin = -1    # -1 is the 'Static Style' trigger
        self.pause_ticks = 0
        
        self.payload = bytes([BLANK_BYTE] * WIDTH)
        self.last_tick = time.monotonic()

    def get_step_delay(self):
        # Matches the v0.9.6 'speed' feel
        s = max(1, min(10, self.speed))
        return max(0.1, 1.0 - (s * 0.1))

    def logic_loop(self):
        """ Implements both v0.9.6 stages: Static Lead-in & Windowed Step """
        while self.running:
            now = time.monotonic()
            delay = self.get_step_delay()
            
            if (now - self.last_tick) >= delay:
                self.last_tick = now

                # --- STYLE 1: THE STATIC LEAD-IN (begin == -1) ---
                if self.begin == -1:
                    # Capture the first 8 characters and HOLD
                    display_text = self.text[0:WIDTH]
                    self.pause_ticks += 1
                    
                    # Pause for approx 2.5 seconds (standard OEM behavior)
                    if self.pause_ticks > (2.5 / delay):
                        self.begin = 0
                        self.pause_ticks = 0
                
                # --- STYLE 2: THE WINDOWED STEP (begin >= 0) ---
                else:
                    # Slice the string one character further each tick
                    display_text = self.text[self.begin : self.begin + WIDTH]
                    self.begin += 1
                    
                    # RESET: Only when the text has COMPLETELY scrolled off (Lead-out)
                    if self.begin > len(self.text):
                        self.begin = -1

                # Formatting: Pad with spaces and translate to AUDSCII
                display_text = display_text.ljust(WIDTH, " ")
                self.payload = bytes(audscii_trans[ord(c)] & 0xFF for c in display_text)

            time.sleep(0.005)

    def tx_loop(self):
        """ The 'Liquid' hardware saturation loop """
        claim = can.Message(arbitration_id=ID_CLAIM, data=[0xA1, 0x01, 0x12, 0x37, 0, 0, 0, 0], is_extended_id=False)
        
        while self.running:
            try:
                self.bus.send(claim)
                msg = can.Message(arbitration_id=ID_LINE1, data=self.payload, is_extended_id=False)
                
                # Saturated burst: keeps the DIS crystals 'warm' for smoothness
                burst = max(1, min(10, self.smooth)) * 4
                for _ in range(burst):
                    self.bus.send(msg)
            except: pass
            time.sleep(0.01) # 100Hz refresh

    def start(self):
        print("🚀 v0.9.6 Logic + Liquid Timing: ACTIVE")
        threading.Thread(target=self.logic_loop, daemon=True).start()
        threading.Thread(target=self.tx_loop, daemon=True).start()
        
        while self.running:
            try:
                inp = input(f"[Speed {self.speed} Smooth {self.smooth}] > ").split()
                if not inp: continue
                if inp[0] == 'q': self.running = False
                elif inp[0] == 'speed': self.speed = int(inp[1])
                elif inp[0] == 'smooth': self.smooth = int(inp[1])
            except: self.running = False

if __name__ == "__main__":
    v096FullEngine(TEXT).start()