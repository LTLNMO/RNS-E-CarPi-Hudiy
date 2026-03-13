#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import logging
import sys
import time

import can
import zmq

CONFIG_PATH = "/home/pi/config.json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [DIS-LITE] %(message)s')
logger = logging.getLogger(__name__)

try:
    from icons import audscii_trans
except Exception:
    print("ERROR: icons.py not found or failed to import.")
    sys.exit(1)


class DisServiceLite:
    """
    Receives baseline refresh text from dis_top_display via ZMQ,
    translates to audscii, and writes to the CAN bus.
    Time-critical slam/tail sends bypass this service entirely
    and go direct to socketcan from dis_top_display.
    """

    def __init__(self):
        with open(CONFIG_PATH) as f:
            cfg = json.load(f)

        can_ids = cfg.get("can_ids", {})
        self.id_fis_line1 = int(can_ids.get("fis_line1", "0x363"), 16)
        self.id_fis_line2 = int(can_ids.get("fis_line2", "0x365"), 16)

        self.bus = can.Bus(
            interface="socketcan",
            channel=cfg.get("can_interface", "can0"),
        )

        self.ctx  = zmq.Context()
        self.pull = self.ctx.socket(zmq.PULL)
        self.pull.bind(cfg["zmq"]["dis_draw"])

        # Translation cache — O(1) lookup for repeated text
        self._cache = {}

        logger.info(
            f"Ready. Line1=0x{self.id_fis_line1:03X} "
            f"Line2=0x{self.id_fis_line2:03X}"
        )

    def _translate(self, text):
        if text not in self._cache:
            # Use text exactly as received — dis_top_display has already
            # centered short text and windowed long text. Re-justifying here
            # would produce different bytes than the slams, causing CANWatcher
            # to see a mismatch and trigger spurious re-slams (flash).
            t = (text or " " * 8)[:8].ljust(8)
            self._cache[text] = [
                audscii_trans[ord(c)] if ord(c) < len(audscii_trans) else 0x20
                for c in t
            ]
            if len(self._cache) > 64:
                self._cache.pop(next(iter(self._cache)))
        return self._cache[text]

    def _send(self, can_id, data):
        try:
            self.bus.send(can.Message(
                arbitration_id=can_id,
                data=data,
                is_extended_id=False,
            ))
        except Exception:
            pass

    def run(self):
        while True:
            try:
                msg  = self.pull.recv_json()
                line = msg.get("line")
                data = self._translate(msg.get("text", ""))
                if line == 1:
                    self._send(self.id_fis_line1, data)
                elif line == 2:
                    self._send(self.id_fis_line2, data)
            except Exception:
                time.sleep(0.01)


if __name__ == "__main__":
    DisServiceLite().run()