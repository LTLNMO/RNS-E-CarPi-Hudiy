#!/usr/bin/env python3
"""
CarPi Power Management
--------------------------------------
Reads all runtime behavior from config.json.
If CarPiHat is enabled, ignition is monitored using lgpio.
If ignition remains OFF for longer than the configured delay,
a clean OS shutdown is triggered.
"""

import lgpio, time, os, signal, sys, logging, json

# ----------------------------------------------------
# LOAD CONFIG.JSON
# ----------------------------------------------------
CONFIG_PATH = "/home/pi/config.json"

try:
    with open(CONFIG_PATH, "r") as f:
        cfg = json.load(f)
except Exception as e:
    print(f"FATAL: Cannot load config.json: {e}")
    sys.exit(1)

carpihat_cfg = cfg["features"]["carpihat"]
carpihat_enabled = carpihat_cfg.get("enabled", False)

IGN_PIN = carpihat_cfg.get("ign_pin", 12)
IGN_LOW_DELAY = carpihat_cfg.get("ign_low_delay_seconds", 10)
DEBOUNCE_TIME = carpihat_cfg.get("debounce_time_seconds", 0.25)
POLL_INTERVAL = carpihat_cfg.get("poll_interval_seconds", 0.1)

CAN_IFACE = cfg.get("can_interface", "can0")

LOG_FILE = "/var/log/carpi.log"

# ----------------------------------------------------
# LOGGING — YOUR FORMAT (NO TIMESTAMP)
# ----------------------------------------------------
logging.basicConfig(
    filename=LOG_FILE,
    filemode="w",
    level=logging.INFO,
    format="%(message)s"
)

logging.info("=== CarPi Power Manager Started ===")
logging.info(f"CarPiHat enabled: {carpihat_enabled}")

# ----------------------------------------------------
# CAN STATE READER
# ----------------------------------------------------
def read_can_state():
    try:
        state = open(f"/sys/class/net/{CAN_IFACE}/operstate").read().strip()
        return "UP" if state == "up" else "DOWN"
    except:
        return "UNKNOWN"

last_can_state = read_can_state()
logging.info(f"Initial CAN state: {last_can_state}")

# ----------------------------------------------------
# If CarPiHat is OFF → do nothing but stay running
# ----------------------------------------------------
if not carpihat_enabled:
    logging.info("CarPiHat disabled — ignition monitoring skipped.")
    logging.info("Script running idle for compatibility with systemd.")
    signal.pause()   # sleep forever
    sys.exit(0)

# ----------------------------------------------------
# GPIO SETUP
# ----------------------------------------------------
chip = lgpio.gpiochip_open(0)
lgpio.gpio_claim_input(chip, IGN_PIN)

try:
    ign_initial = lgpio.gpio_read(chip, IGN_PIN)
except:
    ign_initial = 1  # assume ON

logging.info(f"Initial IGN state: {'ON' if ign_initial else 'OFF'}")

ign_last = ign_initial
ign_change_time = time.time()
shutdown_initiated = False
off_timer = 0.0

# ----------------------------------------------------
# CAN CHANGE LOGGER
# ----------------------------------------------------
def log_can_if_changed():
    global last_can_state
    current = read_can_state()
    if current != last_can_state:
        last_can_state = current
        logging.info(f"CAN state changed: {current}")

# ----------------------------------------------------
# SAFE EXIT HANDLER
# ----------------------------------------------------
def exit_handler(*_):
    try:
        lgpio.gpiochip_close(chip)
    except:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, exit_handler)
signal.signal(signal.SIGTERM, exit_handler)

# ----------------------------------------------------
# MAIN LOOP
# ----------------------------------------------------
try:
    while True:

        try:
            ign = lgpio.gpio_read(chip, IGN_PIN)
        except:
            ign = 1  # assume ON

        now = time.time()

        # Debounce
        if ign != ign_last:
            if now - ign_change_time >= DEBOUNCE_TIME:
                ign_last = ign
                ign_change_time = now

                logging.info(f"IGN state transition: {'IGNITION ON' if ign else 'IGNITION OFF'}")
                log_can_if_changed()

                # If ignition returns, cancel shutdown
                if ign == 1:
                    # NEW: Recovery logic for interrupted staged sleep
                    if shutdown_initiated:
                        # Original logging message preserved
                        logging.info("Shutdown pending but IGN returned — reset")
                        
                        # Added hardware recovery for Listen-Only mode
                        logging.info(f"Physical IGN ON: Restoring full CAN transmission for {CAN_IFACE}.")
                        os.system(f"sudo ip link set {CAN_IFACE} down")
                        os.system(f"sudo ip link set {CAN_IFACE} type can bitrate 100000 triple-sampling on listen-only off")
                        os.system(f"sudo ip link set {CAN_IFACE} up")
                    
                    shutdown_initiated = False
                    off_timer = 0.0

            time.sleep(POLL_INTERVAL)
            continue

        # IGNITION OFF logic
        if ign == 0:
            off_timer += POLL_INTERVAL

            if off_timer >= IGN_LOW_DELAY and not shutdown_initiated:
                shutdown_initiated = True
                logging.info(f"Shutdown threshold reached ({IGN_LOW_DELAY}s) — shutdown triggered")
                log_can_if_changed()
                os.system("sudo shutdown -h now")
                break

        else:
            off_timer = 0.0

        time.sleep(POLL_INTERVAL)

except KeyboardInterrupt:
    exit_handler()
finally:
    exit_handler()