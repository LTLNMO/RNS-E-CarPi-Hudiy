#!/usr/bin/env python3
# v1.7.11 - Full Listen-Only Restoration
# Retains all v1.7.4 logic (Time Sync, OS Shutdown fallback, CarPiHat Staged Shutdown)
# REMOVED: ZMQ PUSH (Transmission) and TV Heartbeat task per user request.

import zmq
import json
import time
import logging
import signal
import sys
import subprocess
import os
import asyncio
import aiozmq
import pytz
from datetime import datetime
from typing import Optional, List, Dict, Any

# --- Global State ---
RUNNING = True
RELOAD_CONFIG = False
CONFIG: Dict[str, Any] = {}
FEATURES: Dict[str, Any] = {}

# --- Logging Setup ---
def setup_logging():
    log_file = '/var/log/rnse_control/can_base_function.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

# --- Helper Utilities ---
def hex_to_bcd(hex_str: str) -> int:
    """Converts a 2-character hex string to a BCD integer."""
    if not (isinstance(hex_str, str) and len(hex_str) == 2 and hex_str.isalnum()):
        raise ValueError(f"Input must be a 2-char hex string, got '{hex_str}'")
    return int(hex_str[0]) * 10 + int(hex_str[1])

def execute_system_command(command_list: List[str], log_command: bool = True) -> bool:
    """Executes a shell command and returns success status."""
    if not command_list: 
        return False
    try:
        if log_command: 
            logger.info(f"Executing: {' '.join(command_list)}")
        subprocess.run(command_list, check=True, capture_output=True, text=True) 
        return True
    except Exception as e:
        logger.error(f"Command failed: {e}")
        return False

# --- State Management ---
class AppState:
    def __init__(self):
        self.last_time_sync_attempt_time: float = 0.0
        self.last_kl15_status: int = 1
        self.last_kls_status: int = 1
        self.shutdown_trigger_timestamp: Optional[float] = None
        self.shutdown_pending: bool = False
        self.active_shutdown_delay: float = 300.0 

    def check_shutdown_condition(self) -> bool:
        """Evaluates if the shutdown timer has expired and executes the appropriate action."""
        if not (self.shutdown_pending and self.shutdown_trigger_timestamp):
            return False

        if time.time() - self.shutdown_trigger_timestamp >= self.active_shutdown_delay:
            is_auto_shutdown_enabled = FEATURES.get('power_management', {}).get('enabled', False)
            is_carpihat_enabled = CONFIG.get('carpihat', False) 
            can_iface = CONFIG.get('can_interface', 'can0')
            
            if not is_auto_shutdown_enabled:
                logger.error("Shutdown initiated but power_management is disabled. Resetting.")
                self._reset_shutdown_state()
                return False
            
            if is_carpihat_enabled:
                # CASE 1: CarPiHat Enabled -> STAGED SHUTDOWN (Silent Mode)
                logger.info(f"Delay reached ({self.active_shutdown_delay}s). Silencing CAN bus.")
                commands = [
                    ["sudo", "ip", "link", "set", can_iface, "down"],
                    ["sudo", "ip", "link", "set", can_iface, "type", "can", "bitrate", "100000", 
                     "triple-sampling", "on", "listen-only", "on"],
                    ["sudo", "ip", "link", "set", can_iface, "up"]
                ]
                if all(execute_system_command(cmd, log_command=False) for cmd in commands):
                    logger.info("CAN interface is now LISTEN-ONLY. RNS-E can now sleep.")
                
                self._reset_shutdown_state()
                return False
            else:
                # CASE 2: CarPiHat Disabled -> FULL SYSTEM SHUTDOWN
                logger.info(f"Delay reached ({self.active_shutdown_delay}s). Shutting down OS.")
                if execute_system_command(["sudo", "shutdown", "-h", "now"]):
                    self._reset_shutdown_state()
                    return True
        return False
        
    def _reset_shutdown_state(self):
        """Resets the shutdown timer and updates delays from config."""
        self.shutdown_pending = False
        self.shutdown_trigger_timestamp = None
        if CONFIG.get('carpihat', False):
            self.active_shutdown_delay = CONFIG.get('canbus_delay', 90) 
        else:
            self.active_shutdown_delay = CONFIG.get('shutdown_delay', 300) 

# --- Configuration Handling ---
def load_and_initialize_config(config_path='/home/pi/config.json') -> bool:
    global CONFIG, FEATURES
    logger.info(f"Loading configuration from {config_path}...")
    try:
        with open(config_path, 'r') as f:
            cfg = json.load(f)
        
        FEATURES = cfg.setdefault('features', {})
        zmq_config = cfg.get('zmq', {})
        can_ids = cfg.get('can_ids', {})
        thresholds = cfg.get('thresholds', {})
        carpihat_enabled = bool(FEATURES.get('carpihat', {}).get('enabled', False))

        CONFIG = {
            'zmq_publish_address': zmq_config.get('publish_address'),
            'can_interface': cfg.get('can_interface', 'can0'),
            'can_ids': {
                'time_data': int(can_ids.get('time_data', '0x623'), 16),
                'ignition_status': int(can_ids.get('ignition_status', '0x2C3'), 16),
            },
            'time_data_format': FEATURES.get('time_sync', {}).get('data_format', 'new_logic'),
            'car_time_zone': FEATURES.get('car_time_zone', 'UTC'),
            'time_sync_threshold_seconds': thresholds.get('time_sync_threshold_minutes', 1.0) * 60,
            'shutdown_delay': thresholds.get('shutdown_delay_ignition_off_seconds', 300), 
            'canbus_delay': thresholds.get('canbus_shutdown_delay_seconds', 90), 
            'carpihat': carpihat_enabled,
        }
        
        log_level = logging.DEBUG if FEATURES.get('debug_mode', False) else logging.INFO
        logger.setLevel(log_level)
        return True
    except Exception as e:
        logger.critical(f"FATAL: Config error: {e}")
        return False

# --- Message Handlers ---
def handle_time_data_message(msg: Dict[str, Any], state: AppState):
    """Parses 0x623 and syncs system time if necessary."""
    if not FEATURES.get('time_sync', {}).get('enabled', False) or msg.get('dlc', 0) < 8:
        return
        
    data_hex = msg['data_hex']
    try:
        if CONFIG['time_data_format'] == 'old_logic':
            second, minute, hour = hex_to_bcd(data_hex[6:8]), hex_to_bcd(data_hex[4:6]), hex_to_bcd(data_hex[2:4])
            day, month, year = hex_to_bcd(data_hex[8:10]), hex_to_bcd(data_hex[10:12]), int(data_hex[12:14] + data_hex[14:16])
        else:
            second, minute, hour = int(data_hex[6:8], 16), int(data_hex[4:6], 16), int(data_hex[2:4], 16)
            day, month, year = int(data_hex[8:10], 16), int(data_hex[10:12], 16), (int(data_hex[12:14], 16) * 100) + int(data_hex[14:16], 16)
        
        state.last_time_sync_attempt_time = time.time()
        car_tz = pytz.timezone(CONFIG['car_time_zone'])
        car_utc_dt = car_tz.localize(datetime(year, month, day, hour, minute, second)).astimezone(pytz.utc)
        time_diff = abs((car_utc_dt - datetime.now(pytz.utc)).total_seconds())

        if time_diff > CONFIG['time_sync_threshold_seconds']:
            logger.info(f"Car time differs by {time_diff:.1f}s. Syncing...")
            execute_system_command(["sudo", "date", "-u", car_utc_dt.strftime('%m%d%H%M%Y.%S')])
    except Exception as e:
        logger.warning(f"Time parse error: {e}")

def handle_power_status_message(msg: Dict[str, Any], state: AppState):
    """Parses 0x2C3 and manages the shutdown timer state."""
    if not FEATURES.get('power_management', {}).get('enabled', False): 
        return
        
    try:
        data_byte0 = int(msg['data_hex'][:2], 16)
        kls, kl15 = data_byte0 & 0x01, (data_byte0 >> 1) & 0x01 
        kls_changed = (kls != state.last_kls_status)
        kl15_changed = (kl15 != state.last_kl15_status)
        
        state.last_kls_status, state.last_kl15_status = kls, kl15
        trigger_config = FEATURES['power_management'].get('trigger', 'ignition_off')

        trigger_event = (trigger_config == 'ignition_off' and kl15_changed and kl15 == 0) or \
                        (trigger_config == 'key_pulled' and kls_changed and kls == 0)

        # Start timer if trigger detected
        if trigger_event and not state.shutdown_pending:
            delay = CONFIG['canbus_delay'] if CONFIG['carpihat'] else CONFIG['shutdown_delay']
            logger.info(f"Trigger detected. Starting {delay}s shutdown timer.")
            state.shutdown_pending = True
            state.shutdown_trigger_timestamp = time.time()
            state.active_shutdown_delay = delay
            
        # Cancel timer if activity resumes
        elif state.shutdown_pending:
            cancel_event = (trigger_config == 'ignition_off' and kl15_changed and kl15 == 1) or \
                           (trigger_config == 'key_pulled' and kls_changed and kls == 1)
            
            if cancel_event:
                logger.info("Activity detected. Cancelling pending shutdown.")
                if CONFIG['carpihat']:
                    iface = CONFIG['can_interface']
                    restore_cmds = [
                        ["sudo", "ip", "link", "set", iface, "down"],
                        ["sudo", "ip", "link", "set", iface, "type", "can", "bitrate", "100000", 
                         "triple-sampling", "on", "listen-only", "off"],
                        ["sudo", "ip", "link", "set", iface, "up"]
                    ]
                    for cmd in restore_cmds:
                        execute_system_command(cmd, log_command=False)
                state._reset_shutdown_state()
    except Exception as e: 
        logger.warning(f"Power status error: {e}")

# --- Async Tasks ---
async def listen_task(state: AppState):
    """Subscribes to ZMQ and routes incoming CAN messages."""
    logger.info("ZMQ listener task started.")
    sub_stream = await aiozmq.create_zmq_stream(zmq.SUB, connect=CONFIG['zmq_publish_address'])
    
    # Filter for specific CAN IDs
    ids = CONFIG['can_ids']
    sub_stream.transport.subscribe(f"CAN_{ids['time_data']:03X}".encode('utf-8'))
    sub_stream.transport.subscribe(f"CAN_{ids['ignition_status']:03X}".encode('utf-8'))
    
    while RUNNING:
        msg = await sub_stream.read()
        msg_dict = json.loads(msg[1].decode('utf-8'))
        cid = msg_dict.get('arbitration_id')
        
        if cid == ids['time_data']: 
            handle_time_data_message(msg_dict, state)
        elif cid == ids['ignition_status']: 
            handle_power_status_message(msg_dict, state)

async def shutdown_monitor_task(state: AppState):
    """Periodic check for shutdown conditions."""
    global RUNNING
    while RUNNING:
        if state.check_shutdown_condition(): 
            RUNNING = False
        await asyncio.sleep(1.0)

async def main_async(state: AppState):
    global RELOAD_CONFIG
    tasks = [
        asyncio.create_task(listen_task(state)), 
        asyncio.create_task(shutdown_monitor_task(state))
    ]
    
    while RUNNING:
        if RELOAD_CONFIG:
            if load_and_initialize_config(): 
                RELOAD_CONFIG = False
        await asyncio.sleep(1)
        
    for task in tasks: 
        task.cancel()

def main():
    if not load_and_initialize_config(): 
        sys.exit(1)
        
    state = AppState()
    state._reset_shutdown_state()
    
    try:
        asyncio.run(main_async(state))
    except KeyboardInterrupt: 
        pass

if __name__ == '__main__':
    main()