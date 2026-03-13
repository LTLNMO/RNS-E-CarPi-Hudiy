#!/usr/bin/env python3
#
# settings_app.py (Full Version)
#
import json
import os
import subprocess
from flask import Flask, jsonify, render_template, request

# --- Configuration ---
CONFIG_PATH = '/home/pi/config.json'
CONFIG_BACKUP_PATH = '/home/pi/config.json.bak'
SERVICES_TO_RESTART = ['can_keyboard_control.service', 'crankshaft_can_features.service']

app = Flask(__name__, template_folder='.')

# --- Helper Functions (Unchanged) ---
def run_shell_command(command):
    try:
        print(f"Executing: {' '.join(command)}")
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        return True, result.stdout
    except Exception as e:
        print(f"ERROR executing command: {e}")
        return False, str(e)

def set_filesystem_rw(writable=True):
    return run_shell_command(['sudo', 'mount', '-o', f'remount,{"rw" if writable else "ro"}', '/'])

def restart_services():
    for service in SERVICES_TO_RESTART:
        run_shell_command(['sudo', 'systemctl', 'restart', service])

# --- API Endpoints ---
@app.route('/api/config', methods=['GET', 'POST'])
def handle_config():
    if request.method == 'POST':
        # --- SAVE LOGIC ---
        new_config = request.json
        if not new_config: return jsonify({"error": "No data received"}), 400
        
        success, msg = set_filesystem_rw(True)
        if not success: return jsonify({"error": "Failed to set filesystem to RW", "details": msg}), 500
        
        if os.path.exists(CONFIG_PATH):
            os.rename(CONFIG_PATH, CONFIG_BACKUP_PATH)
            
        try:
            with open(CONFIG_PATH, 'w') as f:
                json.dump(new_config, f, indent=2)
        except Exception as e:
            if os.path.exists(CONFIG_BACKUP_PATH):
                os.rename(CONFIG_BACKUP_PATH, CONFIG_PATH)
            set_filesystem_rw(False)
            return jsonify({"error": f"Failed to write config: {e}"}), 500
            
        set_filesystem_rw(False)
        restart_services()
        return jsonify({"success": True, "message": "Configuration saved and services restarted."})
    else:
        # --- GET LOGIC ---
        try:
            with open(CONFIG_PATH, 'r') as f:
                return jsonify(json.load(f))
        except Exception as e:
            return jsonify({"error": str(e)}), 500

@app.route('/api/reset', methods=['POST'])
def reset_config():
    if not os.path.exists(CONFIG_BACKUP_PATH):
        return jsonify({"error": "No backup file found."}), 404
    success, msg = set_filesystem_rw(True)
    if not success: return jsonify({"error": "Failed to set filesystem to RW", "details": msg}), 500
    try:
        os.rename(CONFIG_BACKUP_PATH, CONFIG_PATH)
    except Exception as e:
        set_filesystem_rw(False)
        return jsonify({"error": f"Failed to restore backup: {e}"}), 500
    set_filesystem_rw(False)
    restart_services()
    return jsonify({"success": True, "message": "Configuration restored and services restarted."})

@app.route('/api/valid_keys', methods=['GET'])
def get_valid_keys():
    import uinput
    keys = [k for k in dir(uinput) if k.startswith('KEY_') or k.startswith('BTN_')]
    return jsonify(sorted(keys))

@app.route('/api/timezones', methods=['GET'])
def get_timezones():
    try:
        tz_base = "/usr/share/zoneinfo"
        zones = []

        for root, _, files in os.walk(tz_base):
            for name in files:
                rel = os.path.relpath(os.path.join(root, name), tz_base)
                if rel.startswith(("posix", "right", "Etc")):
                    continue
                if "/" in rel:
                    zones.append(rel)

        return jsonify(sorted(set(zones)))
    except Exception:
        # Fallback if filesystem not available
        return jsonify([
            "UTC", "Europe/London", "Europe/Berlin", "Europe/Paris",
            "America/New_York", "America/Chicago", "America/Denver",
            "America/Los_Angeles", "Asia/Tokyo", "Australia/Sydney"
        ])

# --- Frontend Serving ---
@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)