#!/usr/bin/env python3
from flask import Flask
import subprocess
import os

app = Flask(__name__)

# --- Function to launch an application ---
def run_app(cmd):
    env = os.environ.copy()

    # Required for GUI apps on Wayland (RPi5)
    env["WAYLAND_DISPLAY"] = "wayland-0"
    env["XDG_RUNTIME_DIR"] = "/run/user/1000"

    subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )

# --- Power endpoints ---
@app.route("/shutdown")
def shutdown():
    subprocess.Popen(["sudo", "shutdown", "-h", "now"])
    return "Shutdown command sent!", 200

@app.route("/reboot")
def reboot():
    subprocess.Popen(["sudo", "reboot"])
    return "Reboot command sent!", 200

# --- Run Flask server ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=44408)
