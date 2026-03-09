#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Hudiy Data Extractor (V3.1)
- Handles Media, Navigation, Phone, and TP2 status bridge.
- Publishes HUDIY_MEDIA with media_state + track for DIS compatibility.
"""

import json
import logging
import os
import sys
import threading
import time
from queue import Empty, Queue

import zmq

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    api_path = os.path.join(script_dir, "api_files")
    sys.path.insert(0, api_path)

    from common.Client import Client, ClientEventHandler
    import common.Api_pb2 as hudiy_api
except ImportError as e:
    print(f"FATAL: Could not import Hudiy client libraries: {e}")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Hudiy) %(message)s",
)
logger = logging.getLogger(__name__)

ZMQ_CONTEXT = zmq.Context()

MANEUVER_TYPE_MAP = {
    0: "Unknown",
    1: "Depart",
    2: "Name Change",
    3: "Slight turn",
    4: "Turn",
    5: "Sharp turn",
    6: "U-Turn",
    7: "On Ramp",
    8: "Off Ramp",
    9: "Fork",
    10: "Merge",
    11: "Roundabout",
    12: "Roundabout Exit",
    13: "Roundabout",
    14: "Straight",
    16: "Ferry Boat",
    17: "Ferry Train",
    19: "Destination",
}

MANEUVER_SIDE_MAP = {
    1: "left",
    2: "right",
    3: "",
}

CALL_STATE_MAP = {
    0: "IDLE",
    1: "INCOMING",
    2: "ALERTING",
    3: "ACTIVE",
}

CONN_STATE_MAP = {
    1: "CONNECTED",
    2: "DISCONNECTED",
}

MEDIA_SOURCE_MAP = {
    0: "None",
    1: "Android",
    2: "CarPlay",
    3: "Bluetooth",
    4: "Storage",
    5: "FM-Radio",
    6: "Web",
}


class SafePublisher:
    """Thread-safe ZMQ PUB wrapper."""

    def __init__(self, zmq_addr: str):
        self.queue = Queue()
        self.zmq_addr = zmq_addr
        self.running = True
        self.thread = threading.Thread(target=self._worker, daemon=True, name="zmq-pub")
        self.thread.start()

    def publish(self, topic: bytes, data: dict):
        if self.running:
            self.queue.put((topic, data))

    def _worker(self):
        ctx = zmq.Context()
        pub = ctx.socket(zmq.PUB)
        try:
            pub.bind(self.zmq_addr)
            logger.info("SafePublisher bound to %s", self.zmq_addr)
        except Exception as e:
            logger.critical("SafePublisher BIND FAILED: %s", e)
            return

        while self.running:
            try:
                topic, data = self.queue.get(timeout=1.0)
                pub.send_multipart([topic, json.dumps(data).encode("utf-8")])
                self.queue.task_done()
            except Empty:
                continue
            except Exception as e:
                logger.error("SafePublisher error: %s", e)

        try:
            pub.close()
            ctx.term()
        except Exception:
            pass

    def stop(self):
        self.running = False
        self.thread.join(timeout=2.0)


class HudiyEventHandler(ClientEventHandler):
    def __init__(self, safe_publisher: SafePublisher):
        super().__init__()
        self.safe_pub = safe_publisher
        self.last_media = None

        self.current_media_data = {
            "artist": "",
            "title": "",
            "track": "",
            "album": "",
            "playing": False,
            "media_state": "NONE",  # NONE / PLAYING / PAUSED / IDLE
            "duration": "0:00",
            "position": "0:00",
            "source_id": 0,
            "source_label": "None",
            "projection_active": False,
            "timestamp": 0,
        }

        self.current_nav_data = {}

        self.current_phone_data = {
            "connection_state": "DISCONNECTED",
            "name": "",
            "state": "IDLE",
            "caller_name": "",
            "caller_id": "",
            "battery": 0,
            "signal": 0,
            "timestamp": 0,
        }

    def on_hello_response(self, client, message):
        logger.info(
            "Client '%s' Connected - API v%s.%s",
            client._name,
            message.api_version.major,
            message.api_version.minor,
        )

        subs = hudiy_api.SetStatusSubscriptions()

        if client._name == "MEDIA":
            subs.subscriptions.extend([
                hudiy_api.SetStatusSubscriptions.Subscription.MEDIA,
                hudiy_api.SetStatusSubscriptions.Subscription.PROJECTION,
            ])
            client.send(
                hudiy_api.MESSAGE_SET_STATUS_SUBSCRIPTIONS,
                0,
                subs.SerializeToString(),
            )
            logger.info("Client '%s': Subscribed to MEDIA + PROJECTION", client._name)

        elif client._name == "NAV_PHONE":
            subs.subscriptions.extend([
                hudiy_api.SetStatusSubscriptions.Subscription.NAVIGATION,
                hudiy_api.SetStatusSubscriptions.Subscription.PHONE,
            ])
            client.send(
                hudiy_api.MESSAGE_SET_STATUS_SUBSCRIPTIONS,
                0,
                subs.SerializeToString(),
            )
            logger.info("Client '%s': Subscribed to NAV and PHONE", client._name)

    # ---------------- MEDIA ----------------

    def on_media_metadata(self, client, message):
        title = message.title or ""
        artist = message.artist or ""
        album = message.album or ""
        new_meta = f"{artist}|{title}|{album}"

        self.current_media_data.update({
            "artist": artist,
            "title": title,
            "track": title,
            "album": album,
            "duration": getattr(message, "duration_label", "0:00"),
            "timestamp": time.time(),
        })

        if self.current_media_data.get("source_id", 0) != 0 and (title or artist or album):
            if not self.current_media_data.get("playing", False):
                self.current_media_data["media_state"] = "IDLE"

        if new_meta != self.last_media:
            self.last_media = new_meta
            logger.info("🎵 %s - %s", artist, title)

        self.publish_and_write_media(self.current_media_data)

    def on_media_status(self, client, message):
        pos = getattr(message, "position_label", "0:00")
        src_id = getattr(message, "source", 0)
        src_label = MEDIA_SOURCE_MAP.get(src_id, "Unknown")
        is_playing = bool(getattr(message, "is_playing", False))

        if src_id != self.current_media_data.get("source_id"):
            logger.info("SOURCE CHANGED: %s (%s)", src_label, src_id)

        if src_id == 0:
            media_state = "NONE"
        elif is_playing:
            media_state = "PLAYING"
        else:
            has_meta = any((
                self.current_media_data.get("title", ""),
                self.current_media_data.get("artist", ""),
                self.current_media_data.get("album", ""),
            ))
            media_state = "PAUSED" if has_meta else "IDLE"

        self.current_media_data.update({
            "playing": is_playing,
            "media_state": media_state,
            "position": pos,
            "source_id": src_id,
            "source_label": src_label,
            "timestamp": time.time(),
        })

        self.publish_and_write_media(self.current_media_data)

    def on_projection_status(self, client, message):
        active = bool(getattr(message, "active", False))
        logger.info("PROJECTION STATUS: %s", "Active" if active else "Inactive")

        self.current_media_data["projection_active"] = active
        if not active and self.current_media_data.get("source_id", 0) == 0:
            self.current_media_data["media_state"] = "NONE"

        self.current_media_data["timestamp"] = time.time()
        self.publish_and_write_media(self.current_media_data)

    def publish_and_write_media(self, data: dict):
        try:
            self.safe_pub.publish(b"HUDIY_MEDIA", data)
        except Exception as e:
            logger.error("Failed to publish ZMQ media: %s", e)

        try:
            with open("/tmp/now_playing.json", "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # ---------------- NAV ----------------

    def on_navigation_maneuver_details(self, client, message):
        desc = getattr(message, "description", "")
        type_num = getattr(message, "maneuver_type", 0)
        side_num = getattr(message, "maneuver_side", 3)
        angle_num = getattr(message, "maneuver_angle", 0)

        maneuver_text = MANEUVER_TYPE_MAP.get(type_num, "N/A")
        side_text = MANEUVER_SIDE_MAP.get(side_num, "N/A")
        full_maneuver_text = f"{maneuver_text} {side_text}".strip()

        logger.info("NAV: %s (Angle: %s) - %s", full_maneuver_text, angle_num, desc)

        icon_bytes = getattr(message, "icon", None)
        if icon_bytes:
            try:
                script_dir = os.path.dirname(os.path.abspath(__file__))
                icon_dir = os.path.join(script_dir, "..", "nav_icons")
                os.makedirs(icon_dir, exist_ok=True)

                safe_maneuver = maneuver_text.replace(" ", "_").replace("-", "_").lower()
                safe_side = side_text.replace(" ", "_").lower()

                if safe_side and safe_side != "n/a":
                    icon_filename = (
                        f"{icon_dir}/nav_icon_{type_num}_{safe_maneuver}_"
                        f"{side_num}_{safe_side}.png"
                    )
                else:
                    icon_filename = f"{icon_dir}/nav_icon_{type_num}_{safe_maneuver}.png"

                if not os.path.exists(icon_filename):
                    with open(icon_filename, "wb") as f:
                        f.write(icon_bytes)
                    logger.info(
                        "Saved new NAV icon (%d bytes) to %s",
                        len(icon_bytes),
                        icon_filename,
                    )
            except Exception as e:
                logger.error("Failed to save NAV icon: %s", e)

        self.current_nav_data.update({
            "description": desc,
            "maneuver_text": full_maneuver_text,
            "maneuver_type": type_num,
            "maneuver_side": side_num,
            "maneuver_angle": angle_num,
            "timestamp": time.time(),
        })
        self.publish_and_write_nav(self.current_nav_data)

    def on_navigation_maneuver_distance(self, client, message):
        self.current_nav_data["distance"] = getattr(message, "label", "")
        self.current_nav_data["timestamp"] = time.time()
        self.publish_and_write_nav(self.current_nav_data)

    def on_navigation_status(self, client, message):
        source = getattr(message, "source", 0)
        state = getattr(message, "state", 2)

        status_text = "Active" if state == 1 else "Inactive"
        src_text = "AA" if source == 1 else "None"
        logger.info("NAV STATUS: %s (%s)", status_text, src_text)

        nav_status = {
            "active": state == 1,
            "source": source,
            "state": state,
            "timestamp": time.time(),
        }
        self.publish_nav_status(nav_status)

    def publish_nav_status(self, data: dict):
        try:
            self.safe_pub.publish(b"HUDIY_NAV_STATUS", data)
        except Exception:
            pass

    def publish_and_write_nav(self, data: dict):
        try:
            self.safe_pub.publish(b"HUDIY_NAV", data)
        except Exception:
            pass

        try:
            with open("/tmp/current_nav.json", "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    # ---------------- PHONE ----------------

    def on_phone_connection_status(self, client, message):
        state = CONN_STATE_MAP.get(message.state, "DISCONNECTED")
        name = getattr(message, "name", "")
        logger.info("PHONE CONN: %s: %s", state, name)

        self.current_phone_data.update({
            "connection_state": state,
            "name": name,
            "timestamp": time.time(),
        })
        self.publish_and_write_phone(self.current_phone_data)

    def on_phone_levels_status(self, client, message):
        battery = getattr(message, "bettery_level", getattr(message, "battery_level", 0))
        signal = getattr(message, "signal_level", 0)

        self.current_phone_data.update({
            "battery": battery,
            "signal": signal,
            "timestamp": time.time(),
        })
        self.publish_and_write_phone(self.current_phone_data)

    def on_phone_voice_call_status(self, client, message):
        state = CALL_STATE_MAP.get(message.state, "IDLE")
        caller = getattr(message, "caller_name", "") or getattr(message, "caller_id", "") or "Unknown"
        logger.info("PHONE CALL: %s: %s", state, caller)

        self.current_phone_data.update({
            "state": state,
            "caller_name": getattr(message, "caller_name", ""),
            "caller_id": getattr(message, "caller_id", ""),
            "timestamp": time.time(),
        })
        self.publish_and_write_phone(self.current_phone_data)

    def publish_and_write_phone(self, data: dict):
        try:
            self.safe_pub.publish(b"HUDIY_PHONE", data)
        except Exception:
            pass

        try:
            with open("/tmp/current_call.json", "w") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass


class TP2BridgeHandler(ClientEventHandler):
    """
    Bridges Hudiy UI Actions/Icons with the TP2 ZMQ service.
    """

    def __init__(self, zmq_req_addr: str):
        super().__init__()
        self.zmq_addr = zmq_req_addr
        self.socket = None
        self.lock = threading.Lock()
        self.icon_id = None
        self.icon_visible = False
        self.running = True
        self.timer = None

    def init_socket(self):
        with self.lock:
            if self.socket:
                self.socket.close()
            self.socket = ZMQ_CONTEXT.socket(zmq.REQ)
            self.socket.connect(self.zmq_addr)
            self.socket.setsockopt(zmq.RCVTIMEO, 3000)
            self.socket.setsockopt(zmq.LINGER, 0)
            logger.info("TP2 Bridge: Connected to ZMQ REQ %s", self.zmq_addr)

    def stop(self):
        self.running = False
        if self.timer:
            self.timer.cancel()
        with self.lock:
            if self.socket:
                self.socket.close()
                self.socket = None

    def on_hello_response(self, client, message):
        logger.info(
            "TP2 Bridge Connected to Hudiy: v%s.%s",
            message.api_version.major,
            message.api_version.minor,
        )

        req_act = hudiy_api.RegisterActionRequest()
        req_act.action = "toggle_diagnostics"
        client.send(hudiy_api.MESSAGE_REGISTER_ACTION_REQUEST, 0, req_act.SerializeToString())

        req_act_update = hudiy_api.RegisterActionRequest()
        req_act_update.action = "update_rnse"
        client.send(hudiy_api.MESSAGE_REGISTER_ACTION_REQUEST, 0, req_act_update.SerializeToString())

        req_icon = hudiy_api.RegisterStatusIconRequest()
        req_icon.description = "Diagnostics Active"
        req_icon.icon_font_family = "Material Symbols Rounded"
        req_icon.icon_name = "car_repair"
        client.send(
            hudiy_api.MESSAGE_REGISTER_STATUS_ICON_REQUEST,
            0,
            req_icon.SerializeToString(),
        )

    def on_register_action_response(self, client, message):
        logger.info("Action '%s' Registered: %s", message.action, message.result)

    def on_register_status_icon_response(self, client, message):
        if message.result == 1:
            self.icon_id = message.id
            logger.info("Status Icon Registered. ID: %s", self.icon_id)
            self.poll_status(client)
        else:
            logger.error("Failed to register Status Icon")

    def on_dispatch_action(self, client, message):
        if message.action == "toggle_diagnostics":
            logger.info("Hudiy Action: Toggle Diagnostics")
            self.send_command("TOGGLE")
            self.check_status_now(client)

        elif message.action == "update_rnse":
            logger.info("Hudiy Action: Update RNS-E")
            import glob

            req_quit = hudiy_api.DispatchAction()
            req_quit.action = "quit_hudiy"
            client.send(hudiy_api.MESSAGE_DISPATCH_ACTION, 0, req_quit.SerializeToString())

            script_dir = os.path.dirname(os.path.abspath(__file__))
            updater_script = os.path.join(script_dir, "update_rnse.sh")
            env = os.environ.copy()

            if "XDG_RUNTIME_DIR" not in env:
                env["XDG_RUNTIME_DIR"] = "/run/user/1000"

            if "WAYLAND_DISPLAY" not in env:
                w_sock = glob.glob(os.path.join(env["XDG_RUNTIME_DIR"], "wayland-*"))
                w_sock = [s for s in w_sock if not s.endswith(".lock")]
                env["WAYLAND_DISPLAY"] = os.path.basename(w_sock[0]) if w_sock else "wayland-1"

            try:
                import subprocess
                subprocess.Popen(["foot", "--fullscreen", "bash", updater_script], env=env)
            except Exception as e:
                logger.error("Failed to launch updater terminal: %s", e)

    def send_command(self, cmd):
        with self.lock:
            if not self.socket:
                logger.warning("TP2 Socket not initialized in send_command")
                return None

            try:
                self.socket.send_json({"cmd": cmd})
                return self.socket.recv_json()
            except zmq.Again:
                logger.warning("TP2 ZMQ Request Timeout or Busy")
                self.socket.close()
                self.socket = ZMQ_CONTEXT.socket(zmq.REQ)
                self.socket.connect(self.zmq_addr)
                self.socket.setsockopt(zmq.RCVTIMEO, 3000)
                self.socket.setsockopt(zmq.LINGER, 0)
                return None
            except Exception as e:
                logger.error("TP2 ZMQ Error: %s", e)
                return None

    def check_status_now(self, client):
        resp = self.send_command("STATUS")
        if resp and "enabled" in resp:
            enabled = resp["enabled"]
            target_visible = enabled

            if self.icon_id is not None and target_visible != self.icon_visible:
                self.icon_visible = target_visible
                msg = hudiy_api.ChangeStatusIconState()
                msg.id = self.icon_id
                msg.visible = self.icon_visible
                client.send(
                    hudiy_api.MESSAGE_CHANGE_STATUS_ICON_STATE,
                    0,
                    msg.SerializeToString(),
                )
                logger.info("Updated Icon Visibility: %s", self.icon_visible)

    def poll_status(self, client):
        if not self.running:
            return
        self.check_status_now(client)
        self.timer = threading.Timer(2.0, self.poll_status, [client])
        self.timer.start()


class HudiyData:
    def __init__(self, config_path="/home/pi/config.json"):
        config = None
        try:
            with open(config_path, "r") as f:
                config = json.load(f)
            _zmq = config.get("interfaces", {}).get("zmq", {})
            zmq_addr = _zmq.get("metric_stream", "ipc:///run/rnse_control/hudiy_stream.ipc")
        except Exception as e:
            logger.warning("Config Error: %s. Using default ZMQ address.", e)
            zmq_addr = "ipc:///run/rnse_control/hudiy_stream.ipc"

        self.safe_pub = SafePublisher(zmq_addr)
        self.handler = HudiyEventHandler(self.safe_pub)

        self.media_client = None
        self.nav_client = None

        self.tp2_zmq_addr = "ipc:///run/rnse_control/tp2_cmd.ipc"
        if config and "interfaces" in config:
            _zmq = config["interfaces"].get("zmq", {})
            self.tp2_zmq_addr = _zmq.get("tp2_command", self.tp2_zmq_addr)

        self.tp2_handler = TP2BridgeHandler(self.tp2_zmq_addr)
        self.tp2_client = None
        self.running = True

    def connect_media(self):
        while self.running:
            try:
                self.media_client = Client("MEDIA")
                self.media_client.set_event_handler(self.handler)
                self.media_client.connect("127.0.0.1", 44406, use_websocket=True)
                logger.info("MEDIA Thread ACTIVE")

                while self.media_client._connected and self.running:
                    if not self.media_client.wait_for_message():
                        break
            except Exception as e:
                logger.error("MEDIA Thread: %s", e)

            if self.media_client:
                self.media_client.disconnect()

            if self.running:
                logger.info("MEDIA Reconnecting in 5s...")
                time.sleep(5)

    def connect_nav(self):
        while self.running:
            try:
                self.nav_client = Client("NAV_PHONE")
                self.nav_client.set_event_handler(self.handler)
                self.nav_client.connect("127.0.0.1", 44405)
                logger.info("NAV_THREAD ACTIVE")

                while self.nav_client._connected and self.running:
                    if not self.nav_client.wait_for_message():
                        break
            except Exception as e:
                logger.error("NAV Thread: %s", e)

            if self.nav_client:
                self.nav_client.disconnect()

            if self.running:
                logger.info("NAV Reconnecting in 5s...")
                time.sleep(5)

    def connect_tp2(self):
        while self.running:
            try:
                self.tp2_client = Client("TP2_BRIDGE")
                self.tp2_client.set_event_handler(self.tp2_handler)
                self.tp2_client.connect("127.0.0.1", 44405)
                logger.info("TP2_BRIDGE Thread ACTIVE")

                self.tp2_handler.init_socket()

                while self.tp2_client._connected and self.running:
                    if not self.tp2_client.wait_for_message():
                        break
            except Exception as e:
                logger.error("TP2 Bridge Thread: %s", e)

            if self.tp2_client:
                self.tp2_client.disconnect()

            self.tp2_handler.stop()

            if self.running:
                logger.info("TP2 Bridge Reconnecting in 5s...")
                time.sleep(5)
                self.tp2_handler = TP2BridgeHandler(self.tp2_zmq_addr)

    def run(self):
        logger.info("THREADING Hudiy Data ACTIVE!")

        media_thread = threading.Thread(target=self.connect_media, daemon=True, name="hudiy-media")
        nav_thread = threading.Thread(target=self.connect_nav, daemon=True, name="hudiy-nav")
        tp2_thread = threading.Thread(target=self.connect_tp2, daemon=True, name="hudiy-tp2")

        media_thread.start()
        nav_thread.start()
        tp2_thread.start()

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Stopped by user (KeyboardInterrupt)")
            self.running = False

        if self.media_client:
            self.media_client.disconnect()
        if self.nav_client:
            self.nav_client.disconnect()
        if self.tp2_client:
            self.tp2_client.disconnect()

        self.tp2_handler.stop()

        media_thread.join(timeout=2.0)
        nav_thread.join(timeout=2.0)
        tp2_thread.join(timeout=2.0)

        self.safe_pub.stop()
        logger.info("ZMQ publisher closed.")


if __name__ == "__main__":
    try:
        HudiyData(config_path="/home/pi/config.json").run()
    except Exception as e:
        logger.critical("Unhandled exception in main: %s", e, exc_info=True)
    finally:
        ZMQ_CONTEXT.term()
        logger.info("HudiyData service has shut down.")
