#!/usr/bin/env bash
# Start a CAN Detective session.
# Open a second terminal for snapshot commands while this runs.

INSTALL_DIR="/home/pi/can-detective"
CAN_IFACE="can0"
NET_IFACE="eth0"
PORT=29536

echo ""
echo "=== CAN Detective ==="
echo ""

# Bring up CAN interface
sudo ip link set can0 down 2>/dev/null || true
sudo ip link set can0 up type can bitrate 100000 2>/dev/null || true
sudo ip link set can0 up

ip link show can0 | grep -q "UP" && echo "OK can0 up" || echo "WARNING: can0 not up -- check hardware"

# Start socketcand for SavvyCAN
sudo socketcand -i can0 -l eth0 -p  &
SCAND_PID=
PI_IP=10.20.4.238
echo "OK socketcand running on port "
echo "   SavvyCAN: Remote SocketCAN -> :"

echo ""
echo "Snapshot commands (run in a second terminal):"
echo "  python snapshot.py summary"
echo "  python snapshot.py tv"
echo "  python snapshot.py save <name>"
echo "  python snapshot.py diff <a> <b>"
echo "  python snapshot.py unknown"
echo "  python snapshot.py watch 602"
echo ""

cd "/home/pi/can-detective"
"/home/pi/can-detective/.venv/bin/python" server.py

kill  2>/dev/null || true
