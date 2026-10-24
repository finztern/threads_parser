#!/bin/sh
set -eu

Xvfb :99 -screen 0 1360x900x24 &
sleep 1
fluxbox >/dev/null 2>&1 &
x11vnc -display :99 -forever -shared -nopw -quiet -rfbport 5900 &
websockify --web=/usr/share/novnc 6080 localhost:5900 &

sleep 2
echo "[login] noVNC ready on :6080 (open /vnc.html)"

exec python3 /app/scripts/interactive_login.py
