#!/bin/sh
set -eu

if [ -z "${VLESS_URI:-}" ]; then
  echo "[xray] VLESS_URI env var is required (one vless:// line from the subscription)" >&2
  exit 1
fi

mkdir -p /etc/xray
python3 /opt/render_config.py "$VLESS_URI" > /etc/xray/config.json

echo "[xray] config rendered, starting xray..."
exec xray run -config /etc/xray/config.json
