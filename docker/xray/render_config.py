#!/usr/bin/env python3
"""Render an Xray-core client config.json from a single vless:// URI.

Usage: render_config.py 'vless://<uuid>@<host>:<port>?...&sni=...#tag'
stdlib only - runs inside the xray container.
"""
from __future__ import annotations

import json
import sys
from urllib.parse import parse_qs, unquote, urlsplit

SOCKS_LISTEN_PORT = 1080


def parse_vless_uri(uri: str) -> dict:
    parts = urlsplit(uri)
    if parts.scheme != "vless":
        raise ValueError("not a vless:// URI")

    uuid = unquote(parts.username or "")
    address = parts.hostname
    port = parts.port
    if not uuid or not address or not port:
        raise ValueError("incomplete vless URI (need uuid@host:port)")

    query = parse_qs(parts.query)

    def q(name: str, default: str = "") -> str:
        return unquote(query.get(name, [default])[0])

    return {
        "uuid": uuid,
        "address": address,
        "port": port,
        "security": q("security", "tls") or "tls",
        "network": q("type", "tcp") or "tcp",
        "header_type": q("headerType", ""),
        "path": q("path", "/") or "/",
        "host_header": q("host", address) or address,
        "sni": q("sni", address) or address,
        "fingerprint": q("fp", "chrome") or "chrome",
    }


def build_xray_config(parsed: dict) -> dict:
    stream: dict = {"network": parsed["network"], "security": parsed["security"]}

    if parsed["security"] == "tls":
        stream["tlsSettings"] = {
            "serverName": parsed["sni"],
            "fingerprint": parsed["fingerprint"],
            "allowInsecure": False,
        }

    if parsed["network"] == "httpupgrade":
        stream["httpupgradeSettings"] = {"path": parsed["path"], "host": parsed["host_header"]}
    elif parsed["network"] == "ws":
        stream["wsSettings"] = {"path": parsed["path"], "headers": {"Host": parsed["host_header"]}}
    elif parsed["network"] == "tcp" and parsed["header_type"] == "http":
        stream["tcpSettings"] = {
            "header": {
                "type": "http",
                "request": {
                    "path": [parsed["path"]],
                    "headers": {"Host": [parsed["host_header"]]},
                },
            }
        }

    return {
        "log": {"loglevel": "warning"},
        "inbounds": [
            {
                "listen": "0.0.0.0",
                "port": SOCKS_LISTEN_PORT,
                "protocol": "socks",
                "settings": {"auth": "noauth", "udp": True},
                "tag": "socks-in",
            }
        ],
        "outbounds": [
            {
                "protocol": "vless",
                "tag": "vpn-out",
                "settings": {
                    "vnext": [
                        {
                            "address": parsed["address"],
                            "port": parsed["port"],
                            "users": [{"id": parsed["uuid"], "encryption": "none"}],
                        }
                    ]
                },
                "streamSettings": stream,
            },
            {"protocol": "blackhole", "tag": "block"},
        ],
        # Everything goes out via the VPN; no "direct" outbound on purpose,
        # so a broken tunnel fails closed instead of leaking the host IP.
        "routing": {"rules": [{"type": "field", "network": "tcp,udp", "outboundTag": "vpn-out"}]},
    }


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: render_config.py '<vless-uri>'", file=sys.stderr)
        sys.exit(1)
    print(json.dumps(build_xray_config(parse_vless_uri(sys.argv[1].strip())), indent=2))


if __name__ == "__main__":
    main()
