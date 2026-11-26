"""Subscription -> sing-box: parse vless:// and hysteria2:// links and build sing-box configs.

Pure functions (no network, no processes), so they can be tested offline:
  parse_subscription(text)  -> list[Node]   (a list of links, plain or base64; unknown schemes are skipped)
  build_test_config(nodes, ports)            one sing-box with a loopback SOCKS inbound per node (to measure all nodes at once)
  build_run_config(node, port)               one inbound, one outbound: the proxy the SSH tunnels go through
Nothing from a link (user id, password, keys) is logged; `Node.name` is the public label after '#'.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import re
import socket
from dataclasses import dataclass, field
from urllib.parse import parse_qs, unquote, urlsplit

_SCHEMES = ("vless://", "hysteria2://", "hy2://")
_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_FINGERPRINTS = {"chrome", "firefox", "safari", "ios", "android", "edge", "360", "qq", "random", "randomized"}


@dataclass
class Node:
    name: str
    kind: str  # "vless" | "hysteria2"
    outbound: dict  # sing-box outbound WITHOUT "tag"
    key: str = field(default="")  # stable id of the node (hash of server/port/credentials), never logged

    def label(self) -> str:
        return f"{self.name[:40] or 'node'} [{self.kind}]"


def _first(query: dict[str, list[str]], *names: str) -> str:
    for name in names:
        values = query.get(name)
        if values and values[0] != "":
            return values[0]
    return ""


def _truthy(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes")


def _split_authority(rest: str) -> tuple[str, str, str]:
    """'user@host:port/path?query#frag' -> (userinfo, host, port_part). Handles [ipv6] and port lists."""
    authority = re.split(r"[/?#]", rest, maxsplit=1)[0]
    userinfo, _, hostport = authority.rpartition("@")
    if hostport.startswith("["):
        host, _, tail = hostport[1:].partition("]")
        port = tail[1:] if tail.startswith(":") else ""
    else:
        host, _, port = hostport.partition(":")
    return unquote(userinfo), host, port


def _query_of(link: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(link).query, keep_blank_values=True)


def _name_of(link: str) -> str:
    return unquote(urlsplit(link).fragment).strip()


def _tls_block(query: dict[str, list[str]], host: str, security: str) -> dict | None:
    sni = _first(query, "sni", "peer", "host") or host
    insecure = _truthy(_first(query, "allowInsecure", "insecure", "allow_insecure"))
    alpn = [a for a in _first(query, "alpn").split(",") if a]
    fingerprint = _first(query, "fp") or "chrome"
    if fingerprint not in _FINGERPRINTS:
        fingerprint = "chrome"
    if security == "reality":
        public_key = _first(query, "pbk")
        if not public_key:
            return None
        return {
            "enabled": True,
            "server_name": sni,
            "utls": {"enabled": True, "fingerprint": fingerprint},
            "reality": {"enabled": True, "public_key": public_key, "short_id": _first(query, "sid")},
        }
    block: dict = {"enabled": True, "server_name": sni, "insecure": insecure}
    if alpn:
        block["alpn"] = alpn
    if _first(query, "fp"):
        block["utls"] = {"enabled": True, "fingerprint": fingerprint}
    return block


def _vless(link: str) -> Node | None:
    userinfo, host, port_text = _split_authority(link[len("vless://"):])
    if not host or not port_text.isdigit() or not _UUID.match(userinfo):
        return None
    query = _query_of(link)
    security = (_first(query, "security") or "none").lower()
    transport_type = (_first(query, "type") or "tcp").lower()
    if security not in ("none", "tls", "reality"):
        return None
    if _first(query, "encryption") not in ("", "none"):
        return None
    outbound: dict = {"type": "vless", "server": host, "server_port": int(port_text), "uuid": userinfo}
    if security != "none":
        tls = _tls_block(query, host, security)
        if tls is None:
            return None
        outbound["tls"] = tls
    path = unquote(_first(query, "path")) or "/"
    header_host = _first(query, "host")
    if transport_type == "tcp":
        if _first(query, "headerType") not in ("", "none"):
            return None  # HTTP header obfuscation is not supported by sing-box
        flow = _first(query, "flow")
        if flow:
            outbound["flow"] = flow
    elif transport_type == "grpc":
        outbound["transport"] = {"type": "grpc", "service_name": unquote(_first(query, "serviceName"))}
    elif transport_type == "ws":
        transport: dict = {"type": "ws", "path": path}
        if header_host:
            transport["headers"] = {"Host": header_host}
        outbound["transport"] = transport
    elif transport_type == "httpupgrade":
        outbound["transport"] = {"type": "httpupgrade", "path": path, "host": header_host or host}
    elif transport_type in ("h2", "http"):
        if "tls" not in outbound:
            return None
        outbound["transport"] = {"type": "http", "host": [header_host or host], "path": path}
    else:
        return None  # xhttp / splithttp / kcp / quic: not available in sing-box
    return Node(_name_of(link) or host, "vless", outbound)


def _hysteria2(link: str) -> Node | None:
    scheme_len = len("hysteria2://") if link.startswith("hysteria2://") else len("hy2://")
    password, host, port_text = _split_authority(link[scheme_len:])
    if not host or not password:
        return None
    query = _query_of(link)
    outbound: dict = {"type": "hysteria2", "server": host, "password": password}
    ports = [p for p in re.split(r"[,]", port_text) if p]
    single = [p for p in ports if p.isdigit()]
    ranges = [p.replace("-", ":") for p in ports if re.fullmatch(r"\d+[-:]\d+", p)]
    mport = _first(query, "mport")
    if mport:
        ranges += [p.replace("-", ":") for p in mport.split(",") if re.fullmatch(r"\d+[-:]\d+", p)]
        single += [p for p in mport.split(",") if p.isdigit()]
    if single:
        outbound["server_port"] = int(single[0])
    elif not ranges:
        outbound["server_port"] = 443
    if ranges:
        outbound["server_ports"] = ranges
    tls: dict = {
        "enabled": True,
        "insecure": _truthy(_first(query, "insecure", "allowInsecure")),
        "alpn": [a for a in (_first(query, "alpn") or "h3").split(",") if a],
    }
    sni = _first(query, "sni", "peer")
    if sni:
        tls["server_name"] = sni
    outbound["tls"] = tls
    obfs = _first(query, "obfs")
    if obfs:
        if obfs != "salamander":
            return None
        outbound["obfs"] = {"type": "salamander", "password": _first(query, "obfs-password")}
    return Node(_name_of(link) or host, "hysteria2", outbound)


def _decode_base64(text: str) -> str | None:
    compact = re.sub(r"\s+", "", text)
    if not compact or not re.fullmatch(r"[A-Za-z0-9+/_=-]+", compact):
        return None
    compact += "=" * (-len(compact) % 4)
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            return decoder(compact).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            continue
    return None


def _links(text: str, depth: int = 0) -> list[str]:
    found: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(_SCHEMES):
            found.append(line)
        elif depth == 0 and not line.startswith("#"):
            decoded = _decode_base64(line)  # a whole base64 document or one base64 line
            if decoded and "://" in decoded:
                found.extend(_links(decoded, depth + 1))
    return found


def parse_subscription(text: str) -> list[Node]:
    """All usable vless/hysteria2 nodes of a subscription body (duplicates and unsupported variants are dropped)."""
    nodes: dict[str, Node] = {}
    for link in _links(text):
        try:
            node = _vless(link) if link.startswith("vless://") else _hysteria2(link)
        except (ValueError, IndexError):
            node = None
        if node is None:
            continue
        ob = node.outbound
        secret = ob.get("uuid") or ob.get("password") or ""
        node.key = hashlib.sha1(f"{ob['server']}|{ob.get('server_port')}|{ob.get('server_ports')}|{secret}".encode()).hexdigest()[:16]
        nodes.setdefault(node.key, node)
    return list(nodes.values())


def free_ports(count: int) -> list[int]:
    """`count` free loopback ports chosen by the OS (so nothing fixed can ever clash with other programs on the host)."""
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def _config(inbounds: list[dict], outbounds: list[dict], rules: list[dict]) -> dict:
    return {
        "log": {"level": "error", "timestamp": False},
        "inbounds": inbounds,
        "outbounds": outbounds + [{"type": "direct", "tag": "direct"}],
        "route": {"rules": rules, "final": "direct"},
    }


def build_test_config(nodes: list[Node], ports: list[int]) -> dict:
    """One sing-box for many nodes: inbound i (127.0.0.1:ports[i]) goes ONLY through outbound i."""
    inbounds, outbounds, rules = [], [], []
    for i, (node, port) in enumerate(zip(nodes, ports)):
        inbounds.append({"type": "socks", "tag": f"in{i}", "listen": "127.0.0.1", "listen_port": port})
        outbounds.append({**node.outbound, "tag": f"out{i}"})
        rules.append({"inbound": [f"in{i}"], "action": "route", "outbound": f"out{i}"})
    return _config(inbounds, outbounds, rules)


def build_run_config(node: Node, port: int) -> dict:
    """The working proxy: one loopback SOCKS inbound through the chosen node."""
    return build_test_config([node], [port])
