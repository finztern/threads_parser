"""VPN nodes (vless:// links) for the url-check requests.

vpn_nodes.txt holds one vless:// link per line. At start an Xray process is spawned with one local
SOCKS5 inbound per node (127.0.0.1:<base_port + i>), the nodes are benchmarked with the real
url-check call, and the fastest one is used. When the active node fails, the next best one takes
over at once; failed nodes are re-tested in the background with growing back-off.

The links contain a user id: keep vpn_nodes.txt out of git (see .gitignore) and never log links.
"""
from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import parse_qs, unquote, urlsplit

from scraper.constants import LOGGER

# Harmless real query used to measure a node: the answer is a JSON list of already known urls.
PROBE_URLS = ["https://www.threads.net/"]

_NETWORKS = {"tcp": "tcp", "raw": "tcp", "ws": "ws", "httpupgrade": "httpupgrade", "grpc": "grpc",
             "h2": "http", "http": "http", "kcp": "kcp", "splithttp": "splithttp", "xhttp": "xhttp"}


@dataclass(frozen=True)
class VlessNode:
    name: str
    uuid: str
    host: str
    port: int
    params: dict[str, str] = field(default_factory=dict)

    @property
    def label(self) -> str:
        """Safe to log: no user id."""
        return f"{self.name} ({self.host}:{self.port})"


def parse_vless(link: str) -> VlessNode:
    parts = urlsplit(link.strip())
    if parts.scheme != "vless":
        raise ValueError("not a vless:// link")
    if not parts.username or not parts.hostname or not parts.port:
        raise ValueError("vless link needs id@host:port")
    params = {k: v[0] for k, v in parse_qs(parts.query, keep_blank_values=True).items()}
    name = unquote(parts.fragment) or f"{parts.hostname}:{parts.port}"
    return VlessNode(name=name, uuid=unquote(parts.username), host=parts.hostname, port=parts.port, params=params)


def load_nodes(path: Path) -> list[VlessNode]:
    """Nodes from a file; bad lines are skipped with a warning (never echoing the link)."""
    if not path.is_file():
        return []
    nodes: list[VlessNode] = []
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            nodes.append(parse_vless(line))
        except ValueError as exc:
            LOGGER.warning("vpn nodes file: line %d skipped (%s)", number, exc)
    return nodes


def build_outbound(node: VlessNode, tag: str) -> dict[str, Any]:
    p = node.params
    network = _NETWORKS.get((p.get("type") or "tcp").lower(), "tcp")
    security = (p.get("security") or "none").lower()
    path = p.get("path") or "/"
    host_header = p.get("host") or ""

    user: dict[str, Any] = {"id": node.uuid, "encryption": p.get("encryption") or "none"}
    if p.get("flow"):
        user["flow"] = p["flow"]

    stream: dict[str, Any] = {"network": network, "security": security}
    if security == "tls":
        tls: dict[str, Any] = {"serverName": p.get("sni") or host_header or node.host}
        if p.get("fp"):
            tls["fingerprint"] = p["fp"]
        if p.get("alpn"):
            tls["alpn"] = p["alpn"].split(",")
        stream["tlsSettings"] = tls
    elif security == "reality":
        stream["realitySettings"] = {
            "serverName": p.get("sni") or node.host,
            "fingerprint": p.get("fp") or "chrome",
            "publicKey": p.get("pbk", ""),
            "shortId": p.get("sid", ""),
            "spiderX": p.get("spx", ""),
        }

    if network == "ws":
        stream["wsSettings"] = {"path": path, "headers": {"Host": host_header} if host_header else {}}
    elif network == "httpupgrade":
        stream["httpupgradeSettings"] = {"path": path, "host": host_header}
    elif network in ("splithttp", "xhttp"):
        stream[f"{network}Settings"] = {"path": path, "host": host_header, "mode": p.get("mode") or "auto"}
    elif network == "grpc":
        stream["grpcSettings"] = {
            "serviceName": p.get("serviceName") or p.get("path") or "",
            "multiMode": p.get("mode") == "multi",
        }
    elif network == "http":
        stream["httpSettings"] = {"path": path, "host": [host_header] if host_header else []}
    elif network == "tcp" and (p.get("headerType") or "none") == "http":
        stream["tcpSettings"] = {
            "header": {
                "type": "http",
                "request": {"path": [path], "headers": {"Host": [host_header] if host_header else []}},
            }
        }

    return {
        "tag": tag,
        "protocol": "vless",
        "settings": {"vnext": [{"address": node.host, "port": node.port, "users": [user]}]},
        "streamSettings": stream,
    }


def build_xray_config(nodes: list[VlessNode], base_port: int) -> dict[str, Any]:
    """One local SOCKS5 inbound per node; inbound i is routed only to outbound i."""
    inbounds, outbounds, rules = [], [], []
    for i, node in enumerate(nodes):
        inbounds.append({
            "tag": f"in{i}", "listen": "127.0.0.1", "port": base_port + i, "protocol": "socks",
            "settings": {"auth": "noauth", "udp": False},
        })
        outbounds.append(build_outbound(node, f"out{i}"))
        rules.append({"type": "field", "inboundTag": [f"in{i}"], "outboundTag": f"out{i}"})
    outbounds.append({"tag": "block", "protocol": "blackhole"})
    return {
        "log": {"loglevel": "warning"},
        "inbounds": inbounds,
        "outbounds": outbounds,
        "routing": {"domainStrategy": "AsIs", "rules": rules},
    }


Probe = Callable[[str], Awaitable[Any]]  # (proxy_url) -> raises on failure


class _NodeState:
    __slots__ = ("node", "port", "latency", "down_until", "fails")

    def __init__(self, node: VlessNode, port: int) -> None:
        self.node = node
        self.port = port
        self.latency: float | None = None  # seconds, smoothed
        self.down_until = 0.0
        self.fails = 0

    @property
    def proxy(self) -> str:
        return f"socks5h://127.0.0.1:{self.port}"  # "h": the node resolves host names


class VpnPool:
    def __init__(
        self,
        nodes: list[VlessNode],
        base_port: int = 10800,
        probe: Probe | None = None,
        rebench_seconds: float = 600.0,
        xray_bin: str | None = None,
    ) -> None:
        if not nodes:
            raise ValueError("no vpn nodes")
        self._states = [_NodeState(n, base_port + i) for i, n in enumerate(nodes)]
        self._base_port = base_port
        self._probe = probe
        self._rebench = rebench_seconds
        self._xray_bin = xray_bin or os.getenv("XRAY_BIN") or shutil.which("xray")
        self._proc: asyncio.subprocess.Process | None = None
        self._config_dir: str | None = None
        self._active: _NodeState | None = None
        self._task: asyncio.Task | None = None

    def __len__(self) -> int:
        return len(self._states)

    # --- selection -------------------------------------------------------------------------------
    def _alive(self) -> list[_NodeState]:
        now = time.monotonic()
        return [s for s in self._states if s.down_until <= now and s.latency is not None]

    def _pick_best(self) -> _NodeState | None:
        alive = self._alive()
        return min(alive, key=lambda s: s.latency) if alive else None

    def acquire(self) -> _NodeState:
        """The node to use right now (the fastest healthy one). Raises when none is usable."""
        active = self._active
        if active is None or active.down_until > time.monotonic():
            active = self._active = self._pick_best()
            if active is not None:
                LOGGER.info("vpn: using %s (%.2fs)", active.node.label, active.latency or 0.0)
        if active is None:
            raise RuntimeError("no healthy vpn node")
        return active

    def report_success(self, state: _NodeState, seconds: float) -> None:
        state.fails = 0
        state.latency = seconds if state.latency is None else 0.7 * state.latency + 0.3 * seconds

    def report_failure(self, state: _NodeState) -> None:
        now = time.monotonic()
        if state.down_until > now:  # parallel requests failing on the same node: count it once
            return
        state.fails += 1
        state.down_until = now + min(20.0 * 2 ** (state.fails - 1), 600.0)
        LOGGER.warning("vpn: node %s failed (%d in a row), switching", state.node.label, state.fails)
        if self._active is state:
            self._active = None  # next acquire() picks the best of the others

    # --- benchmark -------------------------------------------------------------------------------
    async def _measure(self, state: _NodeState, rounds: int = 2) -> None:
        assert self._probe is not None
        best: float | None = None
        for _ in range(rounds):
            t0 = time.monotonic()
            try:
                await self._probe(state.proxy)
            except Exception as exc:
                LOGGER.debug("vpn probe %s failed: %s", state.node.label, exc)
                self.report_failure(state)
                return
            elapsed = time.monotonic() - t0
            best = elapsed if best is None else min(best, elapsed)
        state.fails = 0
        state.down_until = 0.0
        state.latency = best

    async def benchmark(self) -> None:
        sem = asyncio.Semaphore(8)

        async def one(s: _NodeState) -> None:
            async with sem:
                await self._measure(s)

        await asyncio.gather(*(one(s) for s in self._states))

    async def _maintain(self) -> None:
        last_full = time.monotonic()
        while True:
            await asyncio.sleep(15.0)
            if self._proc is not None and self._proc.returncode is not None:
                LOGGER.warning("vpn: xray exited (%s), restarting", self._proc.returncode)
                try:
                    await self._spawn()
                except Exception as exc:
                    LOGGER.error("vpn: cannot restart xray: %s", exc)
                    continue
            now = time.monotonic()
            if now - last_full >= self._rebench:
                last_full = now
                before = self._active
                await self.benchmark()  # full re-measure of every node, down ones included
                best = self._pick_best()
                # Switch only for a clearly faster node (no flapping on noise).
                if before is not None and best is not None and best is not before and (before.latency or 9e9) > best.latency * 1.25:
                    self._active = best
                    LOGGER.info("vpn: switched to faster %s (%.2fs)", best.node.label, best.latency)
            else:
                # Retry nodes whose back-off has expired.
                due = [s for s in self._states if s.down_until and s.down_until <= now]
                if due:
                    await asyncio.gather(*(self._measure(s, rounds=1) for s in due))

    # --- xray process ----------------------------------------------------------------------------
    async def _spawn(self) -> None:
        if not self._xray_bin:
            raise RuntimeError("xray binary not found (set XRAY_BIN or install xray)")
        if self._config_dir is None:
            self._config_dir = tempfile.mkdtemp(prefix="xray-")
        config_path = os.path.join(self._config_dir, "config.json")
        fd = os.open(config_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(build_xray_config([s.node for s in self._states], self._base_port), f)
        check = await asyncio.create_subprocess_exec(
            self._xray_bin, "run", "-test", "-c", config_path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        out, _ = await check.communicate()
        if check.returncode != 0:
            raise RuntimeError("xray rejected the generated config: " + out.decode(errors="replace")[-400:])
        self._proc = await asyncio.create_subprocess_exec(
            self._xray_bin, "run", "-c", config_path,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        for _ in range(50):  # wait until the first inbound accepts connections
            if self._proc.returncode is not None:
                raise RuntimeError("xray exited right after start")
            try:
                with socket.create_connection(("127.0.0.1", self._base_port), timeout=0.2):
                    return
            except OSError:
                await asyncio.sleep(0.1)
        raise RuntimeError("xray did not open its local ports")

    async def start(self) -> None:
        await self._spawn()
        if self._probe is not None:
            await self.benchmark()
            ok = len(self._alive())
            LOGGER.info("vpn: %d/%d nodes usable", ok, len(self._states))
            for s in sorted(self._alive(), key=lambda s: s.latency)[:5]:
                LOGGER.info("vpn: %s %.2fs", s.node.label, s.latency)
        self._task = asyncio.create_task(self._maintain())

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
        if self._proc is not None and self._proc.returncode is None:
            self._proc.terminate()
            try:
                await asyncio.wait_for(self._proc.wait(), 5.0)
            except asyncio.TimeoutError:
                self._proc.kill()
        if self._config_dir:
            shutil.rmtree(self._config_dir, ignore_errors=True)


def make_vpn_url_check_poster(vpn: VpnPool):
    """POST {"urls": [...]} to the url-check API through the fastest healthy VPN node."""
    from curl_cffi.requests import AsyncSession

    async def post(endpoint: str, urls: list[str], timeout: float):
        state = vpn.acquire()
        t0 = time.monotonic()
        try:
            async with AsyncSession(timeout=timeout) as session:
                response = await session.post(
                    endpoint, json={"urls": urls}, proxies={"http": state.proxy, "https": state.proxy}
                )
        except Exception:
            vpn.report_failure(state)
            raise
        if response.status_code != 200:
            vpn.report_failure(state)
            raise RuntimeError(f"url-check via {state.node.label} answered HTTP {response.status_code}")
        vpn.report_success(state, time.monotonic() - t0)
        return response.json()

    return post


def make_vpn_probe(endpoint: str, timeout: float = 8.0) -> Probe:
    """One real url-check call through `proxy_url`; must answer HTTP 200 with a JSON list."""
    from curl_cffi.requests import AsyncSession

    async def probe(proxy_url: str) -> None:
        async with AsyncSession(timeout=timeout) as session:
            response = await session.post(
                endpoint, json={"urls": PROBE_URLS}, proxies={"http": proxy_url, "https": proxy_url}
            )
        if response.status_code != 200 or not isinstance(response.json(), list):
            raise RuntimeError(f"probe answered HTTP {response.status_code}")

    return probe
