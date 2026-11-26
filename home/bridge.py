"""Home bridge (runs at home in Docker with the host network).

  a local proxy: either the xray of this container (one vless node as a LOCAL SOCKS5 proxy, only this container uses
  it) or, with BRIDGE_UPSTREAM_PROXY set, a proxy that already runs on the home machine (e.g. a VPN client's
  "fastest node" proxy: socks5h://127.0.0.1:1080 or http://127.0.0.1:1081)
    -> one `ssh -R` reverse tunnel per VPS, dialled through that proxy (the VPS IPs are not reachable
       from Russia directly, the proxy exit is)
    -> every VPS listens on BRIDGE_REMOTE_BIND (the docker bridge gateway) and the parsers there POST to it;
       the data comes out of the tunnel at BRIDGE_LOCAL = your collector (127.0.0.1:9000 at home).

The system's own traffic is untouched (no system-wide VPN). The vless link (it holds a user id) is read from a
file and never logged. Configuration comes from the environment, see home/.env.example.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import socket
import time
from pathlib import Path

from scraper.vpn import build_xray_config, load_nodes

LOG = logging.getLogger("bridge")


def _env(name: str, default: str) -> str:
    return (os.getenv(name) or "").strip() or default


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def parse_upstream(value: str) -> tuple[str, int, str]:
    """BRIDGE_UPSTREAM_PROXY (socks5://, socks5h://, http://host:port) -> (host, port, nc proxy protocol: "5" or "connect")."""
    scheme, sep, rest = value.strip().partition("://")
    if not sep:
        raise SystemExit(f"BRIDGE_UPSTREAM_PROXY must look like socks5h://127.0.0.1:1080 or http://127.0.0.1:1081, got {value!r}")
    scheme = scheme.lower()
    if scheme in ("socks5", "socks5h"):
        proto = "5"
    elif scheme in ("http", "https"):
        proto = "connect"
    else:
        raise SystemExit(f"BRIDGE_UPSTREAM_PROXY: unsupported scheme {scheme!r} (use socks5h:// or http://)")
    host, _, port = rest.rstrip("/").rpartition(":")
    if not host or not port.isdigit():
        raise SystemExit(f"BRIDGE_UPSTREAM_PROXY needs host:port, got {value!r}")
    return host, int(port), proto


async def _wait_or_stop(proc: asyncio.subprocess.Process, stop: asyncio.Event) -> int | None:
    """Wait for the process to exit; None if the bridge is stopping first."""
    while not stop.is_set():
        with contextlib.suppress(asyncio.TimeoutError):
            return await asyncio.wait_for(proc.wait(), timeout=2.0)
    return None


async def run_xray(config_path: Path, stop: asyncio.Event, state: dict) -> None:
    delay = 3.0
    while not stop.is_set():
        proc = await asyncio.create_subprocess_exec("xray", "run", "-c", str(config_path))
        state["xray"] = proc
        started = time.monotonic()
        code = await _wait_or_stop(proc, stop)
        if code is None:
            return
        state.pop("xray", None)
        if stop.is_set():
            return
        delay = 3.0 if time.monotonic() - started > 60 else min(delay * 2, 60.0)
        LOG.warning("xray exited with code %s, restarting in %.0fs", code, delay)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)


async def run_tunnel(target: str, cfg: dict, stop: asyncio.Event, state: dict) -> None:
    """Keep `ssh -N -R` to one VPS alive (target: user@host or user@host:port)."""
    user_host, _, port = target.partition(":")

    def build_args() -> list[str]:
        # Built for every start: with a subscription the proxy port changes when a better node is chosen.
        return [
            "ssh", "-N", "-T", "-i", cfg["key"],
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", f"UserKnownHostsFile={cfg['known_hosts']}",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
            "-o", "ExitOnForwardFailure=yes", "-o", "ConnectTimeout=30",
            "-o", f"ProxyCommand=nc -X {cfg['proxy_proto']} -x {cfg['proxy_host']}:{cfg['socks_port']} %h %p",
            "-R", f"{cfg['remote_bind']}:{cfg['local']}",
            "-p", port or "22",
            user_host,
        ]

    delay = 5.0
    while not stop.is_set():
        if not cfg["socks_port"] or not _port_open(cfg["proxy_host"], cfg["socks_port"]):
            await asyncio.sleep(2.0)
            continue
        proc = await asyncio.create_subprocess_exec(*build_args(), stdin=asyncio.subprocess.DEVNULL)
        state[target] = (proc, time.monotonic())
        started = time.monotonic()
        code = await _wait_or_stop(proc, stop)
        if code is None:
            return
        state.pop(target, None)
        if stop.is_set():
            return
        lived = time.monotonic() - started
        delay = 5.0 if lived > 60 else min(delay * 2, 60.0)
        LOG.warning("tunnel %s: ssh exited with code %s after %.0fs, retry in %.0fs", target, code, lived, delay)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)


async def status_loop(targets: list[str], local: str, stop: asyncio.Event, state: dict, proxy_label: str) -> None:
    host, _, port = local.rpartition(":")
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=15.0)  # let xray and the tunnels come up first
    while not stop.is_set():
        up = sum(1 for t in targets if t in state and time.monotonic() - state[t][1] >= 10)
        collector = "up" if _port_open(host or "127.0.0.1", int(port)) else "DOWN"
        level = logging.INFO if up == len(targets) and collector == "up" else logging.WARNING
        LOG.log(level, "bridge: %s tunnels up=%d/%d local collector %s=%s", proxy_label(state), up, len(targets), local, collector)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=60.0)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    targets = [t.strip() for t in _env("BRIDGE_TARGETS", "").split(",") if t.strip()]
    if not targets:
        raise SystemExit("BRIDGE_TARGETS is empty: set user@host[,user@host...] in home/.env")
    upstream = _env("BRIDGE_UPSTREAM_PROXY", "")
    subscription = _env("BRIDGE_SUBSCRIPTION_URL", "")
    if subscription:
        # Own sing-box from a subscription: the port is chosen by the OS when the fastest node is started.
        proxy_host, socks_port, proxy_proto = "127.0.0.1", 0, "5"
    elif upstream:
        # A proxy that already runs on the home machine: no vless link and no xray in this container.
        proxy_host, socks_port, proxy_proto = parse_upstream(upstream)
    else:
        proxy_host, socks_port, proxy_proto = "127.0.0.1", int(_env("BRIDGE_SOCKS_PORT", "10808")), "5"
    cfg = {
        "key": _env("BRIDGE_SSH_KEY", "/keys/id_ed25519"),
        "known_hosts": _env("BRIDGE_KNOWN_HOSTS", "/state/known_hosts"),
        "socks_port": socks_port,
        "proxy_host": proxy_host,
        "proxy_proto": proxy_proto,
        # what the VPS listens on (the docker bridge gateway: containers reach it as host.docker.internal)
        "remote_bind": _env("BRIDGE_REMOTE_BIND", "172.17.0.1:9000"),
        # where the data goes at home
        "local": _env("BRIDGE_LOCAL", "127.0.0.1:9000"),
    }
    xray_config = Path("/tmp/xray.json")
    if not upstream and not subscription:
        nodes = load_nodes(Path(_env("BRIDGE_VLESS_FILE", "/data/vless.txt")))
        if not nodes:
            raise SystemExit("no usable vless:// link in the vless file (or set BRIDGE_UPSTREAM_PROXY to use a proxy that already runs)")
        xray_config.write_text(json.dumps(build_xray_config(nodes[:1], socks_port)), encoding="utf-8")
        os.chmod(xray_config, 0o600)
    LOG.info(
        "bridge starting: %d VPS target(s), remote bind %s -> local %s, proxy %s",
        len(targets), cfg["remote_bind"], cfg["local"],
        "own sing-box from the subscription" if subscription else upstream or f"own xray on 127.0.0.1:{socks_port}",
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    state: dict = {}
    tasks = [] if (upstream or subscription) else [asyncio.create_task(run_xray(xray_config, stop, state))]
    if subscription:
        from proxy_manager import SubscriptionProxy  # needs sing-box, imported only in this mode

        manager = SubscriptionProxy(
            subscription, targets, Path(_env("BRIDGE_STATE_DIR", "/state")),
            float(_env("BRIDGE_RESELECT_MINUTES", "30")), cfg, state,
        )
        tasks.append(asyncio.create_task(manager.run(stop)))
    tasks += [asyncio.create_task(run_tunnel(t, cfg, stop, state)) for t in targets]

    def proxy_label(current: dict) -> str:
        if subscription:
            return f"proxy {cfg.get('node_label') or 'choosing the node'}"
        if upstream:
            host_port = f"{cfg['proxy_host']}:{cfg['socks_port']}"
            return f"proxy {host_port}={'up' if _port_open(cfg['proxy_host'], cfg['socks_port']) else 'DOWN'}"
        return f"xray={'up' if 'xray' in current else 'down'}"

    tasks.append(asyncio.create_task(status_loop(targets, cfg["local"], stop, state, proxy_label)))
    await stop.wait()
    for value in list(state.values()):
        proc = value[0] if isinstance(value, tuple) else value
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
