"""Home bridge (runs at home in Docker with the host network).

  xray (one vless node as a LOCAL SOCKS5 proxy, only this container uses it)
    -> one `ssh -R` reverse tunnel per VPS, dialled through that SOCKS proxy (the VPS IPs are not reachable
       from Russia directly, the vless exit is)
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
    args = [
        "ssh", "-N", "-T", "-i", cfg["key"],
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", f"UserKnownHostsFile={cfg['known_hosts']}",
        "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes", "-o", "ConnectTimeout=30",
        "-o", f"ProxyCommand=nc -X 5 -x 127.0.0.1:{cfg['socks_port']} %h %p",
        "-R", f"{cfg['remote_bind']}:{cfg['local']}",
        "-p", port or "22",
        user_host,
    ]
    delay = 5.0
    while not stop.is_set():
        if not _port_open("127.0.0.1", cfg["socks_port"]):
            await asyncio.sleep(2.0)
            continue
        proc = await asyncio.create_subprocess_exec(*args, stdin=asyncio.subprocess.DEVNULL)
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


async def status_loop(targets: list[str], local: str, stop: asyncio.Event, state: dict) -> None:
    host, _, port = local.rpartition(":")
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=15.0)  # let xray and the tunnels come up first
    while not stop.is_set():
        up = sum(1 for t in targets if t in state and time.monotonic() - state[t][1] >= 10)
        collector = "up" if _port_open(host or "127.0.0.1", int(port)) else "DOWN"
        level = logging.INFO if up == len(targets) and collector == "up" else logging.WARNING
        LOG.log(level, "bridge: xray=%s tunnels up=%d/%d local collector %s=%s",
                "up" if "xray" in state else "down", up, len(targets), local, collector)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=60.0)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    targets = [t.strip() for t in _env("BRIDGE_TARGETS", "").split(",") if t.strip()]
    if not targets:
        raise SystemExit("BRIDGE_TARGETS is empty: set user@host[,user@host...] in home/.env")
    socks_port = int(_env("BRIDGE_SOCKS_PORT", "10808"))
    cfg = {
        "key": _env("BRIDGE_SSH_KEY", "/keys/id_ed25519"),
        "known_hosts": _env("BRIDGE_KNOWN_HOSTS", "/state/known_hosts"),
        "socks_port": socks_port,
        # what the VPS listens on (the docker bridge gateway: containers reach it as host.docker.internal)
        "remote_bind": _env("BRIDGE_REMOTE_BIND", "172.17.0.1:9000"),
        # where the data goes at home
        "local": _env("BRIDGE_LOCAL", "127.0.0.1:9000"),
    }
    nodes = load_nodes(Path(_env("BRIDGE_VLESS_FILE", "/data/vless.txt")))
    if not nodes:
        raise SystemExit("no usable vless:// link in the vless file")
    xray_config = Path("/tmp/xray.json")
    xray_config.write_text(json.dumps(build_xray_config(nodes[:1], socks_port)), encoding="utf-8")
    os.chmod(xray_config, 0o600)
    LOG.info("bridge starting: %d VPS target(s), remote bind %s -> local %s", len(targets), cfg["remote_bind"], cfg["local"])

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)

    state: dict = {}
    tasks = [asyncio.create_task(run_xray(xray_config, stop, state))]
    tasks += [asyncio.create_task(run_tunnel(t, cfg, stop, state)) for t in targets]
    tasks.append(asyncio.create_task(status_loop(targets, cfg["local"], stop, state)))
    await stop.wait()
    for value in list(state.values()):
        proc = value[0] if isinstance(value, tuple) else value
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
