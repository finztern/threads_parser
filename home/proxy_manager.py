"""The home bridge's own proxy from a subscription: sing-box picks nothing by itself, we measure and choose.

Flow:
  1. download the subscription (cached in /state/nodes.txt in case the site is down), parse vless/hysteria2 nodes
  2. validate them with `sing-box check` (a bad node is dropped, not the whole batch)
  3. coarse pass: every node in batches, one loopback SOCKS inbound each, a connection through the node to the SSH
     port of the VPS (the real path of the tunnels; all targets at once): time to the SSH banner + one round trip
  4. fine pass on the best few (several attempts, all VPS targets), the best median wins
  5. run ONE sing-box with the winner on a random free loopback port (nothing fixed, nothing reachable from outside)
  6. watch it: a dead path or a much better node found by the periodic re-check -> switch (tunnels are restarted)
The subscription link and node credentials are never logged.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import statistics
import time
import urllib.request
from pathlib import Path

from scraper.singbox_sub import Node, build_run_config, build_test_config, free_ports, parse_subscription, valid_nodes

LOG = logging.getLogger("proxy")

BATCH = 100  # nodes per test sing-box (each gets its own loopback port)
PROBE_TIMEOUT = 8.0
FINE_TOP = 12
UA = "sing-box"  # providers often choose the answer format by the client name


def parse_target(target: str) -> tuple[str, int]:
    """'user@host[:port]' -> (host, ssh port)."""
    _, _, host_port = target.rpartition("@")
    host, _, port = host_port.partition(":")
    return host, int(port) if port.isdigit() else 22


async def probe(socks_port: int, host: str, port: int, timeout: float = PROBE_TIMEOUT) -> tuple[float, float] | None:
    """Connect through a SOCKS5 proxy to an SSH server: (seconds until its banner, seconds of one round trip) or None."""
    writer = None
    try:
        started = time.monotonic()
        reader, writer = await asyncio.wait_for(asyncio.open_connection("127.0.0.1", socks_port), 3.0)
        writer.write(b"\x05\x01\x00")
        await writer.drain()
        if await asyncio.wait_for(reader.readexactly(2), timeout) != b"\x05\x00":
            return None
        encoded = host.encode()
        writer.write(b"\x05\x01\x00\x03" + bytes([len(encoded)]) + encoded + port.to_bytes(2, "big"))
        await writer.drain()
        reply = await asyncio.wait_for(reader.readexactly(4), timeout)
        if reply[1] != 0:
            return None
        tail = {1: 6, 4: 18}.get(reply[3])
        if tail is None:
            tail = (await reader.readexactly(1))[0] + 2
        await reader.readexactly(tail)
        banner = await asyncio.wait_for(reader.readline(), timeout)
        if not banner.startswith(b"SSH-"):
            return None
        to_banner = time.monotonic() - started
        sent = time.monotonic()
        writer.write(b"SSH-2.0-probe\r\n")
        await writer.drain()
        try:
            await asyncio.wait_for(reader.readexactly(4), timeout)
            round_trip = time.monotonic() - sent
        except (asyncio.TimeoutError, asyncio.IncompleteReadError):
            round_trip = to_banner
        return to_banner, round_trip
    except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError):
        return None
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()


async def _run_singbox(config: dict, tag: str) -> tuple[asyncio.subprocess.Process, Path]:
    path = Path(f"/tmp/sb_{tag}.json")
    path.write_text(json.dumps(config), encoding="utf-8")
    os.chmod(path, 0o600)
    log_file = open(f"/tmp/sb_{tag}.log", "wb")  # noqa: SIM115 - closed when the process is gone (see _stop)
    proc = await asyncio.create_subprocess_exec("sing-box", "run", "-c", str(path), stdout=log_file, stderr=log_file)
    proc._log_file = log_file  # type: ignore[attr-defined]
    return proc, path


async def _stop(proc: asyncio.subprocess.Process | None, path: Path | None = None) -> None:
    if proc is not None:
        with contextlib.suppress(ProcessLookupError):
            proc.terminate()
        with contextlib.suppress(asyncio.TimeoutError, ProcessLookupError):
            await asyncio.wait_for(proc.wait(), timeout=5.0)
        with contextlib.suppress(ProcessLookupError):
            proc.kill()
        log_file = getattr(proc, "_log_file", None)
        if log_file is not None:
            log_file.close()
    if path is not None:
        with contextlib.suppress(OSError):
            path.unlink()


async def _wait_ports(ports: list[int], proc: asyncio.subprocess.Process, seconds: float = 10.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if proc.returncode is not None:
            return False
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", ports[-1])
        except OSError:
            await asyncio.sleep(0.3)
            continue
        writer.close()
        return True
    return False


async def measure(nodes: list[Node], targets: list[tuple[str, int]], attempts: int, tag: str) -> dict[str, float]:
    """node.key -> score (seconds, lower is better; the worst reachable VPS target counts) for nodes that reached some target."""
    scores: dict[str, float] = {}
    for start in range(0, len(nodes), BATCH):
        batch = nodes[start : start + BATCH]
        ports = free_ports(len(batch))
        proc, path = await _run_singbox(build_test_config(batch, ports), tag)
        try:
            if not await _wait_ports(ports, proc):
                LOG.warning("test sing-box did not start (batch %d..%d), see /tmp/sb_%s.log", start, start + len(batch), tag)
                continue

            async def one_target(port: int, host: str, target_port: int) -> float | None:
                results = [await probe(port, host, target_port) for _ in range(attempts)]
                good = [r for r in results if r]
                if len(good) * 2 <= attempts:  # fewer than half of the attempts worked
                    return None
                return statistics.median(rt + 0.25 * banner for banner, rt in good)

            async def one(node: Node, port: int) -> None:
                results = await asyncio.gather(*(one_target(port, h, p) for h, p in targets))
                answered = [r for r in results if r is not None]
                if answered:
                    # the worst answered target counts; a target nobody reaches (a VPS that is gone) costs every node the same
                    scores[node.key] = max(answered) + 5.0 * (len(targets) - len(answered))

            await asyncio.gather(*(one(n, p) for n, p in zip(batch, ports)))
        finally:
            await _stop(proc, path)
    return scores


def _fetch(url: str) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 - the user's own subscription link
        return response.read().decode("utf-8", "replace")


class SubscriptionProxy:
    def __init__(self, url: str, targets: list[str], state_dir: Path, reselect_minutes: float, cfg: dict, state: dict) -> None:
        self.url = url
        self.targets = [parse_target(t) for t in targets]
        self.cache = state_dir / "nodes.txt"
        self.reselect = max(5.0, reselect_minutes) * 60
        self.cfg = cfg  # the bridge's shared config: socks_port is updated when the node changes
        self.state = state
        self.node: Node | None = None
        self.score = 0.0
        self.bad: dict[str, float] = {}  # node.key -> monotonic time until which it is not chosen

    async def load_nodes(self) -> list[Node]:
        try:
            text = await asyncio.to_thread(_fetch, self.url)
            nodes = parse_subscription(text)
            if nodes:
                self.cache.parent.mkdir(parents=True, exist_ok=True)
                self.cache.write_text(text, encoding="utf-8")
                os.chmod(self.cache, 0o600)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("subscription download failed (%s), using the cached copy", type(exc).__name__)
            nodes = parse_subscription(self.cache.read_text(encoding="utf-8")) if self.cache.is_file() else []
        now = time.monotonic()
        return [n for n in nodes if self.bad.get(n.key, 0.0) <= now]

    async def choose(self) -> tuple[Node, float] | None:
        nodes = await self.load_nodes()
        if not nodes:
            LOG.error("no usable vless/hysteria2 nodes in the subscription")
            return None
        started = time.monotonic()
        total = len(nodes)
        nodes = await valid_nodes(nodes)
        coarse = await measure(nodes, self.targets, 1, "coarse")
        ranked = sorted((n for n in nodes if n.key in coarse), key=lambda n: coarse[n.key])
        LOG.info(
            "subscription: %d nodes, %d accepted by sing-box, %d reach the VPS (%.0fs)",
            total, len(nodes), len(ranked), time.monotonic() - started,
        )
        if not ranked:
            return None
        finalists = ranked[:FINE_TOP]
        fine = await measure(finalists, self.targets, 4, "fine")
        best = sorted((n for n in finalists if n.key in fine), key=lambda n: fine[n.key])
        if not best:
            return ranked[0], coarse[ranked[0].key]
        return best[0], fine[best[0].key]

    async def _start(self, node: Node) -> tuple[asyncio.subprocess.Process, Path, int] | None:
        port = free_ports(1)[0]
        proc, path = await _run_singbox(build_run_config(node, port), "run")
        if not await _wait_ports([port], proc):
            LOG.warning("sing-box did not start with the chosen node, see /tmp/sb_run.log")
            await _stop(proc, path)
            return None
        return proc, path, port

    async def _switch_tunnels(self) -> None:
        for key, value in list(self.state.items()):
            if key == "singbox":
                continue
            proc = value[0] if isinstance(value, tuple) else value
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()  # run_tunnel starts it again with the new proxy port

    async def run(self, stop: asyncio.Event) -> None:
        proc: asyncio.subprocess.Process | None = None
        path: Path | None = None
        next_check = 0.0
        failures = 0
        while not stop.is_set():
            now = time.monotonic()
            need_choice = proc is None or proc.returncode is not None or failures >= 3 or now >= next_check
            if need_choice:
                if proc is not None and (proc.returncode is not None or failures >= 3) and self.node is not None:
                    self.bad[self.node.key] = now + 1800.0
                    LOG.warning("node %s is not working, choosing another", self.node.label())
                picked = await self.choose()
                next_check = time.monotonic() + (self.reselect if picked else 60.0)
                failures = 0
                if picked is not None:
                    node, score = picked
                    keep = (
                        proc is not None and proc.returncode is None and self.node is not None
                        and (node.key == self.node.key or score > self.score * 0.7)
                    )
                    if not keep:
                        started = await self._start(node)
                        if started is not None:
                            await _stop(proc, path)
                            proc, path, port = started
                            self.node, self.score = node, score
                            self.cfg["socks_port"] = port
                            self.state["singbox"] = proc
                            self.cfg["node_label"] = f"{node.label()} ~{score * 1000:.0f} ms"
                            LOG.info("proxy: %s, round trip to the VPS ~%.0f ms (loopback port chosen by the OS)", node.label(), score * 1000)
                            await self._switch_tunnels()
                elif proc is None:
                    await asyncio.sleep(10.0)
                    continue
            # health check of the working proxy
            if proc is not None and proc.returncode is None and self.targets:
                host, target_port = self.targets[0]
                result = await probe(self.cfg["socks_port"], host, target_port)
                failures = 0 if result else failures + 1
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=60.0)
        await _stop(proc, path)
