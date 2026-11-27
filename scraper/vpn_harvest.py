"""Free vless nodes as a proxy source: download lists, test with Xray, keep the nodes that open a real Threads page.

Every refresh starts one Xray process with a local SOCKS5 inbound per candidate node (ports base_port+i), probes each
node with a real Threads page and returns the working ones as "socks5h://127.0.0.1:<port>" proxies. The previous Xray
process is stopped by the caller once the new list is in use. Public lists carry no secrets of ours; the links stay in
memory (they are never logged).
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import random
import re
import shutil
import statistics
import tempfile
import time
from pathlib import Path

import httpx
from curl_cffi import requests as cffi

from scraper.constants import LOGGER
from scraper.singbox_sub import build_test_config, free_ports, parse_subscription, valid_nodes
from scraper.vpn import build_xray_config, parse_vless

THREADS_URL = "https://www.threads.com/@zuck/replies"
LOCAL_PREFIX = "socks5h://127.0.0.1:"


def _decode_list(text: str) -> list[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if any(l.startswith(("vless://", "vmess://", "trojan://", "ss://")) for l in lines):
        return lines
    try:  # base64 subscription
        body = text.strip()
        raw = base64.b64decode(body + "=" * (-len(body) % 4)).decode("utf-8", "ignore")
        return [l.strip() for l in raw.splitlines() if l.strip()]
    except Exception:  # noqa: BLE001
        return []


async def _download(urls: list[str]) -> list[str]:
    links: list[str] = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for url in urls:
            try:
                response = await client.get(url)
                response.raise_for_status()
                links += [l for l in _decode_list(response.text) if l.startswith("vless://")]
            except Exception as exc:  # noqa: BLE001 - one dead source must not stop the others
                LOGGER.warning("vpn source %s failed: %s", url[:70], str(exc)[:80])
    return list(dict.fromkeys(links))


async def _xray_accepts(xray: str, node, tmp: Path, base_port: int, sem: asyncio.Semaphore, index: int) -> bool:
    async with sem:
        try:
            path = tmp / f"t{index}.json"
            path.write_text(json.dumps(build_xray_config([node], base_port)), encoding="utf-8")
            proc = await asyncio.create_subprocess_exec(
                xray, "run", "-test", "-c", str(path), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
            )
            return await asyncio.wait_for(proc.wait(), timeout=15) == 0
        except Exception:  # noqa: BLE001 - unsupported node
            return False


async def _probe(port: int, timeout: float, max_latency: float, sem: asyncio.Semaphore) -> float | None:
    async with sem:
        proxy = f"{LOCAL_PREFIX}{port}"
        started = time.monotonic()
        try:
            async with cffi.AsyncSession(impersonate="chrome124", timeout=timeout) as session:
                response = await session.get(
                    THREADS_URL, proxies={"http": proxy, "https": proxy}, allow_redirects=True,
                    headers={"Accept-Language": "en-US,en;q=0.9"},
                )
            latency = time.monotonic() - started
            if (
                response.status_code == 200 and "/login" not in str(response.url)
                and b"data-sjs" in response.content and latency <= max_latency
            ):
                return latency
        except Exception:  # noqa: BLE001 - dead node
            pass
        return None


XRAY_LOG = os.path.join(tempfile.gettempdir(), "xray_vpn.log")


def extra_sources() -> list[str]:
    """Paid/own subscriptions from the env VPN_SOURCES_EXTRA (a secret: the URL carries a token, so it never
    goes to config.yaml). Whitespace or comma separated. Their nodes are always candidates and always kept."""
    return [u for u in re.split(r"[\s,]+", os.getenv("VPN_SOURCES_EXTRA", "")) if u.startswith("http")]


SINGBOX_LOG = os.path.join(tempfile.gettempdir(), "singbox_vpn.log")


async def _wait_listening(ports: list[int], proc: asyncio.subprocess.Process, seconds: float = 15.0) -> bool:
    """True once the last inbound accepts connections (sing-box opens them all at start)."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and proc.returncode is None:
        try:
            _, writer = await asyncio.open_connection("127.0.0.1", ports[-1])
            writer.close()
            return True
        except OSError:
            await asyncio.sleep(0.3)
    return False


async def _download_text(urls: list[str]) -> str:
    bodies: list[str] = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for url in urls:
            try:
                response = await client.get(url)
                response.raise_for_status()
                bodies.append(response.text)
            except Exception as exc:  # noqa: BLE001 - the URL carries a token: never log it
                LOGGER.warning("vpn subscription download failed: %s", type(exc).__name__)
    return "\n".join(bodies)


async def harvest_singbox(cfg: dict) -> tuple[list[str], asyncio.subprocess.Process | None]:
    """Own subscription (vless + hysteria2) through ONE sing-box with a loopback SOCKS inbound per node on OS-chosen
    ports. Every node is probed with a real Threads page; the fastest `vpn_keep` are returned."""
    singbox = os.getenv("SINGBOX_BIN") or shutil.which("sing-box")
    if not singbox:
        return [], None
    text = await _download_text(extra_sources())
    nodes = parse_subscription(text)
    if not nodes:
        LOGGER.warning("vpn subscription: no usable vless/hysteria2 nodes")
        return [], None
    random.shuffle(nodes)
    nodes = await valid_nodes(nodes[: int(cfg.get("vpn_candidates", 800))])
    if not nodes:
        LOGGER.warning("vpn subscription: sing-box accepted no node")
        return [], None
    ports = free_ports(len(nodes))
    fd, name = tempfile.mkstemp(prefix="sb_vpn_", suffix=".json")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(build_test_config(nodes, ports), handle)
    log_file = open(SINGBOX_LOG, "wb")  # noqa: SIM115 - handed to the child process
    proc = await asyncio.create_subprocess_exec(singbox, "run", "-c", name, stdout=log_file, stderr=log_file)
    log_file.close()
    try:
        if not await _wait_listening(ports, proc):
            LOGGER.warning("vpn: sing-box did not start (exit %s). log: %s", proc.returncode, _tail(SINGBOX_LOG))
            _stop(proc)
            return [], None
    finally:
        try:
            os.unlink(name)  # sing-box has read it (the file holds node credentials)
        except OSError:
            pass
    sem = asyncio.Semaphore(int(cfg.get("vpn_probe_concurrency", 30)))
    max_latency = float(cfg.get("vpn_max_latency_seconds", 6.0))
    latencies = await asyncio.gather(*(_probe(port, max_latency * 2.5, max_latency, sem) for port in ports))
    good = sorted((lat, port) for lat, port in zip(latencies, ports) if lat is not None)[: int(cfg.get("vpn_keep", 150))]
    if not good:
        LOGGER.warning("vpn: %d nodes started, none opened a Threads page", len(nodes))
        _stop(proc)
        return [], None
    kinds = {"vless": 0, "hysteria2": 0}
    by_port = dict(zip(ports, nodes))
    for _, port in good:
        kinds[by_port[port].kind] += 1
    LOGGER.info(
        "vpn harvest (sing-box): %d nodes tested, %d open Threads (<=%.0fs), median %.1fs, kept %d vless + %d hysteria2",
        len(nodes), sum(1 for lat in latencies if lat is not None), max_latency, statistics.median(l for l, _ in good),
        kinds["vless"], kinds["hysteria2"],
    )
    return [f"{LOCAL_PREFIX}{port}" for _, port in good], proc


def _tail(path: str, lines: int = 6) -> str:
    try:
        return " | ".join(Path(path).read_text(errors="ignore").splitlines()[-lines:])[:600]
    except OSError:
        return ""


async def harvest_nodes(cfg: dict, base_port: int) -> tuple[list[str], asyncio.subprocess.Process | None]:
    """(working local proxies, the Xray process that serves them). Empty list when nothing worked."""
    sources = [s for s in (cfg.get("vpn_sources") or []) if isinstance(s, str) and s.strip()]
    extra = extra_sources()
    if extra and not sources and (os.getenv("SINGBOX_BIN") or shutil.which("sing-box")):
        return await harvest_singbox(cfg)  # own subscription only: sing-box (vless + hysteria2)
    xray = os.getenv("XRAY_BIN") or shutil.which("xray")
    if not (sources or extra) or not xray:
        return [], None
    free_links = await _download(sources) if sources else []
    extra_links = await _download(extra) if extra else []
    paid_set = set(extra_links)
    random.shuffle(free_links)
    links = extra_links + [l for l in free_links if l not in paid_set]
    candidates = []
    paid_flags: list[bool] = []
    for link in extra_links + [l for l in free_links if l not in paid_set][: int(cfg.get("vpn_candidates", 800))]:
        try:
            candidates.append(parse_vless(link))
        except ValueError:
            continue
        paid_flags.append(link in paid_set)
    want = int(cfg.get("vpn_nodes", 250))
    with tempfile.TemporaryDirectory() as tmp:
        sem = asyncio.Semaphore(8)
        checked = await asyncio.gather(
            *(_xray_accepts(xray, n, Path(tmp), base_port, sem, i) for i, n in enumerate(candidates))
        )
        accepted = [(n, paid) for n, ok, paid in zip(candidates, checked, paid_flags) if ok]
        paid_nodes = [n for n, paid in accepted if paid]
        nodes = paid_nodes + [n for n, paid in accepted if not paid][: max(0, want - len(paid_nodes))]
        if not nodes:
            LOGGER.warning("vpn harvest: %d links, no node accepted by xray", len(links))
            return [], None
        path = Path(tmp) / "xray.json"
        path.write_text(json.dumps(build_xray_config(nodes, base_port)), encoding="utf-8")
        # Xray's own log goes to a file so a later death can be explained (see xray_tail).
        log_file = open(XRAY_LOG, "wb")  # noqa: SIM115 - handed to the child process
        proc = await asyncio.create_subprocess_exec(xray, "run", "-c", str(path), stdout=log_file, stderr=log_file)
        log_file.close()
        await asyncio.sleep(3)  # Xray has read the config by now; the temp dir may go
        if proc.returncode is not None:
            LOGGER.warning("vpn harvest: xray exited with %s", proc.returncode)
            return [], None
    verify_sem = asyncio.Semaphore(int(cfg.get("vpn_probe_concurrency", 30)))
    max_latency = float(cfg.get("vpn_max_latency_seconds", 6.0))
    latencies = await asyncio.gather(*(_probe(base_port + i, max_latency * 2.5, max_latency, verify_sem) for i in range(len(nodes))))
    # nodes[:len(paid_nodes)] are the paid ones: every paid node that opens Threads is kept, free ones fill up to vpn_keep.
    ok_all = [(lat, base_port + i, i < len(paid_nodes)) for i, lat in enumerate(latencies) if lat is not None]
    paid_good = sorted((lat, port) for lat, port, paid in ok_all if paid)
    free_good = sorted((lat, port) for lat, port, paid in ok_all if not paid)
    good = sorted(paid_good + free_good[: max(0, int(cfg.get("vpn_keep", 150)) - len(paid_good))])
    if not good:
        LOGGER.warning("vpn harvest: %d nodes started, none opened a Threads page", len(nodes))
        _stop(proc)
        return [], None
    LOGGER.info(
        "vpn harvest: %d links (%d paid), %d accepted by xray, %d open Threads (<=%.0fs) kept (%d paid), median %.1fs",
        len(links), len(extra_links), len(nodes), len(good), max_latency, len(paid_good), statistics.median(l for l, _ in good),
    )
    return [f"{LOCAL_PREFIX}{port}" for _, port in good], proc


def _stop(proc: asyncio.subprocess.Process | None) -> None:
    if proc is not None and proc.returncode is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass


stop_xray = _stop


def xray_tail(lines: int = 6) -> str:
    """Last lines of the Xray / sing-box log (why did it die?)."""
    return _tail(XRAY_LOG, lines) or _tail(SINGBOX_LOG, lines)
