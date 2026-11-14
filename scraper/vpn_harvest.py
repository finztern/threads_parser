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
import shutil
import statistics
import tempfile
import time
from pathlib import Path

import httpx
from curl_cffi import requests as cffi

from scraper.constants import LOGGER
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


async def harvest_nodes(cfg: dict, base_port: int) -> tuple[list[str], asyncio.subprocess.Process | None]:
    """(working local proxies, the Xray process that serves them). Empty list when nothing worked."""
    sources = [s for s in (cfg.get("vpn_sources") or []) if isinstance(s, str) and s.strip()]
    xray = os.getenv("XRAY_BIN") or shutil.which("xray")
    if not sources or not xray:
        return [], None
    links = await _download(sources)
    random.shuffle(links)
    candidates = []
    for link in links[: int(cfg.get("vpn_candidates", 800))]:
        try:
            candidates.append(parse_vless(link))
        except ValueError:
            continue
    want = int(cfg.get("vpn_nodes", 250))
    with tempfile.TemporaryDirectory() as tmp:
        sem = asyncio.Semaphore(8)
        checked = await asyncio.gather(
            *(_xray_accepts(xray, n, Path(tmp), base_port, sem, i) for i, n in enumerate(candidates))
        )
        nodes = [n for n, ok in zip(candidates, checked) if ok][:want]
        if not nodes:
            LOGGER.warning("vpn harvest: %d links, no node accepted by xray", len(links))
            return [], None
        path = Path(tmp) / "xray.json"
        path.write_text(json.dumps(build_xray_config(nodes, base_port)), encoding="utf-8")
        proc = await asyncio.create_subprocess_exec(
            xray, "run", "-c", str(path), stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        await asyncio.sleep(3)  # Xray has read the config by now; the temp dir may go
        if proc.returncode is not None:
            LOGGER.warning("vpn harvest: xray exited with %s", proc.returncode)
            return [], None
    verify_sem = asyncio.Semaphore(int(cfg.get("vpn_probe_concurrency", 30)))
    max_latency = float(cfg.get("vpn_max_latency_seconds", 6.0))
    latencies = await asyncio.gather(*(_probe(base_port + i, max_latency * 2.5, max_latency, verify_sem) for i in range(len(nodes))))
    good = sorted(((lat, base_port + i) for i, lat in enumerate(latencies) if lat is not None))
    good = good[: int(cfg.get("vpn_keep", 150))]
    if not good:
        LOGGER.warning("vpn harvest: %d nodes started, none opened a Threads page", len(nodes))
        _stop(proc)
        return [], None
    LOGGER.info(
        "vpn harvest: %d links, %d accepted by xray, %d open Threads (<=%.0fs) kept, median %.1fs",
        len(links), len(nodes), len(good), max_latency, statistics.median(l for l, _ in good),
    )
    return [f"{LOCAL_PREFIX}{port}" for _, port in good], proc


def _stop(proc: asyncio.subprocess.Process | None) -> None:
    if proc is not None and proc.returncode is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass


stop_xray = _stop
