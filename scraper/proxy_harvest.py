"""Free proxy harvester: download public lists, probe them cheaply, keep the fast ones.

Sources come from config.yaml `proxy_sources` as "scheme|url" lines (the lists hold bare host:port). Each
candidate is probed with one small request (`proxy_probe_url`); only proxies that answer 200 within
`proxy_max_latency_seconds` are kept, fastest first. Real quality is then enforced live by the pool
(a proxy that times out or gets 429 is cooled down). Nothing secret is involved: the lists are public.
"""
from __future__ import annotations

import asyncio
import random
import re
import statistics
import time

import httpx
from curl_cffi import requests as cffi

from scraper.constants import LOGGER

_HOST_PORT = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}$")


async def _download(client: httpx.AsyncClient, source: str) -> list[str]:
    scheme, _, url = source.partition("|")
    if not url:
        scheme, url = "http", source
    try:
        response = await client.get(url.strip())
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001 - one dead source must not stop the others
        LOGGER.warning("proxy source %s failed: %s", url.strip()[:80], str(exc)[:100])
        return []
    out: list[str] = []
    for line in response.text.splitlines():
        line = line.strip()
        if "://" in line:
            out.append(line)
        elif _HOST_PORT.match(line):
            out.append(f"{scheme.strip()}://{line}")
    return out


async def _probe(
    proxy: str, url: str, timeout: float, max_latency: float, sem: asyncio.Semaphore, need_page: bool = False
) -> tuple[str, float] | None:
    """need_page: the answer must be a real Threads page (HTTP 200, data-sjs, no login redirect)."""
    async with sem:
        started = time.monotonic()
        try:
            async with cffi.AsyncSession(impersonate="chrome124", timeout=timeout) as session:
                response = await session.get(
                    url, proxies={"http": proxy, "https": proxy}, allow_redirects=need_page,
                    headers={"Accept-Language": "en-US,en;q=0.9"},
                )
            latency = time.monotonic() - started
            if response.status_code == 200 and latency <= max_latency:
                if need_page and ("/login" in str(response.url) or b"data-sjs" not in response.content):
                    return None
                return proxy, latency
        except Exception:  # noqa: BLE001 - dead proxy
            pass
        return None


async def harvest(cfg: dict) -> list[str]:
    sources = [s for s in (cfg.get("proxy_sources") or []) if isinstance(s, str) and s.strip()]
    if not sources:
        return []
    probe_url = cfg.get("proxy_probe_url", "https://www.threads.com/robots.txt")
    max_latency = float(cfg.get("proxy_max_latency_seconds", 4.0))
    max_candidates = int(cfg.get("proxy_max_candidates", 3000))
    keep = int(cfg.get("proxy_keep", 400))
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        lists = await asyncio.gather(*(_download(client, s) for s in sources))
    candidates = list(dict.fromkeys(p for lst in lists for p in lst))
    random.shuffle(candidates)
    total = len(candidates)
    candidates = candidates[:max_candidates]
    sem = asyncio.Semaphore(int(cfg.get("proxy_probe_concurrency", 150)))
    results = await asyncio.gather(*(_probe(p, probe_url, max_latency * 2, max_latency, sem) for p in candidates))
    alive = sorted((r for r in results if r), key=lambda r: r[1])
    stage1 = len(alive)
    verify_url = cfg.get("proxy_verify_url", "https://www.threads.com/@zuck/replies")
    if verify_url and alive:
        # Stage 2: a real Threads page through every fast proxy (the cheap probe cannot see 429 / login walls).
        limit = int(cfg.get("proxy_verify_candidates", 600))
        verify_latency = float(cfg.get("proxy_verify_max_latency_seconds", 8.0))
        checked = await asyncio.gather(
            *(_probe(p, verify_url, verify_latency * 2, verify_latency, sem, need_page=True) for p, _ in alive[:limit])
        )
        verified = [r for r in checked if r]
        LOGGER.info("proxy harvest: stage 2 (real page): %d of %d fast proxies pass", len(verified), min(limit, stage1))
        alive = sorted(verified, key=lambda r: r[1])
    alive = alive[:keep]
    if alive:
        LOGGER.info(
            "proxy harvest: %d listed, %d probed, %d fast (<=%.1fs) kept, median %.1fs",
            total, len(candidates), len(alive), max_latency, statistics.median(l for _, l in alive),
        )
    else:
        LOGGER.warning("proxy harvest: %d listed, %d probed, none fast enough", total, len(candidates))
    return [p for p, _ in alive]
