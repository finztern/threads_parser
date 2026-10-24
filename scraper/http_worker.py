"""HTTP mode runner: no browser, no accounts. Every cycle fetches all
targets through the rotating proxy pool with bounded concurrency and hands
the posts/replies to the shared dispatch (max_age filter, SeenCache dedup,
TokenBucket, batch POST to store_items).
"""
from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from pathlib import Path

import httpx

from scraper.config import ConfigStore
from scraper.constants import BASE_DIR, LOGGER
from scraper.http_client import ReplyTracker, ThreadsHttpClient
from scraper.proxies import ProxyPool
from scraper.state import SeenCache, TokenBucket
from scraper.worker import dispatch_posts


def _resolve(path_str: str) -> Path:
    path = Path(path_str)
    return path if path.is_absolute() else BASE_DIR / path


def load_targets(cfg: dict) -> list[str]:
    """Usernames from config.yaml `targets` (type: profile) plus the optional
    `targets_file` (one username per line, '#' comments). Re-read every cycle.
    """
    names: list[str] = []
    for target in cfg.get("targets", []) or []:
        if target.get("type") == "profile" and target.get("value"):
            names.append(str(target["value"]).lstrip("@"))

    targets_file = cfg.get("targets_file")
    if targets_file:
        path = _resolve(targets_file)
        if path.is_file():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    names.append(line.lstrip("@"))

    return list(dict.fromkeys(names))  # dedupe, keep order


async def http_runner(
    config_store: ConfigStore,
    token_bucket: TokenBucket,
    seen_cache: SeenCache,
    stop_event: asyncio.Event,
) -> None:
    cfg = config_store.get()
    try:
        pool = ProxyPool.from_file(
            _resolve(cfg.get("proxies_file", "proxies.txt")),
            cooldown_seconds=cfg.get("proxy_cooldown_seconds", 120),
        )
    except (FileNotFoundError, ValueError) as exc:
        LOGGER.error("cannot start http mode: %s", exc)
        return

    LOGGER.info("http mode: %d proxies in pool", len(pool))
    client = ThreadsHttpClient(
        pool,
        timeout=cfg.get("http_timeout_seconds", 25),
        attempts=cfg.get("http_attempts", 3),
    )
    tracker = ReplyTracker()

    async with httpx.AsyncClient() as store_client:
        while not stop_event.is_set():
            cycle_start = time.monotonic()
            cfg = config_store.get()
            targets = load_targets(cfg)
            if not targets:
                LOGGER.error("no targets (config.yaml `targets` / targets_file) - waiting")
                await _sleep(stop_event, 30)
                continue

            sem = asyncio.Semaphore(cfg.get("http_concurrency", 30))
            fetch_replies = cfg.get("fetch_replies", True)
            reply_max_age = cfg.get("reply_post_max_age_seconds", 7 * 86400)
            totals: Counter[str] = Counter()
            requests_before = client.stats["requests"]
            stats_before = Counter(client.stats)

            async def job(username: str) -> None:
                async with sem:
                    if stop_event.is_set():
                        return
                    try:
                        label, posts, reply_pages = await client.fetch_target(
                            username, tracker, reply_max_age, fetch_replies
                        )
                        totals[f"profile_{label}"] += 1
                        totals["reply_pages"] += reply_pages
                        if posts:
                            totals.update(await dispatch_posts(posts, cfg, store_client, token_bucket, seen_cache))
                    except Exception:
                        totals["profile_error"] += 1
                        LOGGER.exception("target @%s failed", username)

            await asyncio.gather(*(job(u) for u in targets))

            delta = {k: client.stats[k] - stats_before[k] for k in client.stats}
            LOGGER.info(
                "http cycle: targets=%d ok=%d empty=%d not_found=%d failed=%d | requests=%d "
                "blocked=%d transport_err=%d proxies_ok=%d/%d reply_pages=%d | fetched=%d fresh=%d "
                "to_send=%d sent=%d duplicates=%d unconfirmed=%d rate_limited=%d | %.0fs",
                len(targets),
                totals["profile_ok"],
                totals["profile_empty"],
                totals["profile_not_found"],
                totals["profile_failed"] + totals["profile_error"],
                client.stats["requests"] - requests_before,
                delta.get("blocked", 0),
                delta.get("transport_errors", 0),
                pool.available(),
                len(pool),
                totals["reply_pages"],
                totals["fetched"],
                totals["fresh"],
                totals["to_send"],
                totals["sent"],
                totals["duplicates"],
                totals["unconfirmed"],
                totals["rate_limited"],
                time.monotonic() - cycle_start,
            )

            base = cfg.get("poll_interval_seconds", 60)
            jitter = base * cfg.get("poll_jitter_ratio", 0.25) * (2 * random.random() - 1)
            await _sleep(stop_event, max(0.0, base + jitter - (time.monotonic() - cycle_start)))


async def _sleep(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
