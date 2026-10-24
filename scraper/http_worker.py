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
from datetime import datetime, timezone
from pathlib import Path

import httpx

from scraper.config import ConfigStore
from scraper.constants import BASE_DIR, LOGGER
from scraper.discovery import TargetRegistry
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

    disc = cfg.get("discovery", {}) or {}
    discovery_on = bool(disc.get("enabled", True))
    registry = TargetRegistry(
        _resolve(disc.get("state_file", "data/targets_state.json")) if discovery_on else None,
        min_interval=disc.get("min_interval_seconds", 300),
        max_interval=disc.get("max_interval_seconds", 21600),
        seed_interval=cfg.get("poll_interval_seconds", 30),
        max_known=disc.get("max_known_targets", 200_000),
    )
    if discovery_on:
        registry.load()

    async with httpx.AsyncClient() as store_client:
        try:
            while not stop_event.is_set():
                cycle_start = time.monotonic()
                cfg = config_store.get()
                disc = cfg.get("discovery", {}) or {}
                max_per_cycle = int(disc.get("max_targets_per_cycle", 400))
                max_age = cfg.get("max_age_seconds", 86400)

                seeds = load_targets(cfg)
                if not seeds and not registry.stats()["known"]:
                    LOGGER.error("no targets (config.yaml `targets` / targets_file) - waiting")
                    await _sleep(stop_event, 30)
                    continue
                registry.sync_seeds(seeds)

                batch = registry.due(max_per_cycle)
                if not batch:
                    await _sleep(stop_event, min(cfg.get("poll_interval_seconds", 30), registry.seconds_until_next_due() + 0.5))
                    continue

                sem = asyncio.Semaphore(cfg.get("http_concurrency", 30))
                fetch_replies = cfg.get("fetch_replies", True)
                reply_max_age = cfg.get("reply_post_max_age_seconds", 7 * 86400)
                totals: Counter[str] = Counter()
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

                            now = datetime.now(timezone.utc)
                            own_fresh = sum(
                                1
                                for p in posts
                                if p.username.lower() == username.lower()
                                and (now - p.created_at.astimezone(timezone.utc)).total_seconds() <= max_age
                            )
                            registry.report(username, label, own_fresh)
                            if discovery_on and posts:
                                totals["discovered"] += registry.observe({p.username for p in posts})
                            if posts:
                                totals.update(await dispatch_posts(posts, cfg, store_client, token_bucket, seen_cache))
                        except Exception:
                            totals["profile_error"] += 1
                            registry.report(username, "failed", 0)
                            LOGGER.exception("target @%s failed", username)

                await asyncio.gather(*(job(u) for u in batch))
                if discovery_on:
                    registry.save()

                delta = {k: client.stats[k] - stats_before[k] for k in client.stats}
                rs = registry.stats()
                LOGGER.info(
                    "http cycle: scraped=%d ok=%d empty=%d not_found=%d failed=%d | requests=%d "
                    "blocked=%d transport_err=%d proxies_ok=%d/%d reply_pages=%d | known=%d (+%d new) "
                    "active=%d dead=%d due_now=%d | fetched=%d fresh=%d to_send=%d sent=%d "
                    "duplicates=%d unconfirmed=%d rate_limited=%d | %.0fs",
                    len(batch),
                    totals["profile_ok"],
                    totals["profile_empty"],
                    totals["profile_not_found"],
                    totals["profile_failed"] + totals["profile_error"],
                    delta.get("requests", 0),
                    delta.get("blocked", 0),
                    delta.get("transport_errors", 0),
                    pool.available(),
                    len(pool),
                    totals["reply_pages"],
                    rs["known"],
                    totals["discovered"],
                    rs["active"],
                    rs["dead"],
                    rs["due"],
                    totals["fetched"],
                    totals["fresh"],
                    totals["to_send"],
                    totals["sent"],
                    totals["duplicates"],
                    totals["unconfirmed"],
                    totals["rate_limited"],
                    time.monotonic() - cycle_start,
                )

                # More targets already due -> go on right away; otherwise wait a bit.
                if registry.count_due() == 0:
                    base = cfg.get("poll_interval_seconds", 30)
                    jitter = base * cfg.get("poll_jitter_ratio", 0.25) * (2 * random.random() - 1)
                    await _sleep(stop_event, max(0.0, base + jitter - (time.monotonic() - cycle_start)))
        finally:
            if discovery_on:
                registry.save(force=True)


async def _sleep(stop_event: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
