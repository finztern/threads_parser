"""account_worker: one asyncio task per enabled account. Each cycle:
fetch new posts via Playwright, filter by max_age, dedup via SeenCache,
rate-limit via TokenBucket, batch-send via pipeline.send_batch.
"""
from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timezone

import httpx

from scraper.browser import ThreadsBrowserSession
from scraper.config import AccountConfig, ConfigStore
from scraper.constants import LOGGER
from scraper.pipeline import RawThreadsPost, build_payload, send_batch
from scraper.state import SeenCache, TokenBucket


async def account_worker(
    account: AccountConfig,
    config_store: ConfigStore,
    token_bucket: TokenBucket,
    seen_cache: SeenCache,
    stop_event: asyncio.Event,
) -> None:
    session = ThreadsBrowserSession(account)

    try:
        await session.start()
    except Exception:
        LOGGER.exception("[%s] failed to start browser session, worker exiting", account.name)
        return

    async with httpx.AsyncClient() as client:
        try:
            while not stop_event.is_set():
                cycle_start = time.monotonic()
                cfg = config_store.get()

                try:
                    await _run_cycle(account, session, client, cfg, token_bucket, seen_cache)
                except NotImplementedError as exc:
                    # Expected in v1 until fetch_new_posts is implemented.
                    LOGGER.warning("[%s] %s", account.name, exc)
                except Exception:
                    LOGGER.exception("[%s] cycle failed", account.name)

                elapsed = time.monotonic() - cycle_start
                base_interval = cfg.get("poll_interval_seconds", 60)
                jitter_ratio = cfg.get("poll_jitter_ratio", 0.25)
                jitter = base_interval * jitter_ratio * (2 * random.random() - 1)
                sleep_for = max(0.0, base_interval + jitter - elapsed)

                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
                except asyncio.TimeoutError:
                    pass
        finally:
            await session.close()


async def _run_cycle(
    account: AccountConfig,
    session: ThreadsBrowserSession,
    client: httpx.AsyncClient,
    cfg: dict,
    token_bucket: TokenBucket,
    seen_cache: SeenCache,
) -> None:
    targets = cfg.get("targets", [])
    max_feed_scrolls = cfg.get("max_feed_scrolls", 3)
    scroll_pause_seconds = cfg.get("scroll_pause_seconds", 1.5)
    fetch_replies = cfg.get("fetch_replies", True)

    raw_posts: list[RawThreadsPost] = await session.fetch_new_posts(
        targets,
        max_feed_scrolls=max_feed_scrolls,
        scroll_pause_seconds=scroll_pause_seconds,
        fetch_replies=fetch_replies,
    )

    stats = await dispatch_posts(raw_posts, cfg, client, token_bucket, seen_cache)
    LOGGER.info(
        "[%s] cycle: fetched=%d fresh=%d to_send=%d sent=%d unconfirmed=%d "
        "duplicates=%d rate_limited=%d",
        account.name,
        stats["fetched"],
        stats["fresh"],
        stats["to_send"],
        stats["sent"],
        stats["unconfirmed"],
        stats["duplicates"],
        stats["rate_limited"],
    )


async def dispatch_posts(
    raw_posts: list[RawThreadsPost],
    cfg: dict,
    client: httpx.AsyncClient,
    token_bucket: TokenBucket,
    seen_cache: SeenCache,
) -> dict[str, int]:
    """Shared by browser and http modes: filter by max_age, dedup via
    SeenCache, rate-limit via TokenBucket, batch-send to store_items.
    Returns counters for logging.
    """
    max_age_seconds = cfg.get("max_age_seconds", 86400)
    token_wait_timeout = cfg.get("token_wait_timeout_seconds", 10)
    batch_max_items = cfg.get("batch_max_items", 200)
    store_endpoint = cfg["store_endpoint"]

    now = datetime.now(timezone.utc)
    fresh = [
        p for p in raw_posts if (now - p.created_at.astimezone(timezone.utc)).total_seconds() <= max_age_seconds
    ]

    to_send: list[RawThreadsPost] = []
    duplicates = 0
    rate_limited = 0
    for post in fresh:
        if not await seen_cache.try_claim(post.external_id):
            duplicates += 1
            continue
        if not await token_bucket.acquire(timeout=token_wait_timeout):
            rate_limited += 1
            await seen_cache.release(post.external_id)
            continue
        to_send.append(post)

    sent = 0
    unconfirmed = 0
    for i in range(0, len(to_send), batch_max_items):
        chunk = to_send[i : i + batch_max_items]
        payloads = [build_payload(p) for p in chunk]
        confirmed_ids, failed_ids = await send_batch(client, store_endpoint, payloads)

        for post in chunk:
            if post.external_id in confirmed_ids:
                await seen_cache.confirm(post.external_id)
                sent += 1
            else:
                await seen_cache.release(post.external_id)
                unconfirmed += 1

    return {
        "fetched": len(raw_posts),
        "fresh": len(fresh),
        "to_send": len(to_send),
        "sent": sent,
        "unconfirmed": unconfirmed,
        "duplicates": duplicates,
        "rate_limited": rate_limited,
    }
