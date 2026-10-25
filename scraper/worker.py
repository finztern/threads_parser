"""dispatch_posts: filter by max_age, dedup via SeenCache, rate-limit via
TokenBucket, batch-send to store_items via pipeline.send_batch.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

from scraper.pipeline import RawThreadsPost, build_payload, send_batch
from scraper.state import SeenCache, TokenBucket


async def dispatch_posts(
    raw_posts: list[RawThreadsPost],
    cfg: dict,
    client: httpx.AsyncClient,
    token_bucket: TokenBucket,
    seen_cache: SeenCache,
) -> dict[str, int]:
    """Filter by max_age, dedup via
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
