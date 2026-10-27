"""dispatch_posts: filter by max_age, dedup via SeenCache, rate-limit via
TokenBucket, batch-send to store_items via pipeline.send_batch.
"""
from __future__ import annotations

from datetime import datetime, timezone

import httpx

from scraper.pipeline import RawThreadsPost, build_payload, check_url_duplicates, send_batch
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

    claimed: list[RawThreadsPost] = []
    duplicates = 0
    for post in fresh:
        if await seen_cache.try_claim(post.external_id):
            claimed.append(post)
        else:
            duplicates += 1

    # Skip URLs Exorde already has. FAIL CLOSED: if the check did not succeed for a URL it is not
    # sent (the claim is released, so it is retried on a later cycle).
    remote_duplicates = 0
    remote_duplicates_replies = 0  # subset of remote_duplicates that are replies (yield statistics)
    url_unverified = 0
    url_check_endpoint = cfg.get("url_check_endpoint")
    if url_check_endpoint and claimed:
        known, unverified = await check_url_duplicates(
            client, url_check_endpoint, [p.url for p in claimed], timeout=cfg.get("url_check_timeout_seconds", 10)
        )
        still: list[RawThreadsPost] = []
        for post in claimed:
            if post.url in known:
                remote_duplicates += 1
                remote_duplicates_replies += bool(post.external_parent_id)
                await seen_cache.confirm(post.external_id)  # remember: do not ask again
            elif post.url in unverified:
                url_unverified += 1
                await seen_cache.release(post.external_id)
            else:
                still.append(post)
        claimed = still

    to_send: list[RawThreadsPost] = []
    rate_limited = 0
    for post in claimed:
        if not await token_bucket.acquire(timeout=token_wait_timeout):
            rate_limited += 1
            await seen_cache.release(post.external_id)
            continue
        to_send.append(post)

    sent = 0
    sent_replies = 0
    unconfirmed = 0
    for i in range(0, len(to_send), batch_max_items):
        chunk = to_send[i : i + batch_max_items]
        payloads = [build_payload(p) for p in chunk]
        confirmed_ids, failed_ids = await send_batch(client, store_endpoint, payloads)

        for post in chunk:
            if post.external_id in confirmed_ids:
                await seen_cache.confirm(post.external_id)
                sent += 1
                sent_replies += bool(post.external_parent_id)
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
        "remote_duplicates": remote_duplicates,
        "rate_limited": rate_limited,
        # Yield statistics: replies are the posts with a non-empty parent id.
        "fresh_replies": sum(1 for p in fresh if p.external_parent_id),
        "remote_dup_replies": remote_duplicates_replies,
        "sent_replies": sent_replies,
        "url_unverified": url_unverified,
    }
