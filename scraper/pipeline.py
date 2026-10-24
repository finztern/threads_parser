"""build_payload: raw scraped Threads post -> Exorde item schema.
send_batch: POST a batch of items to STORE_ENDPOINT (/store_items).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from scraper.constants import DOMAIN, HTTP_TIMEOUT_SECONDS, LOGGER


class RawThreadsPost:
    """Shape expected from scraper/browser.py for one scraped post/reply.

    All fields must come from the real page/DOM - never invented.
    """

    def __init__(
        self,
        external_id: str,
        content: str,
        created_at: datetime,
        url: str,
        author: str,
        username: str,
        external_parent_id: str = "",
        extra: dict[str, Any] | None = None,
    ) -> None:
        self.external_id = external_id
        self.content = content
        self.created_at = created_at
        self.url = url
        self.author = author
        self.username = username
        # Exorde parent rule: "" for a top-level post, real parent post id for a reply.
        self.external_parent_id = external_parent_id
        self.extra = extra or {}


def build_payload(post: RawThreadsPost) -> dict[str, Any]:
    """Map a RawThreadsPost to the Exorde /store_items item schema.

    summary must be a JSON *string*, not an object.
    """
    created_at_iso = post.created_at.astimezone(timezone.utc).isoformat()

    summary_obj = {
        "platform": "threads",
        **post.extra,
    }

    return {
        "content": post.content,
        "external_id": post.external_id,
        "created_at": created_at_iso,
        "url": post.url,
        "title": "",  # Threads posts have no separate title.
        "author": post.author,
        "username": post.username,
        "external_parent_id": post.external_parent_id,
        "domain": DOMAIN,
        "summary": json.dumps(summary_obj, ensure_ascii=False),
    }


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    reraise=True,
)
async def _post_batch(
    client: httpx.AsyncClient, store_endpoint: str, items: list[dict[str, Any]]
) -> httpx.Response:
    response = await client.post(
        store_endpoint,
        json={"items": items},
        timeout=HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response


async def send_batch(
    client: httpx.AsyncClient, store_endpoint: str, items: list[dict[str, Any]]
) -> tuple[set[str], set[str]]:
    """POST items to store_endpoint. Returns (confirmed_ids, failed_ids).

    NOTE: the exact per-item confirmation shape of the local collector's
    response is not fixed here - adjust the parsing below once the real
    /store_items response format is confirmed. Current behavior:
    - HTTP 2xx with no parseable per-item result -> whole batch confirmed.
    - HTTP error / network error after retries -> whole batch failed
      (SeenCache claims are released so items are retried next cycle).
    """
    if not items:
        return set(), set()

    all_ids = {item["external_id"] for item in items}

    try:
        response = await _post_batch(client, store_endpoint, items)
    except (httpx.HTTPError, httpx.TransportError) as exc:
        LOGGER.warning("send_batch failed for %d item(s): %s", len(items), exc)
        return set(), all_ids

    try:
        data = response.json()
    except ValueError:
        # 2xx with no JSON body: assume the whole batch was accepted.
        return all_ids, set()

    results = data.get("results") if isinstance(data, dict) else None
    if results is not None and not (isinstance(results, list) and all(isinstance(r, dict) for r in results)):
        # Unknown per-item format (a dict, plain strings, ...): do not crash the cycle.
        # HTTP 2xx -> assume the whole batch was accepted; log a sample so the
        # parsing below can be adapted to the real collector response.
        LOGGER.warning(
            "store_items returned unrecognized per-item results, assuming batch accepted. sample=%.300s",
            json.dumps(data, ensure_ascii=False),
        )
        return all_ids, set()

    if isinstance(data, dict) and "results" in data:
        confirmed: set[str] = set()
        failed: set[str] = set()
        for result in data["results"]:
            ext_id = result.get("external_id")
            if ext_id is None:
                continue
            if result.get("ok", result.get("status") == "ok"):
                confirmed.add(ext_id)
            else:
                failed.add(ext_id)
        # Anything not mentioned in results is treated as unconfirmed.
        failed |= all_ids - confirmed - failed
        return confirmed, failed

    # 2xx, JSON body, but no per-item structure recognized -> assume all ok.
    return all_ids, set()
