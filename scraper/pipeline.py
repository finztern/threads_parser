"""build_payload: raw scraped Threads post -> Exorde item schema.
send_batch: POST a batch of items to STORE_ENDPOINT (/store_items).
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from scraper.constants import DOMAIN, HTTP_TIMEOUT_SECONDS, LOGGER


class RawThreadsPost:
    """Shape expected from scraper/threads_parse.py for one scraped post/reply.

    All fields must come from the real page data - never invented.
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
    # exorde_data Item accepts only the "...Z" form, not "+00:00".
    created_at_iso = post.created_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
def describe_exc(exc: BaseException) -> str:
    """Exception type + message + underlying causes (with errno), e.g. the OSError hidden behind
    httpx's generic "All connection attempts failed".
    """
    parts: list[str] = []
    queue: list[BaseException] = [exc]
    while queue and len(parts) < 6:
        cur = queue.pop(0)
        errno = getattr(cur, "errno", None)
        text = str(cur)[:80]
        parts.append(f"{type(cur).__name__}(errno={errno}) {text}" if errno else f"{type(cur).__name__} {text}")
        queue.extend(getattr(cur, "exceptions", None) or [])
        nxt = cur.__cause__ or cur.__context__
        if nxt is not None:
            queue.append(nxt)
    return " <- ".join(parts)


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


# Max simultaneous POSTs to the collector. A burst of ~100 parallel connects can overflow the accept
# queue of a simple single-process collector, which then answers with "connection refused".
_STORE_SEM = asyncio.Semaphore(max(1, int(os.getenv("STORE_CONCURRENCY", "12"))))

# --- url-check ---------------------------------------------------------------------------------
# Exorde's URL dedup API is queried in batches by ONE sender task (all targets queue their URLs),
# optionally through the proxy pool (set_url_check_poster). It fails CLOSED: a URL whose check did
# not succeed is reported as "unverified" and is never sent.
_URLCHECK_FAIL_LIMIT = 5
_URLCHECK_PAUSE_SECONDS = 30.0
_urlcheck_failures = 0
_urlcheck_skip_until = 0.0

_URL_BATCH_SIZE = 300  # max urls per request
_URL_MIN_BATCH = 30  # wait until at least this many urls are queued ...
_URL_MAX_WAIT_SECONDS = 2.0  # ... but never longer than this
UrlPoster = Callable[[str, list[str], float], Awaitable[Any]]  # (endpoint, urls, timeout) -> parsed JSON
_URL_POSTER: UrlPoster | None = None
_URL_PENDING: list[tuple[list[str], asyncio.Future]] = []
_URL_FLUSHER: asyncio.Task | None = None


def configure_url_check(batch_size: int, min_batch: int, max_wait: float = 2.0) -> None:
    global _URL_BATCH_SIZE, _URL_MIN_BATCH, _URL_MAX_WAIT_SECONDS
    _URL_BATCH_SIZE = max(1, int(batch_size))
    _URL_MIN_BATCH = max(1, min(int(min_batch), _URL_BATCH_SIZE))
    _URL_MAX_WAIT_SECONDS = max(0.0, float(max_wait))


def set_url_check_poster(poster: UrlPoster | None) -> None:
    """Route url-check requests through `poster` (e.g. the proxy pool) instead of the shared client."""
    global _URL_POSTER
    _URL_POSTER = poster


async def _post_url_check(client: httpx.AsyncClient, endpoint: str, urls: list[str], timeout: float) -> Any:
    if _URL_POSTER is not None:
        return await _URL_POSTER(endpoint, urls, timeout)
    response = await client.post(endpoint, json={"urls": urls}, timeout=timeout)
    response.raise_for_status()
    return response.json()


async def _query_url_check(
    client: httpx.AsyncClient, endpoint: str, urls: list[str], timeout: float
) -> tuple[set[str], set[str]]:
    """Ask Exorde which of `urls` it already has (last 3 days).

    Returns (duplicates, unverified). Unverified = the check failed for them (after one retry);
    the caller must NOT send those.
    """
    duplicates: set[str] = set()
    unverified: set[str] = set()

    async def check_chunk(part: list[str]) -> None:
        global _urlcheck_failures, _urlcheck_skip_until
        if time.monotonic() < _urlcheck_skip_until:
            unverified.update(part)
            return
        data: Any = None
        error: BaseException | None = None
        for _attempt in range(2):  # the service sometimes never answers one request: retry (new proxy)
            try:
                data = await _post_url_check(client, endpoint, part, timeout)
                error = None
                break
            except Exception as exc:
                error = exc
        if error is None and isinstance(data, list):
            _urlcheck_failures = 0
            duplicates.update(u for u in data if isinstance(u, str))
            return
        unverified.update(part)
        if error is not None:
            LOGGER.warning("url check failed (%d urls), not sending them: %s", len(part), describe_exc(error))
        else:
            LOGGER.warning("url check: unexpected answer %.200s", json.dumps(data, ensure_ascii=False))
        _urlcheck_failures += 1
        if _urlcheck_failures >= _URLCHECK_FAIL_LIMIT:
            _urlcheck_failures = 0
            _urlcheck_skip_until = time.monotonic() + _URLCHECK_PAUSE_SECONDS
            LOGGER.warning(
                "url check paused for %.0fs after repeated failures (nothing is sent meanwhile)",
                _URLCHECK_PAUSE_SECONDS,
            )

    # Chunks of one flush go out in parallel (each through its own proxy): one slow proxy
    # must not hold back the whole queue.
    await asyncio.gather(*(check_chunk(urls[i : i + _URL_BATCH_SIZE]) for i in range(0, len(urls), _URL_BATCH_SIZE)))
    return duplicates, unverified


async def _url_flush_loop(client: httpx.AsyncClient, endpoint: str, timeout: float) -> None:
    while _URL_PENDING:
        waited = 0.0
        while waited < _URL_MAX_WAIT_SECONDS and sum(len(u) for u, _ in _URL_PENDING) < _URL_MIN_BATCH:
            await asyncio.sleep(0.1)
            waited += 0.1
        batch = _URL_PENDING[:]
        _URL_PENDING.clear()
        duplicates: set[str] = set()
        unverified: set[str] = {u for urls, _ in batch for u in urls}  # pessimistic until proven otherwise
        try:
            unique = list(dict.fromkeys(u for urls, _ in batch for u in urls))
            duplicates, unverified = await _query_url_check(client, endpoint, unique, timeout)
        except Exception:
            LOGGER.exception("url check flush failed, nothing from this batch is sent")
        finally:
            for urls, fut in batch:
                if not fut.done():
                    fut.set_result(({u for u in urls if u in duplicates}, {u for u in urls if u in unverified}))


async def check_url_duplicates(
    client: httpx.AsyncClient, endpoint: str, urls: list[str], timeout: float = 10.0
) -> tuple[set[str], set[str]]:
    """Queue `urls` for the shared batched check. Returns (known_duplicates, unverified)."""
    global _URL_FLUSHER
    if not urls:
        return set(), set()
    if time.monotonic() < _urlcheck_skip_until:
        return set(), set(urls)
    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    _URL_PENDING.append((urls, fut))
    if _URL_FLUSHER is None or _URL_FLUSHER.done():
        _URL_FLUSHER = loop.create_task(_url_flush_loop(client, endpoint, timeout))
    return await fut


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

    response: httpx.Response | None = None
    last_exc: BaseException | None = None
    # A refused/failed CONNECT means the request never reached the collector, so repeating it cannot
    # duplicate items. This rides out short collector gaps (restart, full accept queue).
    for delay in (0.0, 1.0, 2.0, 4.0):
        if delay:
            await asyncio.sleep(delay)
        try:
            async with _STORE_SEM:
                response = await _post_batch(client, store_endpoint, items)
            break
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            last_exc = exc
        except (httpx.HTTPError, httpx.TransportError) as exc:
            last_exc = exc
            break
    if response is None:
        LOGGER.warning("send_batch failed for %d item(s): %s", len(items), describe_exc(last_exc) if last_exc else "?")
        return set(), all_ids

    try:
        data = response.json()
    except ValueError:
        # 2xx with no JSON body: assume the whole batch was accepted.
        return all_ids, set()

    results = data.get("results") if isinstance(data, dict) else None
    if isinstance(results, dict) and all(isinstance(v, int) for v in results.values()):
        # The local collector answers with aggregate counters, not per-item
        # statuses, e.g. {"count": 99, "results": {"to_gpu": 16, "dropped": 69, ...}}.
        # HTTP 2xx means the batch was received; what the collector then did
        # with each item (to_gpu = accepted, dropped/duplicate/... = filtered)
        # is only visible here, so log it.
        LOGGER.info(
            "store_items: received=%s breakdown=%s",
            data.get("count"),
            json.dumps(results, sort_keys=True),
        )
        return all_ids, set()

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
