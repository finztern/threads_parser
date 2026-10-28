"""Browserless Threads access: plain HTTP (curl_cffi with a Chrome TLS
fingerprint) through a rotating proxy pool, no accounts.

Threads server-renders the first page of a profile and of a post (with its
direct replies) into the HTML as <script type="application/json" data-sjs>
blocks, so one GET returns real posts/replies. Parsing reuses the extractors
in scraper/threads_parse.py (nothing is invented).
"""
from __future__ import annotations

import asyncio
import multiprocessing
import os
import re
from collections import Counter, OrderedDict, deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from typing import Callable

from curl_cffi.requests import AsyncSession

from scraper.threads_parse import THREADS_BASE_URL, _extract_ssr_feed_posts, _extract_ssr_reply_posts
from scraper.constants import LOGGER
from scraper.pipeline import RawThreadsPost
from scraper.proxies import ProxyPool, mask_proxy

HEADERS = {"Accept-Language": "en-US,en;q=0.9"}


class ReplyTracker:
    """Remembers the reply count we last saw per post, so a post page is
    re-opened only when it got new replies (bounded memory).
    """

    def __init__(self, max_size: int = 200_000) -> None:
        self._max = max_size
        self._counts: OrderedDict[str, int] = OrderedDict()

    def needs_fetch(self, post_id: str, reply_count: int, min_growth: int = 1) -> bool:
        """Open the post page only when at least `min_growth` replies appeared since the last time
        we fetched it (never fetched = all its replies are new). Several new replies per request
        instead of one; the growth keeps accumulating until the threshold is reached.
        """
        return reply_count > 0 and reply_count - self._counts.get(post_id, 0) >= max(1, min_growth)

    def mark(self, post_id: str, reply_count: int) -> None:
        self._counts[post_id] = reply_count
        self._counts.move_to_end(post_id)
        while len(self._counts) > self._max:
            self._counts.popitem(last=False)


class ThreadsHttpClient:
    def __init__(
        self,
        pool: ProxyPool,
        timeout: float = 20.0,
        attempts: int = 3,
        impersonate: str = "chrome124",
        parse_workers: int | None = 0,
        error_cooldown: float = 600.0,
        concurrency: int = 30,
    ) -> None:
        """parse_workers: >0 that many parser processes, 0 = auto, None = parse
        inline in the event loop (tests). Parsing a ~1 MB page is CPU work, so at
        tens of requests per second it must not share one core with the network.
        concurrency: max simultaneous HTTP requests (all targets together).
        """
        self._pool = pool
        self._timeout = timeout
        self._attempts = max(1, attempts)
        self._impersonate = impersonate
        self._error_cooldown = error_cooldown
        self._req_sem = asyncio.Semaphore(max(1, int(concurrency)))
        self._conn_window: deque[bool] = deque(maxlen=100)  # True = could not connect to the proxy
        self.stats: Counter[str] = Counter()
        self.error_kinds: Counter[str] = Counter()  # normalized transport error messages, per cycle
        self._executor: ProcessPoolExecutor | None = None
        if parse_workers is not None:
            workers = parse_workers if parse_workers > 0 else max(1, min(os.cpu_count() or 2, 4))
            # "spawn": forking a process that already runs network threads is unsafe.
            self._executor = ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn"))

    def _local_trouble(self) -> bool:
        """True when more than half of the last attempts failed to connect at all."""
        window = self._conn_window
        return len(window) >= 50 and sum(window) / len(window) > 0.5

    def set_concurrency(self, concurrency: int) -> None:
        """Hot-reload of http_concurrency. Call only between cycles (no requests in flight)."""
        self._req_sem = asyncio.Semaphore(max(1, int(concurrency)))

    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)

    async def _parse(self, fn: Callable[[str, str], list[RawThreadsPost]], html: str, arg: str) -> list[RawThreadsPost]:
        if self._executor is None:
            return fn(html, arg)
        try:
            return await asyncio.get_running_loop().run_in_executor(self._executor, fn, html, arg)
        except Exception:
            # Broken worker pool etc.: never lose the page, parse inline.
            LOGGER.exception("parser process failed, parsing inline")
            self.stats["parse_fallback"] += 1
            return fn(html, arg)

    async def _fetch_parsed(
        self, url: str, parse: Callable[[str, str], list[RawThreadsPost]], arg: str
    ) -> tuple[str, list[RawThreadsPost]]:
        """GET url through up to `attempts` different proxies.

        Returns (label, items): "ok" (items found), "empty" (page fine but
        nothing parsed, possibly a transient Threads error), "not_found",
        or "failed" (proxy/transport/wall problems on every attempt).
        """
        last = "failed"
        empties = 0
        for _ in range(self._attempts):
            response = None
            error: Exception | None = None
            # The slot is held only for the network round trip, not for parsing.
            async with self._req_sem:
                proxy = self._pool.next()  # pick the proxy once the slot is free (fresh ban state)
                self.stats["requests"] += 1
                try:
                    async with AsyncSession(impersonate=self._impersonate, timeout=self._timeout) as session:
                        response = await session.get(
                            url, headers=HEADERS, proxies={"http": proxy, "https": proxy}, allow_redirects=True
                        )
                except Exception as exc:
                    error = exc

            if error is not None:
                exc = error
                self.stats["transport_errors"] += 1
                self.error_kinds[re.sub(r"\d+", "N", str(exc))[:90]] += 1
                if "timed out" in str(exc).lower() or "timeout" in str(exc).lower():
                    # Slow, not necessarily dead (overloaded link / our own concurrency): short pause.
                    self.stats["timeouts"] += 1
                    self._pool.ban(proxy, 60.0)
                else:
                    self.stats["conn_errors"] += 1
                    self._conn_window.append(True)
                    if self._local_trouble():
                        # Most attempts cannot connect at all: our side (fds, conntrack, provider
                        # connection cap) is the problem. Banning proxies one by one would empty the pool.
                        self.stats["conn_errors_unbanned"] += 1
                    else:
                        self._pool.ban(proxy, self._error_cooldown)  # refused / auth / reset: dead proxy
                    if self.stats["conn_errors"] <= 5:
                        LOGGER.warning("proxy error via %s: %s", mask_proxy(proxy), str(exc)[:200])
                LOGGER.debug("request via %s failed: %s", mask_proxy(proxy), exc)
                last = "failed"
                continue

            self._conn_window.append(False)
            status = response.status_code
            if status == 404:
                self.stats["not_found"] += 1
                return "not_found", []
            if status == 429:
                self.stats["blocked"] += 1
                self._pool.ban(proxy, 300.0)
                last = "failed"
                continue
            if status in (401, 403) or status >= 500 or "/login" in str(response.url):
                self.stats["blocked"] += 1
                self._pool.ban(proxy)
                last = "failed"
                continue
            if status != 200 or "data-sjs" not in response.text:
                self.stats["bad_page"] += 1
                self._pool.ban(proxy)
                last = "failed"
                continue

            items = await self._parse(parse, response.text, arg)
            if items:
                return "ok", items
            self.stats["empty_page"] += 1
            last = "empty"  # maybe a transient "Something went wrong"; retry once on another proxy
            empties += 1
            if empties >= 2:  # private / no posts: do not burn more requests on it
                break
        return last, []

    async def fetch_profile(self, username: str) -> tuple[str, list[RawThreadsPost]]:
        return await self._fetch_parsed(f"{THREADS_BASE_URL}/@{username}", _extract_ssr_feed_posts, username)

    async def fetch_post_replies(
        self, username: str, code: str, post_pk: str
    ) -> tuple[str, list[RawThreadsPost]]:
        return await self._fetch_parsed(
            f"{THREADS_BASE_URL}/@{username}/post/{code}", _extract_ssr_reply_posts, post_pk
        )

    async def fetch_target(
        self,
        username: str,
        tracker: ReplyTracker,
        reply_post_max_age_seconds: float,
        fetch_replies: bool = True,
        reply_min_growth: int = 1,
    ) -> tuple[str, list[RawThreadsPost], int]:
        """Profile posts + (where it makes sense) their direct replies.

        Returns (profile_label, posts_and_replies, reply_pages_fetched).
        """
        label, posts = await self.fetch_profile(username)
        if not posts:
            return label, [], 0

        collected: dict[str, RawThreadsPost] = {p.external_id: p for p in posts}
        reply_pages = 0
        if fetch_replies:
            now = datetime.now(timezone.utc)
            eligible: list[tuple[RawThreadsPost, int]] = []
            for post in posts:
                count = int((post.extra or {}).get("direct_reply_count") or 0)
                age = (now - post.created_at.astimezone(timezone.utc)).total_seconds()
                if age > reply_post_max_age_seconds or not tracker.needs_fetch(post.external_id, count, reply_min_growth):
                    continue
                eligible.append((post, count))

            async def one(post: RawThreadsPost) -> tuple[str, list[RawThreadsPost]]:
                code = post.url.rsplit("/post/", 1)[-1]
                return await self.fetch_post_replies(username, code, post.external_id)

            # Reply pages of one profile run in parallel (bounded by the shared request semaphore).
            reply_pages = len(eligible)
            results = await asyncio.gather(*(one(p) for p, _ in eligible), return_exceptions=True)
            for (post, count), result in zip(eligible, results):
                if isinstance(result, BaseException):
                    LOGGER.warning("reply page for %s failed: %s", post.url, result)
                    continue
                rlabel, replies = result
                if rlabel == "ok":
                    tracker.mark(post.external_id, count)
                    for reply in replies:
                        collected[reply.external_id] = reply
        return "ok", list(collected.values()), reply_pages


def make_url_check_poster(pool: ProxyPool, impersonate: str | None = None):
    """POST {"urls": [...]} to the url-check API through a proxy from the pool (fresh proxy and a
    fresh connection per request). Raises on transport errors and non-200 answers; the caller
    retries, which picks another proxy.
    """

    async def post(endpoint: str, urls: list[str], timeout: float):
        proxy = pool.next()
        try:
            async with AsyncSession(impersonate=impersonate, timeout=timeout) as session:
                response = await session.post(
                    endpoint, json={"urls": urls}, proxies={"http": proxy, "https": proxy}
                )
        except Exception:
            pool.ban(proxy, 60.0)
            raise
        if response.status_code != 200:
            pool.ban(proxy, 60.0)
            raise RuntimeError(f"url-check via {mask_proxy(proxy)} answered HTTP {response.status_code}")
        return response.json()

    return post
