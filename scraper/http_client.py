"""Browserless Threads access: plain HTTP (curl_cffi with a Chrome TLS
fingerprint) through a rotating proxy pool, no accounts.

Threads server-renders the first page of a profile and of a post (with its
direct replies) into the HTML as <script type="application/json" data-sjs>
blocks, so one GET returns real posts/replies. Parsing reuses the extractors
in scraper/browser.py (same schema, nothing is invented).
"""
from __future__ import annotations

from collections import Counter, OrderedDict
from datetime import datetime, timezone
from typing import Callable

from curl_cffi.requests import AsyncSession

from scraper.browser import THREADS_BASE_URL, _extract_ssr_feed_posts, _extract_ssr_reply_posts
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

    def needs_fetch(self, post_id: str, reply_count: int) -> bool:
        return reply_count > 0 and self._counts.get(post_id, -1) < reply_count

    def mark(self, post_id: str, reply_count: int) -> None:
        self._counts[post_id] = reply_count
        self._counts.move_to_end(post_id)
        while len(self._counts) > self._max:
            self._counts.popitem(last=False)


class ThreadsHttpClient:
    def __init__(
        self,
        pool: ProxyPool,
        timeout: float = 25.0,
        attempts: int = 3,
        impersonate: str = "chrome124",
    ) -> None:
        self._pool = pool
        self._timeout = timeout
        self._attempts = max(1, attempts)
        self._impersonate = impersonate
        self.stats: Counter[str] = Counter()

    async def _fetch_parsed(self, url: str, parse: Callable[[str], list[RawThreadsPost]]) -> tuple[str, list[RawThreadsPost]]:
        """GET url through up to `attempts` different proxies.

        Returns (label, items): "ok" (items found), "empty" (page fine but
        nothing parsed, possibly a transient Threads error), "not_found",
        or "failed" (proxy/transport/wall problems on every attempt).
        """
        last = "failed"
        empties = 0
        for _ in range(self._attempts):
            proxy = self._pool.next()
            self.stats["requests"] += 1
            try:
                async with AsyncSession(impersonate=self._impersonate, timeout=self._timeout) as session:
                    response = await session.get(
                        url, headers=HEADERS, proxies={"http": proxy, "https": proxy}, allow_redirects=True
                    )
            except Exception as exc:
                self.stats["transport_errors"] += 1
                self._pool.ban(proxy)
                LOGGER.debug("request via %s failed: %s", mask_proxy(proxy), exc)
                last = "failed"
                continue

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

            items = parse(response.text)
            if items:
                return "ok", items
            self.stats["empty_page"] += 1
            last = "empty"  # maybe a transient "Something went wrong"; retry once on another proxy
            empties += 1
            if empties >= 2:  # private / no posts: do not burn more requests on it
                break
        return last, []

    async def fetch_profile(self, username: str) -> tuple[str, list[RawThreadsPost]]:
        return await self._fetch_parsed(
            f"{THREADS_BASE_URL}/@{username}", lambda html: _extract_ssr_feed_posts(html, username)
        )

    async def fetch_post_replies(
        self, username: str, code: str, post_pk: str
    ) -> tuple[str, list[RawThreadsPost]]:
        return await self._fetch_parsed(
            f"{THREADS_BASE_URL}/@{username}/post/{code}",
            lambda html: _extract_ssr_reply_posts(html, post_pk),
        )

    async def fetch_target(
        self,
        username: str,
        tracker: ReplyTracker,
        reply_post_max_age_seconds: float,
        fetch_replies: bool = True,
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
            for post in posts:
                count = int((post.extra or {}).get("direct_reply_count") or 0)
                age = (now - post.created_at.astimezone(timezone.utc)).total_seconds()
                if age > reply_post_max_age_seconds or not tracker.needs_fetch(post.external_id, count):
                    continue
                code = post.url.rsplit("/post/", 1)[-1]
                reply_pages += 1
                rlabel, replies = await self.fetch_post_replies(username, code, post.external_id)
                if rlabel == "ok":
                    tracker.mark(post.external_id, count)
                    for reply in replies:
                        collected[reply.external_id] = reply
        return "ok", list(collected.values()), reply_pages

