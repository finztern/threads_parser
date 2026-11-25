"""Browserless Threads access: plain HTTP (curl_cffi with a Chrome TLS
fingerprint) through a rotating proxy pool, no accounts.

Threads server-renders the first page of a profile and of a post (with its
direct replies) into the HTML as <script type="application/json" data-sjs>
blocks, so one GET returns real posts/replies. Parsing reuses the extractors
in scraper/threads_parse.py (nothing is invented).
"""
from __future__ import annotations

import asyncio
import time
import multiprocessing
import os
import re
from collections import Counter, OrderedDict, deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from typing import Callable

from curl_cffi.requests import AsyncSession

from scraper.threads_parse import (
    THREADS_BASE_URL,
    _extract_ssr_feed_posts,
    _extract_ssr_reply_posts,
    _extract_ssr_user_replies,
    parse_bytes,
    parse_shm,
)
from scraper.constants import LOGGER
from scraper.pipeline import RawThreadsPost
from scraper.threads_graphql import (
    BOOT_URL,
    GRAPHQL_URL,
    TAB_URL,
    build_request,
    build_tab_request,
    find_lsd,
    parse_graphql_replies,
    parse_graphql_tab,
)
from scraper.proxies import ProxyPool, mask_proxy, transport_options

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


class _GqlSession:
    """A curl session that already loaded a Threads page: it holds the lsd token the GraphQL query needs."""

    __slots__ = ("session", "request_opts", "lsd", "uses", "lock", "seen")

    def __init__(self, session, request_opts: dict) -> None:
        self.session = session
        self.request_opts = request_opts
        self.lsd: str | None = None
        self.uses = 0
        self.lock = asyncio.Lock()
        self.seen = time.monotonic()


class _GqlRejected(Exception):
    """The bootstrap page or the query was refused (HTTP status kept for the statistics)."""

    def __init__(self, status: int, what: str) -> None:
        super().__init__(f"{what} HTTP {status}")
        self.status = status


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
        static_timeout: float = 0.0,
    ) -> None:
        """parse_workers: >0 that many parser processes, 0 = auto, None = parse
        inline in the event loop (tests). Parsing a ~1 MB page is CPU work, so at
        tens of requests per second it must not share one core with the network.
        concurrency: max simultaneous HTTP requests (all targets together).
        """
        self._pool = pool
        self._timeout = timeout
        self._static_timeout = float(static_timeout)  # 0 = same as `timeout`; free proxies must not hold a slot for long
        self._attempts = max(1, attempts)
        self._impersonate = impersonate
        self._error_cooldown = error_cooldown
        self._req_sem = asyncio.Semaphore(max(1, int(concurrency)))
        self._max_rps = 0.0  # 0 = no cap
        # Persistent curl sessions for the static / vpn sources (one per proxy): a new session per request costs a TLS
        # handshake and a curl_multi cleanup in the event-loop thread (about 40% of its time in the profile).
        self._reuse = False
        self._reuse_limit = 300
        self._reuse_static = False  # free public proxies: one throwaway session each (hundreds of live sessions eat GBs)
        self._sessions: dict[str, AsyncSession] = {}
        self._session_uses: dict[str, int] = {}
        self._session_seen: dict[str, float] = {}
        self._next_slot = 0.0
        self._conn_window: deque[bool] = deque(maxlen=100)  # True = could not connect to the proxy
        self.stats: Counter[str] = Counter()
        self.error_kinds: Counter[str] = Counter()  # normalized transport error messages, per cycle
        self.status_kinds: Counter[str] = Counter()  # HTTP status / block reason of non-ok pages, per cycle
        # Anonymous GraphQL sessions (one per proxy: the lsd token and cookies belong to the session).
        self._gql: dict[str, _GqlSession] = {}
        self._gql_uses_limit = 150
        self._user_ids: dict[str, str] = {}  # username (lower) -> numeric id, learned from parsed pages
        self._tab_gql = False  # Replies tab through GraphQL (set by the worker from config graphql_tabs)
        self._tab_first = 5
        self._executor: ProcessPoolExecutor | None = None
        # PARSE_SHM=1 sends pages to the parse worker through shared memory instead of pickle. Measured: not faster
        # (a new block per page costs more than the pickle it saves), so it is off by default.
        self._use_shm = os.environ.get("PARSE_SHM", "0").strip().lower() in ("1", "true", "yes", "on")
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
        self._prune_sessions()

    def close(self) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=True)

    async def _parse(self, fn: Callable[[str, str], list[RawThreadsPost]], body: bytes, arg: str) -> list[RawThreadsPost]:
        if self._executor is None:
            return parse_bytes(fn, body, arg)
        loop = asyncio.get_running_loop()
        shm = None
        if self._use_shm and body:
            try:
                from multiprocessing import shared_memory

                shm = shared_memory.SharedMemory(create=True, size=len(body))
                shm.buf[: len(body)] = body
            except Exception:
                # /dev/shm full or unavailable: fall back to pickling this page.
                self.stats["shm_fallback"] += 1
                if shm is not None:
                    self._free_shm(shm)
                shm = None
        try:
            if shm is not None:
                return await loop.run_in_executor(self._executor, parse_shm, fn, shm.name, len(body), arg)
            return await loop.run_in_executor(self._executor, parse_bytes, fn, body, arg)
        except Exception:
            # Broken worker pool etc.: never lose the page, parse inline.
            LOGGER.exception("parser process failed, parsing inline")
            self.stats["parse_fallback"] += 1
            return parse_bytes(fn, body, arg)
        finally:
            if shm is not None:
                self._free_shm(shm)

    @staticmethod
    def _free_shm(shm) -> None:
        try:
            shm.close()
            shm.unlink()
        except Exception:
            pass

    def set_session_reuse(self, enabled: bool, max_uses: int = 300, reuse_static: bool = False) -> None:
        self._reuse = bool(enabled)
        self._reuse_limit = max(10, int(max_uses))
        self._reuse_static = bool(reuse_static)
        if not self._reuse and self._sessions:
            for proxy in list(self._sessions):
                self._retire(proxy)

    def session_count(self) -> int:
        return len(self._sessions)

    def _retire(self, proxy: str) -> None:
        """Stop handing out this session; close it a minute later (requests may still be running on it)."""
        session = self._sessions.pop(proxy, None)
        self._session_uses.pop(proxy, None)
        self._session_seen.pop(proxy, None)
        if session is None:
            return

        async def _close() -> None:
            await asyncio.sleep(60.0)
            try:
                await session.close()
            except Exception:  # noqa: BLE001
                pass

        asyncio.get_running_loop().create_task(_close())

    def _prune_sessions(self) -> None:
        cutoff = time.monotonic() - 600.0
        for proxy, seen in list(self._session_seen.items()):
            if seen < cutoff:
                self._retire(proxy)
        for proxy, entry in list(self._gql.items()):
            if entry.seen < cutoff:
                self._retire_gql(proxy)

    async def _get(self, proxy: str, src: str, url: str, timeout: float):
        session_opts, request_opts = transport_options(proxy)
        if not (self._reuse and (src == "vpn" or (src == "static" and self._reuse_static))):
            async with AsyncSession(impersonate=self._impersonate, timeout=timeout, **session_opts) as session:
                return await session.get(url, headers=HEADERS, allow_redirects=True, **request_opts)
        session = self._sessions.get(proxy)
        if session is None:
            session = self._sessions[proxy] = AsyncSession(impersonate=self._impersonate, timeout=timeout, **session_opts)
            self._session_uses[proxy] = 0
        self._session_seen[proxy] = time.monotonic()
        try:
            session.cookies.clear()  # keep every request anonymous: only the TCP/TLS connection is reused
        except Exception:  # noqa: BLE001
            pass
        try:
            response = await session.get(url, headers=HEADERS, allow_redirects=True, timeout=timeout, **request_opts)
        except Exception:
            if self._sessions.get(proxy) is session:
                self._retire(proxy)  # a broken connection must not be reused
            raise
        self._session_uses[proxy] = self._session_uses.get(proxy, 0) + 1
        if self._session_uses[proxy] >= self._reuse_limit and self._sessions.get(proxy) is session:
            self._retire(proxy)  # recycle now and then
        return response

    def set_max_rps(self, rps: float) -> None:
        """Global cap on HTTP requests per second (0 = off); keeps the per-IP 429 limits from being burned."""
        self._max_rps = max(0.0, float(rps))

    async def _pace(self) -> None:
        if self._max_rps <= 0:
            return
        now = time.monotonic()
        slot = max(now, self._next_slot)
        self._next_slot = slot + 1.0 / self._max_rps
        if slot > now:
            await asyncio.sleep(slot - now)

    async def _fetch_parsed(
        self, url: str, parse: Callable[[str, str], list[RawThreadsPost]], arg: str, empty_retries: int = 2
    ) -> tuple[str, list[RawThreadsPost]]:
        """GET url through up to `attempts` different proxies.

        Returns (label, items): "ok" (items found), "empty" (page fine but
        nothing parsed, possibly a transient Threads error), "not_found",
        or "failed" (proxy/transport problems on every attempt), or "walled" (login wall: needs an account).
        """
        last = "failed"
        empties = 0
        kind = "post" if "/post/" in url else "tab" if url.endswith("/replies") else "profile"
        for attempt in range(self._attempts):
            response = None
            error: Exception | None = None
            await self._pace()
            # The slot is held only for the network round trip, not for parsing.
            async with self._req_sem:
                # After a failed attempt prefer the cleaner source if the pool has one (static proxies see far fewer 429).
                pick = getattr(self._pool, "next_retry", self._pool.next) if attempt > 0 else self._pool.next
                proxy = pick()  # pick the proxy once the slot is free (fresh ban state)
                self.stats["requests"] += 1
                src = self._pool.source(proxy) if hasattr(self._pool, "source") else "pool"
                request_timeout = self._static_timeout if (src in ("static", "vpn") and self._static_timeout > 0) else self._timeout
                self.stats[f"src_{src}_requests"] += 1
                try:
                    response = await self._get(proxy, src, url, request_timeout)
                except Exception as exc:
                    error = exc

            if error is not None:
                exc = error
                self.stats["transport_errors"] += 1
                self.error_kinds[re.sub(r"\d+", "N", str(exc))[:90]] += 1
                if "timed out" in str(exc).lower() or "timeout" in str(exc).lower():
                    # Slow, not necessarily dead (overloaded link / our own concurrency): short pause.
                    self.stats["timeouts"] += 1
                    self.stats[f"src_{src}_timeout"] += 1
                    self._pool.ban(proxy, 60.0)
                else:
                    self.stats["conn_errors"] += 1
                    self.stats[f"src_{src}_error"] += 1
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
                self.status_kinds[f"429_{kind}"] += 1
                self.stats[f"src_{src}_429"] += 1
                self.stats["blocked"] += 1
                self._pool.ban(proxy, 300.0)
                last = "failed"
                continue
            if "/login" in str(response.url):
                # Measured: no profile that hit the login wall ever opened through another proxy, so
                # the wall belongs to the page, not to the exit. Do not retry, do not ban the proxy.
                self._pool.ok(proxy)
                self.stats["walled"] += 1
                self.status_kinds[f"login_{kind}"] += 1
                return "walled", []
            if status in (401, 403) or status >= 500:
                self.status_kinds[f"{status}_{kind}"] += 1
                self.stats["blocked"] += 1
                self._pool.ban(proxy)
                last = "failed"
                continue
            body = response.content
            self.stats[f"bytes_{kind}"] += len(body)  # decoded size, only to compare request kinds (wire is ~5-8x smaller)
            self.stats[f"pages_{kind}"] += 1
            if status != 200 or b"data-sjs" not in body:
                self.status_kinds[f"bad_page_{status}"] += 1
                self.stats["bad_page"] += 1
                self._pool.ban(proxy)
                last = "failed"
                continue

            self._pool.ok(proxy)
            self.stats[f"src_{src}_ok"] += 1
            items = await self._parse(parse, body, arg)
            if items:
                return "ok", items
            self.stats["empty_page"] += 1
            last = "empty"  # maybe a transient "Something went wrong"; retry once on another proxy
            empties += 1
            if empties >= empty_retries:  # private / no posts: do not burn more requests on it
                break
        return last, []

    async def fetch_profile(self, username: str) -> tuple[str, list[RawThreadsPost]]:
        label, posts = await self._fetch_parsed(f"{THREADS_BASE_URL}/@{username}", _extract_ssr_feed_posts, username)
        self.learn_user_ids(posts)
        return label, posts

    async def fetch_user_replies(self, username: str) -> tuple[str, list[RawThreadsPost]]:
        """Replies written by @username (the "Replies" tab). Nothing found is normal (the user did not
        reply lately), so an empty page is not retried."""
        if self._tab_gql:
            label, posts = await self.fetch_user_replies_gql(username)
            if label == "ok":
                return ("ok" if posts else "empty"), posts
            self.stats["gql_tab_fallback"] += 1
        label, posts = await self._fetch_parsed(
            f"{THREADS_BASE_URL}/@{username}/replies", _extract_ssr_user_replies, username, empty_retries=1
        )
        self.learn_user_ids(posts)
        return label, posts

    def _retire_gql(self, proxy: str) -> None:
        entry = self._gql.pop(proxy, None)
        if entry is None:
            return

        async def _close() -> None:
            await asyncio.sleep(60.0)
            try:
                await entry.session.close()
            except Exception:  # noqa: BLE001
                pass

        asyncio.get_running_loop().create_task(_close())

    async def _gql_request(self, proxy: str, make: Callable[[str], tuple[str, str, dict[str, str]]], timeout: float) -> bytes:
        """One replies query on the proxy's own session. The first use loads a small page for the lsd token."""
        entry = self._gql.get(proxy)
        if entry is None:
            session_opts, request_opts = transport_options(proxy)
            entry = self._gql[proxy] = _GqlSession(
                AsyncSession(impersonate=self._impersonate, timeout=timeout, **session_opts), request_opts
            )
        entry.seen = time.monotonic()
        try:
            async with entry.lock:
                if entry.lsd is None:
                    boot = await entry.session.get(
                        BOOT_URL, headers=HEADERS, allow_redirects=True, timeout=timeout, **entry.request_opts
                    )
                    self.stats["gql_boot"] += 1
                    self.stats["bytes_gql_boot"] += len(boot.content)
                    lsd = find_lsd(boot.text) if boot.status_code == 200 else None
                    if not lsd:
                        raise _GqlRejected(boot.status_code, "gql bootstrap")
                    entry.lsd = lsd
            url, data, headers = make(entry.lsd)
            csrf = entry.session.cookies.get("csrftoken")
            if csrf:
                headers["X-CSRFToken"] = csrf
            response = await entry.session.post(
                url, data=data, headers=headers, timeout=timeout, **entry.request_opts
            )
        except Exception:
            if self._gql.get(proxy) is entry:
                self._retire_gql(proxy)  # a broken connection or an expired token must not be reused
            raise
        entry.uses += 1
        if entry.uses >= self._gql_uses_limit and self._gql.get(proxy) is entry:
            self._retire_gql(proxy)
        if response.status_code != 200:
            raise _GqlRejected(response.status_code, "gql")
        return response.content

    async def _gql_page(
        self, page_url: str, post_pk: str, first: int, after: str | None
    ) -> tuple[str, list[RawThreadsPost], str | None, bool]:
        """One page of the newest replies through up to `attempts` different proxies: (label, replies, cursor, has_next)."""

        def make(lsd: str) -> tuple[str, str, dict[str, str]]:
            data, headers = build_request(lsd, page_url, post_pk, first, after)
            return GRAPHQL_URL, data, headers

        label, posts, cursor, has_next = await self._gql_run(make, parse_graphql_replies, post_pk)
        return label, posts, cursor, has_next

    async def fetch_user_replies_gql(self, username: str) -> tuple[str, list[RawThreadsPost]]:
        """Replies written by @username through the anonymous Replies-tab query: ~3x fewer bytes than the tab page.
        Needs the numeric userID (learned from earlier pages). Label "ok" (also empty list) or "failed" (caller falls back)."""
        user_id = self._user_ids.get(username.lower())
        if not user_id:
            return "failed", []
        page_url = f"{THREADS_BASE_URL}/@{username}/replies"
        first = self._tab_first

        def make(lsd: str) -> tuple[str, str, dict[str, str]]:
            data, headers = build_tab_request(lsd, page_url, user_id, first)
            return TAB_URL, data, headers

        label, posts, _cursor, _more = await self._gql_run(make, parse_graphql_tab, username, tab=True)
        if label == "ok":
            self.stats["gql_tab_ok"] += 1
        return label, posts

    def learn_user_ids(self, posts: list[RawThreadsPost]) -> None:
        """Remember username -> numeric id from parsed pages (the GraphQL tab query needs the id)."""
        for post in posts:
            pk = getattr(post, "author_pk", "")
            if pk and post.username:
                if len(self._user_ids) > 300_000 and post.username.lower() not in self._user_ids:
                    continue
                self._user_ids[post.username.lower()] = pk

    async def _gql_run(self, make, parse_fn, arg: str, tab: bool = False):
        """Shared transport of both GraphQL queries: (label, posts, cursor, has_next)."""
        for attempt in range(self._attempts):
            await self._pace()
            async with self._req_sem:
                pick = getattr(self._pool, "next_retry", self._pool.next) if attempt > 0 else self._pool.next
                proxy = pick()
                self.stats["requests"] += 1
                src = self._pool.source(proxy) if hasattr(self._pool, "source") else "pool"
                timeout = self._static_timeout if (src in ("static", "vpn") and self._static_timeout > 0) else self._timeout
                self.stats[f"src_{src}_requests"] += 1
                try:
                    body = await self._gql_request(proxy, make, timeout)
                except _GqlRejected as exc:
                    self.stats["gql_rejected"] += 1
                    self.status_kinds[f"{exc.status}_gql"] += 1
                    if exc.status in (401, 403, 429) or exc.status >= 500:
                        self.stats["blocked"] += 1
                        self._pool.ban(proxy, 300.0 if exc.status == 429 else self._pool_cooldown())
                    continue
                except Exception as exc:  # noqa: BLE001 - transport problems, same handling as for pages
                    self.stats["transport_errors"] += 1
                    self.error_kinds[re.sub(r"\d+", "N", str(exc))[:90]] += 1
                    if "timed out" in str(exc).lower() or "timeout" in str(exc).lower():
                        self.stats["timeouts"] += 1
                        self.stats[f"src_{src}_timeout"] += 1
                        self._pool.ban(proxy, 60.0)
                    else:
                        self.stats["conn_errors"] += 1
                        self.stats[f"src_{src}_error"] += 1
                        self._conn_window.append(True)
                        if not self._local_trouble():
                            self._pool.ban(proxy, self._error_cooldown)
                    continue
            self._conn_window.append(False)
            self.stats["bytes_gql"] += len(body)
            self.stats["pages_gql"] += 1
            if tab:
                ok, posts = await self._parse(parse_graphql_tab, body, arg)
                cursor, has_next = None, False
            else:
                ok, posts, cursor, has_next = await self._parse(parse_fn, body, arg)
            if not ok:
                self.stats["gql_bad_answer"] += 1
                continue  # not a replies answer (error JSON): try another proxy
            self._pool.ok(proxy)
            self.stats[f"src_{src}_ok"] += 1
            self.stats["gql_comments"] += len(posts)
            return "ok", posts, cursor, has_next
        return "failed", [], None, False

    def _pool_cooldown(self) -> float:
        return 120.0

    async def fetch_recent_replies(
        self, username: str, code: str, post_pk: str, first: int = 20, max_pages: int = 3, page_age_seconds: float = 600.0
    ) -> tuple[str, list[RawThreadsPost]]:
        """Newest replies of a post through the anonymous GraphQL query (sortOrder=RECENT).

        Pages back in time while a page still holds at least 3 replies younger than `page_age_seconds` (older ones are
        mostly known to Exorde already), at most `max_pages` requests. Label: "ok" (also with an empty list: nothing new) or
        "failed" (the caller falls back to the post page).
        """
        page_url = f"{THREADS_BASE_URL}/@{username}/post/{code}"
        collected: dict[str, RawThreadsPost] = {}
        cursor: str | None = None
        now = datetime.now(timezone.utc)
        for page_no in range(max(1, max_pages)):
            label, posts, cursor, has_next = await self._gql_page(page_url, post_pk, max(1, first), cursor)
            if label != "ok":
                if page_no == 0:
                    return "failed", []
                break  # a later page failed: keep what we have
            for post in posts:
                collected[post.external_id] = post
            ages = [(now - p.created_at.astimezone(timezone.utc)).total_seconds() for p in posts]
            # Threads of RECENT are ordered by activity, so a page can hold an old reply inside a fresh thread: judge the page
            # by how many replies are still young, not by its oldest one.
            if not has_next or not cursor or sum(1 for a in ages if a <= page_age_seconds) < 3:
                break
        return "ok", list(collected.values())

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
            session_opts, request_opts = transport_options(proxy)
            async with AsyncSession(impersonate=impersonate, timeout=timeout, **session_opts) as session:
                response = await session.post(endpoint, json={"urls": urls}, **request_opts)
        except Exception:
            pool.ban(proxy, 60.0)
            raise
        if response.status_code != 200:
            pool.ban(proxy, 60.0)
            raise RuntimeError(f"url-check via {mask_proxy(proxy)} answered HTTP {response.status_code}")
        return response.json()

    return post
