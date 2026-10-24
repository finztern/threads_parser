"""Playwright session for one Threads account: proxy + auth state applied,
ready to navigate.

Auth state priority: storage_state/<account>.json (real interactive login,
cookies + localStorage) if present, else cookie_file (Cookie Editor JSON).

fetch_new_posts() is implemented via response interception, not by
hand-building GraphQL requests: several required headers on Threads'
internal GraphQL calls (x-fb-lsd, x-asbd-id, x-bloks-version-id,
x-web-session-id, x-ig-app-id) are session/build-specific and are set by
Threads' own client JS, not something we can safely hardcode or guess.
Instead, we let the real logged-in page make its own requests while we
navigate/scroll it, and we read the JSON bodies of the matching GraphQL
responses. Every post/reply below is built ONLY from fields present in
those real responses - see docs/threads_graphql_examples for the schema
this is based on.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from playwright.async_api import Browser, BrowserContext, Page, Response, async_playwright

from scraper.config import AccountConfig, load_cookies
from scraper.constants import BASE_DIR, BROWSER_NAV_TIMEOUT_MS, LOGGER
from scraper.pipeline import RawThreadsPost

# Real site host (NOT the same as the Exorde schema's fixed "domain": "threads.net").
THREADS_BASE_URL = "https://www.threads.com"

# GraphQL friendly-names we listen for, confirmed from a real authenticated
# session (see docs/threads_graphql_examples/README.md for doc_id per name).
FEED_QUERY_NAMES = {"BarcelonaFeedDirectQuery", "BarcelonaFeedPaginationDirectQuery"}
POST_QUERY_NAMES = {"BarcelonaPostPageTargetQuery", "BarcelonaPostPageDownwardQuery"}
GRAPHQL_URL_PARTS = ("/graphql/query", "/api/graphql")

# Substrings in page.url meaning we got bounced to a login/checkpoint page.
LOGIN_WALL_MARKERS = ("accounts/login", "/login", "checkpoint")

DEBUG_DIR = BASE_DIR / "debug"


def _is_graphql_response(response: Response) -> bool:
    return response.request.method == "POST" and any(part in response.url for part in GRAPHQL_URL_PARTS)


def _friendly_name(response: Response) -> str | None:
    post_data = response.request.post_data
    if not post_data:
        return None
    for part in post_data.split("&"):
        if part.startswith("fb_api_req_friendly_name="):
            return part.split("=", 1)[1]
    return None


def _parse_post_node(node: dict[str, Any], external_parent_id: str) -> RawThreadsPost | None:
    """Build a RawThreadsPost from one 'post'/'node' object of the Threads
    GraphQL schema (same shape in feed, target post and replies).
    Returns None if a required real field is missing - never invents
    ids/timestamps/content.
    """
    pk = node.get("pk")
    caption = node.get("caption") or {}
    content = caption.get("text")
    taken_at = node.get("taken_at")
    user = node.get("user") or {}
    username = user.get("username")
    code = node.get("code")

    if not pk or content is None or not taken_at or not username or not code:
        return None

    tpai = node.get("text_post_app_info") or {}
    return RawThreadsPost(
        external_id=str(pk),
        content=content,
        created_at=datetime.fromtimestamp(taken_at, tz=timezone.utc),
        url=f"{THREADS_BASE_URL}/@{username}/post/{code}",
        author=user.get("full_name") or username,
        username=username,
        # Exorde parent rule: "" for top-level, real parent post id for a reply.
        external_parent_id=external_parent_id,
        extra={
            "like_count": node.get("like_count"),
            "direct_reply_count": tpai.get("direct_reply_count"),
            "is_reply": tpai.get("is_reply", bool(external_parent_id)),
        },
    )


def _extract_feed_posts(payload: dict[str, Any]) -> list[RawThreadsPost]:
    """data.feedData.edges[].node.text_post_app_thread.thread_items[].post
    thread_items can hold more than one post (self-thread) - all top-level,
    so external_parent_id is always "" here.
    """
    posts: list[RawThreadsPost] = []
    edges = payload.get("data", {}).get("feedData", {}).get("edges", [])
    for edge in edges:
        thread = (edge.get("node") or {}).get("text_post_app_thread") or {}
        for item in thread.get("thread_items", []):
            node = item.get("post")
            if not node:
                continue
            post = _parse_post_node(node, external_parent_id="")
            if post:
                posts.append(post)
    return posts


def _extract_reply_posts(payload: dict[str, Any], parent_post_id: str) -> list[RawThreadsPost]:
    """data.media.text_post_app_info.direct_replies.edges[].node.posts.edges[].node

    Threads does not embed a parent id inside the reply object itself -
    the link is structural: parent_post_id is the postID we requested
    replies FOR (see docs/threads_graphql_examples/README.md).
    """
    posts: list[RawThreadsPost] = []
    media = payload.get("data", {}).get("media") or {}
    direct_replies = (media.get("text_post_app_info") or {}).get("direct_replies") or {}
    for edge in direct_replies.get("edges", []):
        reply_node = edge.get("node") or {}
        for inner_edge in (reply_node.get("posts") or {}).get("edges", []):
            node = inner_edge.get("node")
            if not node:
                continue
            post = _parse_post_node(node, external_parent_id=parent_post_id)
            if post:
                posts.append(post)
    return posts


class ThreadsBrowserSession:
    """One Playwright browser + context per account, with proxy and
    cookies applied. Mandatory proxy per Exorde requirements.
    """

    def __init__(self, account: AccountConfig) -> None:
        self._account = account
        self._playwright = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    async def start(self) -> None:
        if not self._account.proxy.server:
            raise ValueError(
                f"[{self._account.name}] proxy.server is required - refusing to "
                "start a browser session without a proxy"
            )

        self._playwright = await async_playwright().start()

        proxy_config = {"server": self._account.proxy.server}
        if self._account.proxy.username:
            proxy_config["username"] = self._account.proxy.username
        if self._account.proxy.password:
            proxy_config["password"] = self._account.proxy.password

        self._browser = await self._playwright.chromium.launch(
            headless=True,
            proxy=proxy_config,
        )
        if self._account.storage_state_file.exists():
            LOGGER.info("[%s] using storage_state %s", self._account.name, self._account.storage_state_file)
            self._context = await self._browser.new_context(
                storage_state=str(self._account.storage_state_file),
                user_agent=self._account.user_agent or None,
            )
        else:
            LOGGER.info(
                "[%s] no storage_state at %s, falling back to cookie_file %s",
                self._account.name,
                self._account.storage_state_file,
                self._account.cookie_file,
            )
            self._context = await self._browser.new_context(
                user_agent=self._account.user_agent or None,
            )
            await self._context.add_cookies(load_cookies(self._account.cookie_file))
        self._context.set_default_navigation_timeout(BROWSER_NAV_TIMEOUT_MS)

        self._page = await self._context.new_page()
        LOGGER.info("[%s] browser session started", self._account.name)

    async def close(self) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()
        LOGGER.info("[%s] browser session closed", self._account.name)

    async def _dump_debug(self, tag: str) -> None:
        """Save screenshot + HTML of the current page under debug/ (gitignored)."""
        assert self._page is not None
        try:
            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            shot = DEBUG_DIR / f"{self._account.name}_{tag}.png"
            html = DEBUG_DIR / f"{self._account.name}_{tag}.html"
            await self._page.screenshot(path=str(shot), full_page=True)
            html.write_text(await self._page.content(), encoding="utf-8")
            LOGGER.warning("[%s] debug dump saved: %s, %s", self._account.name, shot, html)
        except Exception:
            LOGGER.exception("[%s] failed to save debug dump", self._account.name)

    async def _check_login_wall(self, expected_path: str) -> bool:
        """True (and a debug dump) if the page landed on a login/checkpoint."""
        assert self._page is not None
        url = self._page.url
        if "threads.com" not in url or any(m in url for m in LOGIN_WALL_MARKERS):
            LOGGER.warning(
                "[%s] expected %s but landed on %s - login wall/checkpoint "
                "(auth state expired or session flagged)",
                self._account.name,
                expected_path,
                url,
            )
            await self._dump_debug("login_wall")
            return True
        return False

    async def fetch_new_posts(
        self,
        targets: list[dict[str, str]],
        max_feed_scrolls: int = 3,
        scroll_pause_seconds: float = 1.5,
        fetch_replies: bool = True,
    ) -> list[RawThreadsPost]:
        """Scrape new posts/replies for the given targets.

        v1 supports target type "profile" only (a username). For each
        profile: open it, scroll a bit to trigger pagination, collect
        top-level posts from the feed responses the page itself makes;
        then, for posts that report replies, open the post page and
        collect its DIRECT replies the same way.

        NOT implemented yet (left for a follow-up once needed):
        - target type "post"/"thread" (a specific thread URL)
        - upward parent-chain fetch (BarcelonaPostPageUpwardQuery)
        - "show more replies" deep pagination (...RepliesRefetchQuery)
        These are additive - the doc_ids for them are already captured in
        docs/threads_graphql_examples if/when we wire them in.
        """
        if self._page is None:
            raise RuntimeError(f"[{self._account.name}] browser session not started - call start() first")

        all_posts: dict[str, RawThreadsPost] = {}

        for target in targets:
            target_type = target.get("type")
            value = target.get("value")
            if not value:
                continue
            if target_type != "profile":
                LOGGER.warning(
                    "[%s] target type %r not supported yet (only 'profile' in v1), skipping %r",
                    self._account.name,
                    target_type,
                    value,
                )
                continue

            top_level = await self._fetch_profile_feed(value, max_feed_scrolls, scroll_pause_seconds)
            for post in top_level:
                all_posts[post.external_id] = post

            if not fetch_replies:
                continue

            for post in top_level:
                reply_count = (post.extra or {}).get("direct_reply_count") or 0
                if not reply_count:
                    continue
                code = post.url.rsplit("/post/", 1)[-1]
                try:
                    replies = await self._fetch_post_replies(value, code, post.external_id, scroll_pause_seconds)
                except Exception:
                    LOGGER.exception(
                        "[%s] failed to fetch replies for post %s", self._account.name, post.external_id
                    )
                    continue
                for reply in replies:
                    all_posts[reply.external_id] = reply

        return list(all_posts.values())

    async def _fetch_profile_feed(
        self, username: str, max_scrolls: int, scroll_pause_seconds: float
    ) -> list[RawThreadsPost]:
        assert self._page is not None
        captured: list[Response] = []

        async def on_response(response: Response) -> None:
            if _is_graphql_response(response) and _friendly_name(response) in FEED_QUERY_NAMES:
                captured.append(response)

        self._page.on("response", on_response)
        try:
            await self._page.goto(f"{THREADS_BASE_URL}/@{username}", wait_until="networkidle")
            await self._check_login_wall(f"/@{username}")
            for _ in range(max_scrolls):
                await self._page.mouse.wheel(0, 4000)
                await self._page.wait_for_timeout(int(scroll_pause_seconds * 1000))
        finally:
            self._page.remove_listener("response", on_response)

        posts: list[RawThreadsPost] = []
        for response in captured:
            try:
                payload = await response.json()
            except Exception:
                LOGGER.warning("[%s] failed to parse GraphQL response body: %s", self._account.name, response.url)
                continue
            posts.extend(_extract_feed_posts(payload))

        if not captured:
            LOGGER.warning(
                "[%s] no feed GraphQL response captured for @%s - dumping page for inspection",
                self._account.name,
                username,
            )
            await self._dump_debug("no_feed_captured")
        return posts

    async def _fetch_post_replies(
        self, username: str, code: str, post_pk: str, pause_seconds: float
    ) -> list[RawThreadsPost]:
        assert self._page is not None
        captured: list[Response] = []

        async def on_response(response: Response) -> None:
            if _is_graphql_response(response) and _friendly_name(response) in POST_QUERY_NAMES:
                captured.append(response)

        self._page.on("response", on_response)
        try:
            await self._page.goto(f"{THREADS_BASE_URL}/@{username}/post/{code}", wait_until="networkidle")
            await self._check_login_wall(f"/@{username}/post/{code}")
            await self._page.wait_for_timeout(int(pause_seconds * 1000))
        finally:
            self._page.remove_listener("response", on_response)

        replies: list[RawThreadsPost] = []
        for response in captured:
            try:
                payload = await response.json()
            except Exception:
                LOGGER.warning("[%s] failed to parse GraphQL response body: %s", self._account.name, response.url)
                continue
            replies.extend(_extract_reply_posts(payload, parent_post_id=post_pk))
        return replies