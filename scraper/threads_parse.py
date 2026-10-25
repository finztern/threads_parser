"""Parsing of Threads pages: posts and replies embedded in the server-rendered
HTML as <script type="application/json" data-sjs> blocks.

Every post/reply is built ONLY from fields present in the real page data;
a post missing a required field is skipped, nothing is invented.
Top-level functions only (picklable, usable from a process pool).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from scraper.pipeline import RawThreadsPost

# Real site host (NOT the same as the Exorde schema's fixed "domain": "threads.net").
THREADS_BASE_URL = "https://www.threads.com"


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


try:  # orjson parses JSON several times faster and with less memory than json
    import orjson as _fastjson

    def _loads(raw: str) -> Any:
        return _fastjson.loads(raw)
except ImportError:  # pragma: no cover - optional dependency
    def _loads(raw: str) -> Any:
        return json.loads(raw)


def _sjs_blocks(html: str, marker: str):
    """Yield data-sjs script bodies that contain `marker`.

    Plain str.find scanning (C speed): a regex over a ~1 MB page costs far
    more CPU, and most blocks are irrelevant and never need parsing.
    """
    if marker not in html:
        return
    pos = 0
    while True:
        end = html.find("</script>", pos)
        if end == -1:
            return
        start = html.rfind("<script", pos, end)
        pos = end + 9
        if start == -1:
            continue
        head_end = html.find(">", start, end)
        if head_end == -1 or "data-sjs" not in html[start:head_end]:
            continue
        body = html[head_end + 1 : end]
        if marker in body:
            yield body


def _walk_thread_nodes(obj: Any):
    """Yield every dict that holds a 'thread_items' list, at any depth."""
    if isinstance(obj, dict):
        if isinstance(obj.get("thread_items"), list):
            yield obj
        for value in obj.values():
            yield from _walk_thread_nodes(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_thread_nodes(value)


def _extract_ssr_feed_posts(html: str, username: str) -> list[RawThreadsPost]:
    """Top-level posts of @username embedded in the profile page HTML.

    Same 'post' object shape as in GraphQL feed responses, so the real
    fields go through _parse_post_node (nothing is invented). Only posts
    authored by the requested username are kept.
    """
    posts: dict[str, RawThreadsPost] = {}
    for raw in _sjs_blocks(html, "thread_items"):
        try:
            data = _loads(raw)
        except ValueError:
            continue
        for node in _walk_thread_nodes(data):
            for item in node["thread_items"]:
                post_node = (item or {}).get("post")
                if not post_node:
                    continue
                post = _parse_post_node(post_node, external_parent_id="")
                if post and post.username.lower() == username.lower():
                    posts[post.external_id] = post
    return list(posts.values())


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


def _walk_data_roots(obj: Any):
    """Yield every dict that looks like a GraphQL result root ({"data": {"media": ...}})."""
    if isinstance(obj, dict):
        data = obj.get("data")
        if isinstance(data, dict) and isinstance(data.get("media"), dict):
            yield obj
        for value in obj.values():
            yield from _walk_data_roots(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_data_roots(value)


def _extract_ssr_reply_posts(html: str, parent_post_id: str) -> list[RawThreadsPost]:
    """Direct replies embedded in a post page's HTML (same schema as the
    GraphQL post-page response, so _extract_reply_posts does the parsing).
    """
    posts: dict[str, RawThreadsPost] = {}
    for raw in _sjs_blocks(html, "direct_replies"):
        try:
            data = _loads(raw)
        except ValueError:
            continue
        for root in _walk_data_roots(data):
            for post in _extract_reply_posts(root, parent_post_id):
                posts[post.external_id] = post
    return list(posts.values())
