"""Does the logged-out "Replies" tab of a profile (/@user/replies) give many comments per request?

Research helper (nothing here is used by the scraper, nothing is sent to the collector).
For every profile it fetches https://www.threads.com/@USER/replies through a fresh Tor circuit and
reports: HTTP status / login wall, how many posts of that user are replies, which structure the
parent has, the age of those replies and - with the url-check endpoint of config.yaml - how many of
them are NEW to Exorde. Post texts are never printed.

Usage (inside the running container; no arguments = the 10 most productive known profiles):
  docker compose exec parser python3 scripts/replies_tab_probe.py
  docker compose exec parser python3 scripts/replies_tab_probe.py user1 user2 ...
Needs TOR_HOST / TOR_BASE_PORT / TOR_INSTANCES like the parser (they are set in the container).
"""
from __future__ import annotations

import asyncio
import json
import os
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402
import yaml  # noqa: E402
from curl_cffi import requests as cffi  # noqa: E402

from scraper.constants import BASE_DIR  # noqa: E402
from scraper.threads_parse import (  # noqa: E402
    THREADS_BASE_URL,
    _loads,
    _parse_post_node,
    _sjs_blocks,
    _walk_thread_nodes,
)
from scraper.tor_pool import TorPool  # noqa: E402

HEADERS = {"Accept-Language": "en-US,en;q=0.9"}
DEBUG_DIR = BASE_DIR / "debug"


def default_users(count: int = 10) -> list[str]:
    path = BASE_DIR / "data" / "targets_state.json"
    try:
        targets = json.loads(path.read_text(encoding="utf-8"))["targets"]
    except (OSError, ValueError, KeyError):
        return ["zuck", "mrbeast", "nasa"]
    ranked = sorted(targets.items(), key=lambda kv: -(kv[1][4] if len(kv[1]) > 4 else 0.0))
    return [name for name, _ in ranked[:count]]


def make_pool() -> TorPool:
    return TorPool(os.getenv("TOR_HOST", "tor"), int(os.getenv("TOR_BASE_PORT", "9050")), int(os.getenv("TOR_INSTANCES", "10") or 10))


async def fetch(pool: TorPool, url: str) -> tuple[int, str, str]:
    """(status, final url, html) - three tries on different circuits."""
    last = (0, "", "")
    for _ in range(3):
        proxy = pool.next()
        try:
            async with cffi.AsyncSession(impersonate="chrome124", timeout=25) as session:
                response = await session.get(url, headers=HEADERS, allow_redirects=True, proxies={"http": proxy, "https": proxy})
            last = (response.status_code, str(response.url), response.text)
            if response.status_code == 200 and "/login" not in str(response.url):
                return last
        except Exception as exc:  # noqa: BLE001 - a probe: show and go on
            last = (0, f"error: {str(exc)[:100]}", "")
    return last


async def check_known(urls: list[str]) -> set[str]:
    cfg = yaml.safe_load((BASE_DIR / "config.yaml").read_text(encoding="utf-8")) or {}
    endpoint = cfg.get("url_check_endpoint") or ""
    if not endpoint or not urls:
        return set()
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(endpoint, json={"urls": urls})
        response.raise_for_status()
        return {u for u in response.json() if isinstance(u, str)}


def analyse(username: str, html: str) -> dict:
    """What the replies tab holds, without printing any text."""
    now = time.time()
    own: list[dict] = []
    nodes = 0
    info_keys: Counter[str] = Counter()
    shapes: Counter[str] = Counter()
    for raw in _sjs_blocks(html, "thread_items"):
        try:
            data = _loads(raw)
        except ValueError:
            continue
        for node in _walk_thread_nodes(data):
            nodes += 1
            items = [(i or {}).get("post") or {} for i in node["thread_items"]]
            shapes[",".join("U" if (p.get("user") or {}).get("username", "").lower() == username.lower() else "o" for p in items)] += 1
            for index, post in enumerate(items):
                if (post.get("user") or {}).get("username", "").lower() != username.lower():
                    continue
                tpai = post.get("text_post_app_info") or {}
                info_keys.update(k for k in tpai if "reply" in k or "parent" in k or "root" in k)
                parsed = _parse_post_node(post, external_parent_id="")
                if parsed is None:
                    continue
                parent_pk = str(items[index - 1].get("pk")) if index > 0 and items[index - 1].get("pk") else ""
                own.append(
                    {
                        "url": parsed.url,
                        "is_reply": bool(tpai.get("is_reply")),
                        "has_prev": bool(parent_pk),
                        "prev_other": index > 0 and (items[index - 1].get("user") or {}).get("username", "").lower() != username.lower(),
                        "age_min": (now - parsed.created_at.timestamp()) / 60,
                        "media": bool(parsed.extra.get("has_media")),
                    }
                )
    return {"nodes": nodes, "own": own, "info_keys": info_keys, "shapes": shapes}


async def main(users: list[str]) -> None:
    pool = make_pool()
    DEBUG_DIR.mkdir(exist_ok=True)
    total_replies = total_new = pages = 0
    for username in users:
        status, final, html = await fetch(pool, f"{THREADS_BASE_URL}/@{username}/replies")
        login = "/login" in final
        print(f"\n@{username}: HTTP {status}{' LOGIN WALL' if login else ''} {len(html) // 1024} KB sjs={html.count('data-sjs')}")
        if status != 200 or login or not html:
            print(f"  final url: {final[:120]}")
            continue
        (DEBUG_DIR / f"replies_tab_{username}.html").write_text(html, encoding="utf-8")
        result = analyse(username, html)
        own = result["own"]
        replies = [o for o in own if o["is_reply"]]
        pages += 1
        print(f"  thread nodes: {result['nodes']}, posts by @{username}: {len(own)}, of them replies: {len(replies)}")
        print(f"  node shapes (U=this user, o=other): {dict(result['shapes'].most_common(4))}")
        print(f"  reply-related keys: {dict(result['info_keys'])}")
        if replies:
            ages = [o["age_min"] for o in replies]
            print(
                f"  replies age: min {min(ages):.0f} min, median {statistics.median(ages):.0f} min, max {max(ages) / 60:.1f} h; "
                f"with a previous item in the node: {sum(o['has_prev'] for o in replies)}, previous is another user: "
                f"{sum(o['prev_other'] for o in replies)}, with media: {sum(o['media'] for o in replies)}"
            )
            try:
                known = await check_known([o["url"] for o in replies])
                new = len(replies) - len(known)
                total_replies += len(replies)
                total_new += new
                print(f"  NEW to Exorde: {new} of {len(replies)}")
            except Exception as exc:  # noqa: BLE001
                print(f"  url-check failed: {str(exc)[:100]}")
    print(f"\nTOTAL: {pages} pages, {total_replies} replies found, {total_new} new -> {total_new / max(pages, 1):.1f} new per page")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:] or default_users()))
