"""Probe a page WITHOUT any account: plain browser through one proxy.

Usage (inside the parser image):
    python3 scripts/anon_probe.py <url>

Env:
    PROBE_PROXY  proxy URL, e.g. http://user:pass@host:port or socks5://host:port
                 (keep it in .env, never in git or chat)

Reports where the page landed, whether the SSR JSON with posts is present
(Threads) and whether a login wall/captcha is shown. Saves
debug/anon_dump.html and debug/anon_dump.png for inspection.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from playwright.async_api import async_playwright  # noqa: E402

from scraper.browser import _SJS_BLOCK_RE, _extract_ssr_feed_posts  # noqa: E402
from scraper.constants import BASE_DIR  # noqa: E402

WALL_MARKERS = ("login", "captcha", "challenge", "checkpoint", "verify")

TIKTOK_DATA_RE = re.compile(
    r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>', re.S
)


def summarize_tiktok(html: str) -> None:
    """Print what the TikTok page embeds (scopes, status, video/comment counts)."""
    match = TIKTOK_DATA_RE.search(html)
    if not match:
        print("TikTok rehydration data: NOT present (blocked, captcha or different page)")
        return
    try:
        data = json.loads(match.group(1))
    except ValueError:
        print("TikTok rehydration data: present but not valid JSON")
        return
    scope = data.get("__DEFAULT_SCOPE__", {})
    print("TikTok scopes:", sorted(scope.keys()))
    for name in ("webapp.user-detail", "webapp.video-detail"):
        body = scope.get(name)
        if isinstance(body, dict):
            print(f"  {name}: statusCode={body.get('statusCode')} statusMsg={body.get('statusMsg')!r}")
    info = (scope.get("webapp.user-detail") or {}).get("userInfo") or {}
    user, stats = info.get("user") or {}, info.get("stats") or {}
    if user:
        print(
            "  user:", user.get("uniqueId"), "| privateAccount:", user.get("privateAccount"),
            "| followers:", stats.get("followerCount"), "| videos:", stats.get("videoCount"),
        )
    item = ((scope.get("webapp.video-detail") or {}).get("itemInfo") or {}).get("itemStruct") or {}
    if item:
        print("  video:", item.get("id"), "| desc:", (item.get("desc") or "")[:60], "| stats:", item.get("stats"))


def proxy_from_url(raw: str) -> dict:
    parts = urlsplit(raw)
    if not parts.scheme or not parts.hostname or not parts.port:
        raise ValueError("PROBE_PROXY must look like scheme://[user:pass@]host:port")
    proxy = {"server": f"{parts.scheme}://{parts.hostname}:{parts.port}"}
    if parts.username:
        proxy["username"] = unquote(parts.username)
    if parts.password:
        proxy["password"] = unquote(parts.password)
    return proxy


async def main() -> None:
    if len(sys.argv) != 2:
        print("usage: anon_probe.py <url>", file=sys.stderr)
        sys.exit(1)
    url = sys.argv[1]
    raw_proxy = os.environ.get("PROBE_PROXY")
    if not raw_proxy:
        print("PROBE_PROXY env var is required", file=sys.stderr)
        sys.exit(1)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, proxy=proxy_from_url(raw_proxy))
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        api_calls: list[str] = []

        def on_response(response) -> None:
            if "/api/" in response.url:
                path = urlsplit(response.url).path
                api_calls.append(f"{response.status} {path}")

        page.on("response", on_response)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            await page.wait_for_timeout(5000)
            await page.mouse.wheel(0, 3000)  # trigger lazy lists once
            await page.wait_for_timeout(3000)
            html = await page.content()

            host = (urlsplit(url).hostname or "site").replace("www.", "").split(".")[0]
            out = BASE_DIR / "debug"
            out.mkdir(parents=True, exist_ok=True)
            (out / f"anon_{host}.html").write_text(html, encoding="utf-8")
            await page.screenshot(path=str(out / f"anon_{host}.png"), full_page=True)

            landed = page.url
            username = urlsplit(url).path.strip("/").split("/")[0].lstrip("@")
            print("requested:", url)
            print("landed on:", landed)
            print("wall markers in URL:", [m for m in WALL_MARKERS if m in landed.lower()] or "none")
            print("html size:", len(html), "| sjs json blocks:", len(_SJS_BLOCK_RE.findall(html)))
            print("threads posts found in HTML:", len(_extract_ssr_feed_posts(html, username)))
            summarize_tiktok(html)
            if "tiktok" in url:
                links = await page.eval_on_selector_all(
                    'a[href*="/video/"]', "els => [...new Set(els.map(e => e.href))]"
                )
                print("video links on page:", len(links))
                for link in links[:5]:
                    print("  ", link)
            counts: dict[str, int] = {}
            for call in api_calls:
                counts[call] = counts.get(call, 0) + 1
            print("API calls:", dict(sorted(counts.items())) or "none")
            print(f"saved: debug/anon_{host}.html, debug/anon_{host}.png")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
