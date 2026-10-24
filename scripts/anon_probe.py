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
    for name, body in scope.items():
        if isinstance(body, dict):
            status = body.get("statusCode", body.get("status_code"))
            print(f"  {name}: statusCode={status} keys={sorted(body.keys())[:12]}")


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
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            await page.wait_for_timeout(4000)
            html = await page.content()

            out = BASE_DIR / "debug"
            out.mkdir(parents=True, exist_ok=True)
            (out / "anon_dump.html").write_text(html, encoding="utf-8")
            await page.screenshot(path=str(out / "anon_dump.png"), full_page=True)

            landed = page.url
            username = urlsplit(url).path.strip("/").split("/")[0].lstrip("@")
            print("requested:", url)
            print("landed on:", landed)
            print("wall markers in URL:", [m for m in WALL_MARKERS if m in landed.lower()] or "none")
            print("html size:", len(html), "| sjs json blocks:", len(_SJS_BLOCK_RE.findall(html)))
            print("threads posts found in HTML:", len(_extract_ssr_feed_posts(html, username)))
            summarize_tiktok(html)
            print("saved: debug/anon_dump.html, debug/anon_dump.png")
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
