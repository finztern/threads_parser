"""TikTok research probe, no accounts: what is reachable by plain HTTP and
what only by a browser, through ONE proxy.

Usage (inside the parser image):
    python3 scripts/tiktok_probe.py <username> [video_url]

Env: PROBE_PROXY (http://user:pass@host:port) - keep it in .env only.

Part A  (browser) open the profile, save the bodies of /api/post/item_list/
        and open one video to save /api/comment/list/ bodies.
Part B  (HTTP only, curl_cffi with Chrome TLS fingerprint, NO browser)
        profile page, video page, unsigned item_list, oEmbed.
Raw bodies go to debug/tt_*.json|html (gitignored).
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from curl_cffi import requests as cffi  # noqa: E402
from playwright.async_api import async_playwright  # noqa: E402

from scraper.constants import BASE_DIR  # noqa: E402

OUT = BASE_DIR / "debug"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DATA_RE = re.compile(r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>', re.S)


def proxy_parts(raw: str) -> dict:
    p = urlsplit(raw)
    proxy = {"server": f"{p.scheme}://{p.hostname}:{p.port}"}
    if p.username:
        proxy["username"] = unquote(p.username)
    if p.password:
        proxy["password"] = unquote(p.password)
    return proxy


def head(text: str, n: int = 300) -> str:
    return text[:n].replace("\n", " ")


def scope_of(html: str) -> dict:
    m = DATA_RE.search(html)
    if not m:
        return {}
    try:
        return json.loads(m.group(1)).get("__DEFAULT_SCOPE__", {})
    except ValueError:
        return {}


def find_ids(obj, out: set[str]) -> None:
    """Collect video ids from an item_list style payload."""
    if isinstance(obj, dict):
        if isinstance(obj.get("itemList"), list):
            for it in obj["itemList"]:
                if isinstance(it, dict) and it.get("id"):
                    out.add(str(it["id"]))
        for v in obj.values():
            find_ids(v, out)
    elif isinstance(obj, list):
        for v in obj:
            find_ids(v, out)


async def part_a_browser(raw_proxy: str, username: str, video_url: str | None) -> tuple[set[str], str | None]:
    print("\n=== PART A: browser ===")
    ids: set[str] = set()
    found_video_url = video_url
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, proxy=proxy_parts(raw_proxy))
        context = await browser.new_context(user_agent=UA)
        page = await context.new_page()
        captured: list = []
        page.on("response", lambda r: captured.append(r) if ("/api/post/item_list" in r.url or "/api/comment/list" in r.url) else None)

        async def dump(prefix: str) -> None:
            for i, resp in enumerate(list(captured)):
                kind = "comments" if "comment" in resp.url else "items"
                try:
                    body = (await resp.body()).decode("utf-8", "replace")
                except Exception as exc:  # body may be evicted
                    print(f"  {kind} #{i}: status={resp.status} body unavailable ({exc})")
                    continue
                (OUT / f"tt_{prefix}_{kind}_{i}.json").write_text(body, encoding="utf-8")
                info = ""
                try:
                    data = json.loads(body)
                    n = len(data.get("itemList", data.get("comments") or []) or [])
                    info = f"status_code={data.get('statusCode', data.get('status_code'))} count={n} hasMore={data.get('hasMore', data.get('has_more'))}"
                    if kind == "items":
                        find_ids(data, ids)
                except ValueError:
                    info = "not JSON"
                print(f"  {kind} #{i}: http={resp.status} len={len(body)} {info} | {head(body, 160)!r}")
            captured.clear()

        await page.goto(f"https://www.tiktok.com/@{username}", wait_until="domcontentloaded", timeout=45_000)
        await page.wait_for_timeout(6000)
        await page.mouse.wheel(0, 2500)
        await page.wait_for_timeout(3000)
        print("profile page item_list responses:", sum(1 for r in captured if "item_list" in r.url))
        await dump("profile")

        links = await page.eval_on_selector_all('a[href*="/video/"]', "els => [...new Set(els.map(e => e.href))]")
        print("video links in DOM:", len(links))
        if not found_video_url and links:
            found_video_url = links[0]
        if not found_video_url and ids:
            found_video_url = f"https://www.tiktok.com/@{username}/video/{sorted(ids)[0]}"

        if found_video_url:
            print("opening video:", found_video_url)
            await page.goto(found_video_url, wait_until="domcontentloaded", timeout=45_000)
            await page.wait_for_timeout(7000)
            await page.mouse.wheel(0, 1500)
            await page.wait_for_timeout(3000)
            await dump("video")
        else:
            print("no video url available (item_list empty) - pass one as 2nd argument")
        await browser.close()
    return ids, found_video_url


def part_b_http(raw_proxy: str, username: str, video_url: str | None, ids: set[str]) -> None:
    print("\n=== PART B: plain HTTP, no browser (curl_cffi, chrome fingerprint) ===")
    s = cffi.Session(impersonate="chrome124", proxies={"http": raw_proxy, "https": raw_proxy}, timeout=30)
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

    r = s.get(f"https://www.tiktok.com/@{username}")
    sc = scope_of(r.text)
    ud = sc.get("webapp.user-detail", {})
    print(f"profile: http={r.status_code} len={len(r.text)} scopes={len(sc)} user-detail.statusCode={ud.get('statusCode')}"
          f" user={((ud.get('userInfo') or {}).get('user') or {}).get('uniqueId')}")
    (OUT / "tt_http_profile.html").write_text(r.text, encoding="utf-8")
    secuid = ((ud.get("userInfo") or {}).get("user") or {}).get("secUid")

    if video_url:
        r = s.get(video_url)
        sc = scope_of(r.text)
        vd = sc.get("webapp.video-detail", {})
        item = (vd.get("itemInfo") or {}).get("itemStruct") or {}
        print(f"video page: http={r.status_code} len={len(r.text)} video-detail.statusCode={vd.get('statusCode')} "
              f"id={item.get('id')} desc={(item.get('desc') or '')[:70]!r} stats={item.get('stats')} "
              f"createTime={item.get('createTime')}")
        (OUT / "tt_http_video.html").write_text(r.text, encoding="utf-8")

        o = s.get(f"https://www.tiktok.com/oembed?url={quote(video_url, safe='')}")
        print(f"oEmbed: http={o.status_code} {head(o.text, 200)!r}")

    if secuid:
        api = (
            "https://www.tiktok.com/api/post/item_list/?aid=1988&app_language=en&app_name=tiktok_web"
            f"&count=30&cursor=0&device_platform=web_pc&secUid={quote(secuid)}"
        )
        a = s.get(api, headers={"Referer": f"https://www.tiktok.com/@{username}"})
        print(f"unsigned item_list: http={a.status_code} len={len(a.text)} {head(a.text, 200)!r}")
    else:
        print("no secUid - skipping unsigned item_list")


async def main() -> None:
    if len(sys.argv) not in (2, 3):
        print("usage: tiktok_probe.py <username> [video_url]", file=sys.stderr)
        sys.exit(1)
    raw_proxy = os.environ.get("PROBE_PROXY")
    if not raw_proxy:
        print("PROBE_PROXY env var is required", file=sys.stderr)
        sys.exit(1)
    username = sys.argv[1].lstrip("@")
    video_url = sys.argv[2] if len(sys.argv) == 3 else None
    OUT.mkdir(parents=True, exist_ok=True)

    ids, found_video = await part_a_browser(raw_proxy, username, video_url)
    print("video ids seen via item_list:", sorted(ids)[:5] or "none")
    await asyncio.to_thread(part_b_http, raw_proxy, username, found_video, ids)


if __name__ == "__main__":
    asyncio.run(main())
