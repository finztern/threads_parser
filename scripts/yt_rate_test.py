"""YouTube comments throughput test (not a parser): how many requests/comments per second can the free vless nodes sustain?

Flow per video (one curl_cffi session per video = one TCP/TLS connection, Chrome impersonation):
  1. GET watch page -> InnerTube key, client version and the first comments continuation token
  2. POST youtubei/v1/next with the token, then follow the next continuation tokens (pagination) a few times
Comments are counted by commentId. Run through scripts/yt_rate_test.sh (it starts the nodes).
Bootstrap modes: "page" (watch page, ~2.5 MB) or "next" (POST youtubei/v1/next with videoId, much lighter). Both also collect the
ids of related videos, so the video list grows by itself (snowball) and comments stop repeating.
Usage: python3 scripts/yt_rate_test.py PORT_FIRST PORT_COUNT [seconds] [concurrency] [pages_per_video] [page|next] [top|new]
"""
from __future__ import annotations

import asyncio
import json
import random
import re
import sys
import time
from collections import Counter

from curl_cffi.requests import AsyncSession

CHANNELS = [  # popular channels, only used to collect fresh video ids from their public RSS feeds
    "UCX6OQ3DkcsbYNE6H8uQQuVA", "UCq-Fj5jknLsUf-MWSy4_brA", "UC-lHJZR3Gqxm24_Vd_AJ5Yw", "UCbCmjCuTUZos6Inko4u57UQ",
    "UCJ5v_MCY6GNUBTO8-D3XoAg", "UCFFbwnve3yF62-tVXkTyHqg", "UCk8GzjMOrta8yxDcKfylJYw", "UCvlE5gTbOvjiolFlEm-c_Ow",
    "UCJplp5SjeGSdVdwsfb9Q7lQ",
]
FALLBACK_VIDEOS = ["dQw4w9WgXcQ", "9bZkp7q19f0", "kJQP7kiw5Fk", "JGwWNGJdvx8", "RgKAFK5djSk"]
HEADERS = {"Accept-Language": "en-US,en;q=0.9", "Cookie": "SOCS=CAI; CONSENT=PENDING+987"}

KEY_RE = re.compile(r'"INNERTUBE_API_KEY":"([^"]+)"')
VER_RE = re.compile(r'"INNERTUBE_CONTEXT_CLIENT_VERSION":"([^"]+)"')
VID_RE = re.compile(r'"videoId":"([A-Za-z0-9_-]{11})"')
STATUS_RE = re.compile(r'"playabilityStatus":\{"status":"([A-Z_]+)"(?:,"reason":"([^"]*)")?')
WEB_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"  # public key of the web client (same for everybody)
WEB_VERSION = "2.20241120.01.00"
AGE_RE = re.compile(r'"publishedTime":"([^"]+)"')
NEWEST_RE = re.compile(r'Newest first.{0,1500}?"token":"([^"]+)"', re.S)
SECTION_RE = re.compile(r'"sectionIdentifier":"([^"]+)"')


def snippet(text: str, needle: str, before: int = 120, after: int = 160) -> str:
    i = text.find(needle)
    return text[max(0, i - before):i + after].replace("\n", " ") if i >= 0 else "(not found)"
TOKEN_RE = re.compile(r'"token":"([^"]+)"')
NEXT_RE = re.compile(r'"continuationCommand":\{"token":"([^"]+)"')
CID_RE = re.compile(r'"commentId":"([^"]+)"')
TEXT_RE = re.compile(r'"commentId":"[^"]+".{0,1500}?"content":\{"content":"((?:[^"\\]|\\.)*)"', re.S)


def first_token(page: str) -> str | None:
    """Comments continuation: the token sits in the itemSectionRenderer just before sectionIdentifier=comment-item-section."""
    idx = page.find('"sectionIdentifier":"comment-item-section"')
    if idx < 0:
        idx = page.find('"targetId":"comments-section"')
    if idx < 0:
        return None
    tokens = TOKEN_RE.findall(page[max(0, idx - 4000):idx])
    return tokens[-1] if tokens else None


class Stats:
    def __init__(self) -> None:
        self.c: Counter[str] = Counter()
        self.comment_ids: set[str] = set()
        self.samples: list[str] = []
        self.lat: dict[str, list[float]] = {"page": [], "next": []}

    def window(self) -> tuple[int, int, int, int]:
        return self.c["page_ok"] + self.c["next_ok"], len(self.comment_ids), self.c["429"], self.c["err"] + self.c["timeout"]


async def main() -> None:
    first, count = int(sys.argv[1]), int(sys.argv[2])
    seconds = float(sys.argv[3]) if len(sys.argv) > 3 else 90
    conc = int(sys.argv[4]) if len(sys.argv) > 4 else 80
    pages = int(sys.argv[5]) if len(sys.argv) > 5 else 6
    boot = sys.argv[6] if len(sys.argv) > 6 else "next"
    sort = sys.argv[7] if len(sys.argv) > 7 else "new"
    ports = list(range(first, first + count))
    banned: dict[int, float] = {}
    st = Stats()

    def pick() -> int:
        now = time.monotonic()
        live = [p for p in ports if banned.get(p, 0) < now]
        return random.choice(live or ports)

    def proxies(port: int) -> dict:
        return {"http": f"socks5h://127.0.0.1:{port}", "https": f"socks5h://127.0.0.1:{port}"}

    # video ids from the public RSS feeds of busy channels
    videos: list[str] = []

    async def rss(ch: str) -> None:
        try:
            async with AsyncSession(impersonate="chrome", timeout=15, proxies=proxies(pick())) as s:
                r = await s.get(f"https://www.youtube.com/feeds/videos.xml?channel_id={ch}", headers=HEADERS)
            if r.status_code == 200:
                videos.extend(re.findall(r"<yt:videoId>([^<]+)</yt:videoId>", r.text))
        except Exception:  # noqa: BLE001
            pass

    await asyncio.gather(*(rss(c) for c in CHANNELS))
    print(f"video ids from RSS: {len(videos)} ({'fallback list added' if len(videos) < 20 else 'ok'})")
    videos[:] = list(dict.fromkeys(videos + FALLBACK_VIDEOS))
    known = set(videos)

    deadline = time.monotonic() + seconds
    sem = asyncio.Semaphore(conc)

    def fail(kind: str, exc_or_code, port: int, ban: float = 30.0) -> None:
        text = str(exc_or_code).lower()
        st.c["timeout" if "timed out" in text or "timeout" in text else "err"] += 1
        st.c[f"fail_{kind}_{str(exc_or_code)[:40]}"] += 1
        banned[port] = time.monotonic() + ban

    async def one_video(vid: str) -> None:
        port = pick()
        async with sem:
            if time.monotonic() > deadline:
                return
            async with AsyncSession(impersonate="chrome", timeout=20, proxies=proxies(port)) as s:
                t0 = time.monotonic()
                try:
                    if boot == "next":
                        r = await s.post(
                            f"https://www.youtube.com/youtubei/v1/next?key={WEB_KEY}&prettyPrint=false",
                            json={"context": {"client": {"clientName": "WEB", "clientVersion": WEB_VERSION, "hl": "en", "gl": "US"}},
                                  "videoId": vid},
                            headers={**HEADERS, "Origin": "https://www.youtube.com", "X-Youtube-Client-Name": "1",
                                     "X-Youtube-Client-Version": WEB_VERSION},
                        )
                    else:
                        r = await s.get(f"https://www.youtube.com/watch?v={vid}", headers=HEADERS)
                except Exception as exc:  # noqa: BLE001
                    fail("page", exc, port)
                    return
                st.lat["page"].append(time.monotonic() - t0)
                if r.status_code == 429:
                    st.c["429"] += 1
                    banned[port] = time.monotonic() + 120
                    return
                if r.status_code != 200:
                    st.c[f"page_http_{r.status_code}"] += 1
                    banned[port] = time.monotonic() + 30
                    return
                body = r.text
                st.c["page_bytes"] += len(r.content)
                if "consent.youtube.com" in str(r.url):
                    st.c["page_consent_wall"] += 1
                    return
                for found in VID_RE.findall(body):  # related videos: the list grows by itself
                    if found not in known and len(videos) < 50000:
                        known.add(found)
                        videos.append(found)
                if boot == "next":
                    key, ver, token = WEB_KEY, WEB_VERSION, first_token(body)
                else:
                    km, vm = KEY_RE.search(body), VER_RE.search(body)
                    key, ver, token = (km.group(1) if km else None), (vm.group(1) if vm else None), first_token(body)
                if not (key and ver and token):
                    st.c["page_no_token"] += 1
                    m = STATUS_RE.search(body)
                    why = f"{m.group(1)}:{(m.group(2) or '')[:50]}" if m else ("bot_check" if "not a bot" in body else "no_status")
                    st.c[f"notoken_{why}"] += 1  # comments off / live / age or bot wall: the reason
                    if st.c["debug_notoken"] < 2:
                        st.c["debug_notoken"] += 1
                        print(f"DEBUG no-token vid={vid} http={r.status_code} len={len(body)} sections={sorted(set(SECTION_RE.findall(body)))}"
                              f"\n  head: {body[:160]!r}\n  turned-off context: {snippet(body, 'Comments are turned off')!r}", flush=True)
                    for marker in ("comments-section", "commentsEntryPointHeaderRenderer", "Comments are turned off", "isLiveContent",
                                   "liveChat", "Sign in to confirm", "engagement-panel-comments-section"):
                        if marker in body:
                            st.c[f"marker_{marker}"] += 1
                    return
                st.c["page_ok"] += 1
                first_step = True
                for _ in range(pages):
                    if not token or time.monotonic() > deadline:
                        break
                    payload = {"context": {"client": {"clientName": "WEB", "clientVersion": ver, "hl": "en", "gl": "US"}},
                               "continuation": token}
                    t1 = time.monotonic()
                    try:
                        r = await s.post(
                            f"https://www.youtube.com/youtubei/v1/next?key={key}&prettyPrint=false",
                            json=payload,
                            headers={**HEADERS, "Origin": "https://www.youtube.com", "X-Youtube-Client-Name": "1",
                                     "X-Youtube-Client-Version": ver},
                        )
                    except Exception as exc:  # noqa: BLE001
                        fail("next", exc, port)
                        return
                    st.lat["next"].append(time.monotonic() - t1)
                    if r.status_code == 429:
                        st.c["429"] += 1
                        banned[port] = time.monotonic() + 120
                        return
                    if r.status_code != 200:
                        st.c[f"next_http_{r.status_code}"] += 1
                        return
                    text = r.text
                    st.c["next_bytes"] += len(r.content)
                    if sort == "new" and first_step:
                        first_step = False
                        newest = NEWEST_RE.search(text)
                        st.c["sort_probe"] += 1
                        if newest:
                            token = newest.group(1)
                            st.c["newest_token_found"] += 1
                            continue  # the first batch is "top comments": skip it, ask again sorted by newest
                        st.c["newest_token_missing"] += 1
                        if st.c["newest_token_missing"] <= 2:
                            print(f"DEBUG no newest token vid={vid} len={len(text)} Newest: {snippet(text, 'Newest')!r}"
                                  f"\n  sortMenu: {snippet(text, 'sortMenu', 20, 300)!r}", flush=True)
                    first_step = False
                    ids = CID_RE.findall(text)
                    if not ids:
                        st.c["next_no_comments"] += 1
                        return
                    st.c["next_ok"] += 1
                    st.c["raw_comments"] += len(ids)
                    new = [i for i in ids if i not in st.comment_ids]
                    st.comment_ids.update(new)
                    for age in AGE_RE.findall(text):
                        unit = re.search(r"(second|minute|hour|day|week|month|year)", age)
                        st.c[f"age_{unit.group(1) if unit else 'other'}"] += 1
                    if len(st.samples) < 4:
                        m = TEXT_RE.search(text)
                        if m:
                            st.samples.append(m.group(1)[:110])
                    nxt = NEXT_RE.findall(text)
                    token = nxt[-1] if nxt else None

    async def monitor() -> None:
        last = st.window()
        t_last = time.monotonic()
        while time.monotonic() < deadline:
            await asyncio.sleep(10)
            now, cur = time.monotonic(), st.window()
            dt = now - t_last
            print(f"  {now - (deadline - seconds):4.0f}s | req/s {(cur[0] - last[0]) / dt:5.1f} | unique comments/s {(cur[1] - last[1]) / dt:6.1f}"
                  f" | 429 {cur[2] - last[2]} | fail {cur[3] - last[3]}", flush=True)
            last, t_last = cur, now

    async def feeder() -> None:
        tasks: set[asyncio.Task] = set()
        i = 0
        while time.monotonic() < deadline:
            if len(tasks) < conc * 2:
                t = asyncio.create_task(one_video(videos[i % len(videos)] if i < len(videos) else random.choice(videos)))
                tasks.add(t)
                t.add_done_callback(tasks.discard)
                i += 1
            await asyncio.sleep(0.02)
        await asyncio.gather(*tasks, return_exceptions=True)

    started = time.monotonic()
    await asyncio.gather(feeder(), monitor())
    span = time.monotonic() - started
    reqs, uniq = st.c["page_ok"] + st.c["next_ok"], len(st.comment_ids)

    def avg(v: list[float]) -> str:
        return f"{sum(v) / len(v):.1f}s" if v else "-"

    print("\n================ YOUTUBE RESULT ================")
    print(f"nodes: {count} | concurrency: {conc} | pages per video: {pages} | duration: {span:.0f}s | videos known: {len(videos)}")
    print(f"requests OK: {reqs} (watch pages {st.c['page_ok']}, comment batches {st.c['next_ok']}) = {reqs / span:.1f} req/s")
    print(f"bootstrap mode: {boot} | videos known at the end: {len(videos)}")
    print(f"unique comments: {uniq} = {uniq / span:.1f}/s | raw comments per batch: {st.c['raw_comments'] / max(1, st.c['next_ok']):.1f}")
    print(f"latency avg: page {avg(st.lat['page'])}, next {avg(st.lat['next'])}")
    mb = (st.c["page_bytes"] + st.c["next_bytes"]) / 1e6
    print(f"traffic (decoded): {mb:.0f} MB = {mb / span:.1f} MB/s | page avg {st.c['page_bytes'] / max(1, st.c['page_ok']) / 1024:.0f} KB,"
          f" next avg {st.c['next_bytes'] / max(1, st.c['next_ok']) / 1024:.0f} KB")
    print(f"429: {st.c['429']} | timeouts: {st.c['timeout']} | errors: {st.c['err']} | consent wall: {st.c['page_consent_wall']}"
          f" | page without token: {st.c['page_no_token']} | next without comments: {st.c['next_no_comments']}")
    ages = {k[4:]: v for k, v in st.c.items() if k.startswith("age_")}
    if ages:
        total_age = sum(ages.values())
        fresh = ages.get("second", 0) + ages.get("minute", 0)
        print(f"comment age (relative text of YouTube): {json.dumps(ages)} | under 1 hour: {100 * fresh / total_age:.0f}% = about {fresh / span:.1f}/s")
    print(f"sort newest: token found {st.c['newest_token_found']}, missing {st.c['newest_token_missing']}")
    markers = {k[7:]: v for k, v in st.c.items() if k.startswith("marker_")}
    if markers:
        print("pages without comments token contain:", json.dumps(markers))
    other = {k: v for k, v in st.c.items() if k.startswith(("fail_", "page_http_", "next_http_", "notoken_"))}
    if other:
        print("failures:", json.dumps(dict(sorted(other.items(), key=lambda kv: -kv[1])[:9]), ensure_ascii=False))
    if st.samples:
        print("sample comments:")
        for text in st.samples:
            print("  ", text)


if __name__ == "__main__":
    asyncio.run(main())
