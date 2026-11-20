"""YouTube "hot videos" throughput test (not a parser): how many FRESH comments per second can we really collect, and how many of
them does Exorde not have yet (official url-check dedup)?

What it does:
  * explores videos (seeds from RSS + the related-videos snowball), opens each through youtubei/v1/next, switches the comments to
    "Newest" and remembers the newest-first continuation token of the video;
  * videos that gave fresh comments become "hot": they are polled again every HOT_INTERVAL seconds with the cached token
    (one small request, no page download);
  * a comment counts when it is text, >= 7 chars and not older than 1 day ("1 day ago" is the last accepted bucket; YouTube only gives
    relative ages, so the real age error is up to one unit);
  * fresh comments are checked against the Exorde url-check API from this host (no proxy), URL format
    https://www.youtube.com/watch?v=<video>&lc=<comment id> (the permalink). "new to Exorde" = not reported as known.
Usage: python3 scripts/yt_hot_test.py PORT_FIRST PORT_COUNT [seconds=240] [workers=80]
Env: URL_CHECK_PROXY=<socks5h://...>  HOT_INTERVAL=30  HOT_MIN=1.0  EXPLORE_SHARE=0.7  URL_CHECK=<endpoint or "off">  MAX_PAGES=3
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import re
import sys
import time
from collections import Counter, deque

from curl_cffi.requests import AsyncSession

CHANNELS = [  # public RSS feeds only seed the video list; no kids channels (comments are off there)
    "UCX6OQ3DkcsbYNE6H8uQQuVA", "UCq-Fj5jknLsUf-MWSy4_brA", "UC-lHJZR3Gqxm24_Vd_AJ5Yw",
    "UCJ5v_MCY6GNUBTO8-D3XoAg", "UCFFbwnve3yF62-tVXkTyHqg",
]
FALLBACK = ["dQw4w9WgXcQ", "9bZkp7q19f0", "kJQP7kiw5Fk", "JGwWNGJdvx8", "RgKAFK5djSk"]
HEADERS = {"Accept-Language": "en-US,en;q=0.9", "Cookie": "SOCS=CAI; CONSENT=PENDING+987"}
WEB_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"  # public key of the web client
WEB_VERSION = "2.20241120.01.00"
NEXT_URL = f"https://www.youtube.com/youtubei/v1/next?key={WEB_KEY}&prettyPrint=false"
POST_HEADERS = {**HEADERS, "Origin": "https://www.youtube.com", "X-Youtube-Client-Name": "1", "X-Youtube-Client-Version": WEB_VERSION}
CLIENT = {"clientName": "WEB", "clientVersion": WEB_VERSION, "hl": "en", "gl": "US"}

VID_RE = re.compile(r'"videoId":"([A-Za-z0-9_-]{11})"')
TOKEN_RE = re.compile(r'"token":"([^"]+)"')
NEXT_RE = re.compile(r'"continuationCommand":\{"token":"([^"]+)"')
NEWEST_RE = re.compile(r'"title":"Newest(?: first)?".{0,1500}?"continuationCommand":\{"token":"([^"]+)"', re.S)
CID_RE = re.compile(r'"commentId":"([^"]+)"')
AGE_RE = re.compile(r'"publishedTime":"([^"]+)"')
TEXT_RE = re.compile(r'"content":\{"content":"((?:[^"\\]|\\.)*)"')
VIEWS_RE = re.compile(r'"originalViewCount":"(\d+)"')
BUCKETS = [(1_000, "<1k"), (10_000, "1k-10k"), (100_000, "10k-100k"), (1_000_000, "100k-1M"), (10**18, ">1M")]
VAGE_RE = re.compile(r'"relativeDateText":\{"simpleText":"([^"]+)"')


def script_of(text: str) -> str:
    """Rough writing system of a comment: Exorde dedup depends on who else collects which language."""
    for ch in text:
        o = ord(ch)
        if ch.isalpha():
            if o < 0x250:
                return "latin"
            if 0x400 <= o < 0x530:
                return "cyrillic"
            if 0x600 <= o < 0x780:
                return "arabic"
            if 0x900 <= o < 0xE00:
                return "indic"
            if 0xE00 <= o < 0xE80:
                return "thai"
            if 0x3040 <= o < 0xA000 or 0xAC00 <= o < 0xD7B0:
                return "cjk_kana_hangul"
            return "other"
    return "no_letters"


def video_age_bucket(text: str) -> str:
    m = AGE_PARSE.search(text)
    if not m:
        return "?"
    n, unit = int(m.group(1)), m.group(2)
    if unit in ("second", "minute", "hour"):
        return "video <1d"
    return "video 1-7d" if unit == "day" and n < 7 else "video older"


AGE_PARSE = re.compile(r"(\d+)\s+(second|minute|hour|day|week|month|year)")
MIN_TEXT = 7


def first_token(text: str) -> str | None:
    idx = text.find('"sectionIdentifier":"comment-item-section"')
    if idx < 0:
        return None
    tokens = TOKEN_RE.findall(text[max(0, idx - 4000):idx])
    return tokens[-1] if tokens else None


def parse_comments(text: str) -> dict[str, tuple[str, str]]:
    """comment id -> (relative age, text). One comment id appears several times in a response: merge the occurrences."""
    found: dict[str, list[str]] = {}
    spots = [(m.start(), m.group(1)) for m in CID_RE.finditer(text)]
    for i, (pos, cid) in enumerate(spots):
        end = min(spots[i + 1][0] if i + 1 < len(spots) else pos + 4000, pos + 4000)
        window = text[pos:end]
        entry = found.setdefault(cid, ["", ""])
        if not entry[0] and (m := AGE_RE.search(window)):
            entry[0] = m.group(1)
        if not entry[1] and (m := TEXT_RE.search(window)):
            entry[1] = m.group(1)
    return {cid: (a, t) for cid, (a, t) in found.items() if a and t}


def is_fresh(age: str) -> bool:
    m = AGE_PARSE.search(age)
    if not m:
        return False
    n, unit = int(m.group(1)), m.group(2)
    return unit in ("second", "minute", "hour") or (unit == "day" and n <= 1)


class Video:
    __slots__ = ("vid", "token", "newest", "last_poll", "ema", "polls", "dead", "bucket", "vage")

    def __init__(self, vid: str) -> None:
        self.vid, self.token, self.newest = vid, None, False
        self.last_poll, self.ema, self.polls, self.dead, self.bucket, self.vage = 0.0, 0.0, 0, False, "?", "?"


async def main() -> None:
    first, count = int(sys.argv[1]), int(sys.argv[2])
    seconds = float(sys.argv[3]) if len(sys.argv) > 3 else 240
    workers = int(sys.argv[4]) if len(sys.argv) > 4 else 80
    hot_interval = float(os.environ.get("HOT_INTERVAL", "30"))
    explore_share = float(os.environ.get("EXPLORE_SHARE", "0.7"))
    max_pages = int(os.environ.get("MAX_PAGES", "3"))
    hot_min = float(os.environ.get("HOT_MIN", "1.0"))  # a video stays hot while it gives >= this many fresh comments per poll
    check_endpoint = os.environ.get("URL_CHECK", "https://url-check.exorde.network/v1/check")
    ports = list(range(first, first + count))
    banned: dict[int, float] = {}
    c: Counter[str] = Counter()
    seen: set[str] = set()
    samples: list[str] = []
    videos: dict[str, Video] = {}
    explore: deque[str] = deque()
    url_queue: list[tuple[str, tuple[str, str, str]]] = []  # (url, (views bucket, video age bucket, script))
    bucket_stat: dict[str, dict[str, Counter]] = {"views": {}, "video age": {}, "script": {}}

    def add_video(vid: str) -> None:
        if vid not in videos and len(videos) < 100_000:
            videos[vid] = Video(vid)
            explore.append(vid)

    def pick_port() -> int:
        now = time.monotonic()
        live = [p for p in ports if banned.get(p, 0) < now]
        return random.choice(live or ports)

    def proxies(port: int) -> dict:
        return {"http": f"socks5h://127.0.0.1:{port}", "https": f"socks5h://127.0.0.1:{port}"}

    async def seed(ch: str) -> None:
        try:
            async with AsyncSession(impersonate="chrome", timeout=15, proxies=proxies(pick_port())) as s:
                r = await s.get(f"https://www.youtube.com/feeds/videos.xml?channel_id={ch}", headers=HEADERS)
            if r.status_code == 200:
                for vid in re.findall(r"<yt:videoId>([^<]+)</yt:videoId>", r.text):
                    add_video(vid)
        except Exception:  # noqa: BLE001
            pass

    await asyncio.gather(*(seed(ch) for ch in CHANNELS))
    for vid in FALLBACK:
        add_video(vid)
    print(f"seed videos: {len(videos)}", flush=True)

    deadline = time.monotonic() + seconds
    t_start = time.monotonic()

    def pick_video() -> tuple[Video | None, str]:
        now = time.monotonic()
        if random.random() >= explore_share:
            best, best_ema = None, 0.0
            for v in list(videos.values()):
                if v.polls and not v.dead and v.ema >= hot_min and now - v.last_poll >= hot_interval and v.ema > best_ema:
                    best, best_ema = v, v.ema
            if best:
                return best, "hot"
        while explore:
            v = videos[explore.popleft()]
            if not v.polls:
                return v, "explore"
        pool = [v for v in videos.values() if not v.dead and now - v.last_poll >= hot_interval]
        return (random.choice(pool), "repoll") if pool else (None, "")

    async def post_next(session: AsyncSession, payload: dict) -> tuple[int, str, int]:
        r = await session.post(NEXT_URL, json={"context": {"client": CLIENT}, **payload}, headers=POST_HEADERS)
        return r.status_code, r.text, len(r.content)

    def take(text: str, vid: str, kind: str) -> tuple[int, int]:
        """Count the fresh, text, unseen comments of a response; queue their urls for the url check."""
        comments = parse_comments(text)
        fresh_new = 0
        for cid, (age, body) in comments.items():
            c["comments_received"] += 1
            if not is_fresh(age):
                c["old"] += 1
                continue
            if len(body.strip()) < MIN_TEXT:
                c["too_short"] += 1
                continue
            if cid in seen:
                c["repeat"] += 1
                continue
            seen.add(cid)
            fresh_new += 1
            c["fresh_new"] += 1
            c[f"fresh_from_{kind}"] += 1
            url_queue.append((f"https://www.youtube.com/watch?v={vid}&lc={cid}", (videos[vid].bucket, videos[vid].vage, script_of(body))))
            if len(samples) < 4:
                samples.append(f"[{age}] {body[:100]}")
        return len(comments), fresh_new

    async def poll(session: AsyncSession, v: Video, kind: str) -> None:
        v.last_poll = time.monotonic()
        if v.token is None:  # bootstrap: related videos + comments token, then the "Newest" token
            status, text, size = await post_next(session, {"videoId": v.vid})
            c["req_bootstrap"] += 1
            c["bytes_bootstrap"] += size
            if status != 200:
                raise RuntimeError(f"http {status}")
            for found in VID_RE.findall(text):
                add_video(found)
            if m := VIEWS_RE.search(text):
                views = int(m.group(1))
                v.bucket = next(name for limit, name in BUCKETS if views < limit)
            if m := VAGE_RE.search(text):
                v.vage = video_age_bucket(m.group(1))
            token = first_token(text)
            if not token:
                v.dead = True
                c["videos_without_comments"] += 1
                return
            status, text, size = await post_next(session, {"continuation": token})
            c["req_sort"] += 1
            c["bytes_sort"] += size
            newest = NEWEST_RE.search(text) if status == 200 else None
            v.token, v.newest = (newest.group(1), True) if newest else (token, False)
            c["newest_found" if newest else "newest_missing"] += 1
        token, reused = v.token, v.polls > 0
        fresh_total = 0
        for page in range(max_pages):
            status, text, size = await post_next(session, {"continuation": token})
            c["req_comments"] += 1
            c["bytes_comments"] += size
            if status != 200:
                v.token = None
                c["token_fail" if reused else "comments_http_fail"] += 1
                raise RuntimeError(f"http {status}")
            total, fresh_new = take(text, v.vid, kind)
            if page == 0:
                c["token_reuse_ok" if reused else "first_poll_ok"] += 1
                if total == 0:
                    v.token = None  # token went stale or no comments: bootstrap again next time
                    c["token_stale"] += 1
                    return
            fresh_total += fresh_new
            nxt = NEXT_RE.findall(text)
            if not nxt or total == 0 or fresh_new < 0.5 * total:
                break  # the rest of the list is older or already seen
            token = nxt[-1]
        v.polls += 1
        # the first poll returns the whole backlog of the last day: it must not make a video "hot"; the first re-poll is a trial
        v.ema = hot_min * 1.5 if v.polls == 1 else 0.5 * v.ema + 0.5 * fresh_total
        c["polls_ok"] += 1
        c[f"polls_{kind}"] += 1

    async def worker() -> None:
        port = pick_port()
        session = AsyncSession(impersonate="chrome", timeout=20, proxies=proxies(port))
        errors = 0
        try:
            while time.monotonic() < deadline:
                v, kind = pick_video()
                if v is None:
                    await asyncio.sleep(0.5)
                    continue
                try:
                    await poll(session, v, kind)
                    errors = 0
                except Exception as exc:  # noqa: BLE001
                    errors += 1
                    text = str(exc).lower()
                    c["timeout" if "timed out" in text or "(28)" in text else "err"] += 1
                    c[f"fail_{str(exc)[:45]}"] += 1
                    if errors >= 1:  # dead node: take another one with a fresh connection
                        banned[port] = time.monotonic() + 60
                        await session.close()
                        port = pick_port()
                        session = AsyncSession(impersonate="chrome", timeout=20, proxies=proxies(port))
                        errors = 0
        finally:
            await session.close()

    async def url_checker() -> None:
        if check_endpoint == "off":
            return
        uc_proxy = os.environ.get("URL_CHECK_PROXY", "")  # e.g. socks5h://127.0.0.1:10809 on the home PC (direct is blocked there)
        uc_proxies = {"http": uc_proxy, "https": uc_proxy} if uc_proxy else None
        async with AsyncSession(impersonate="chrome", timeout=15, proxies=uc_proxies) as s:
            while time.monotonic() < deadline + 5:
                await asyncio.sleep(1.0)
                while url_queue:
                    items, url_queue[:300] = url_queue[:300], []
                    batch = [u for u, _ in items]
                    bucket_of = dict(items)
                    try:
                        r = await s.post(check_endpoint, json={"urls": batch})
                        data = r.json() if r.status_code == 200 else None
                    except Exception:  # noqa: BLE001
                        data = None
                    if isinstance(data, list):
                        known = {u for u in data if isinstance(u, str)}
                        c["url_known"] += len(known)
                        c["url_new"] += len(batch) - len(known)
                        for u in batch:
                            for dim, label in zip(("views", "video age", "script"), bucket_of[u]):
                                bucket_stat[dim].setdefault(label, Counter())["known" if u in known else "new"] += 1
                    else:
                        c["url_unverified"] += len(batch)
                        c["url_check_fail"] += 1

    async def monitor() -> None:
        last = Counter(c)
        t_last = time.monotonic()
        while time.monotonic() < deadline:
            await asyncio.sleep(10)
            now = time.monotonic()
            dt = now - t_last
            hot = sum(1 for v in videos.values() if v.polls and not v.dead and v.ema >= hot_min)
            print(f"  {now - t_start:4.0f}s | polls/s {(c['polls_ok'] - last['polls_ok']) / dt:5.1f} | fresh/s {(c['fresh_new'] - last['fresh_new']) / dt:6.1f}"
                  f" | new to Exorde/s {(c['url_new'] - last['url_new']) / dt:6.1f} | hot {hot} | videos {len(videos)} | fail {c['err'] + c['timeout'] - last['err'] - last['timeout']}",
                  flush=True)
            last, t_last = Counter(c), now

    await asyncio.gather(monitor(), url_checker(), *(worker() for _ in range(workers)))
    span = time.monotonic() - t_start
    mb = (c["bytes_bootstrap"] + c["bytes_sort"] + c["bytes_comments"]) / 1e6
    print("\n================ YOUTUBE HOT RESULT ================")
    print(f"nodes: {count} | workers: {workers} | duration: {span:.0f}s | hot interval: {hot_interval:.0f}s | max pages: {max_pages}")
    print(f"videos known: {len(videos)} | without comments: {c['videos_without_comments']} | polls ok: {c['polls_ok']}"
          f" (hot {c['polls_hot']}, explore {c['polls_explore']}, repoll {c['polls_repoll']})")
    print(f"requests: bootstrap {c['req_bootstrap']}, sort {c['req_sort']}, comments {c['req_comments']} | newest token found {c['newest_found']}, missing {c['newest_missing']}")
    print(f"cached token: reused ok {c['token_reuse_ok']}, failed {c['token_fail']}, stale {c['token_stale']}")
    print(f"comments received {c['comments_received']}: old {c['old']}, too short {c['too_short']}, repeats {c['repeat']}, FRESH NEW {c['fresh_new']}"
          f" = {c['fresh_new'] / span:.1f}/s (hot {c['fresh_from_hot']}, explore {c['fresh_from_explore']}, repoll {c['fresh_from_repoll']})")
    if check_endpoint != "off":
        total = c["url_known"] + c["url_new"]
        print(f"url check: known to Exorde {c['url_known']}, NEW to Exorde {c['url_new']} = {c['url_new'] / span:.1f}/s"
              f" ({100 * c['url_new'] / max(1, total):.0f}% of checked) | unverified {c['url_unverified']} (check failures {c['url_check_fail']})")
    if check_endpoint != "off":
        print("new to Exorde by video views / video age / comment script (fresh comments checked | new | known share):")
        for dim, labels in bucket_stat.items():
            for label, st in sorted(labels.items(), key=lambda kv: -(kv[1]["known"] + kv[1]["new"])):
                tot = st["known"] + st["new"]
                if tot >= 30:
                    print(f"   {dim:>9} {label:>16}: {tot:6d} | {st['new']:6d} | known {100 * st['known'] / tot:3.0f}%")
    print(f"traffic (decoded): {mb:.0f} MB = {mb / span:.1f} MB/s | per comment request {c['bytes_comments'] / max(1, c['req_comments']) / 1024:.0f} KB")
    print(f"timeouts {c['timeout']}, errors {c['err']}")
    fails = {k[5:]: v for k, v in c.items() if k.startswith("fail_")}
    if fails:
        print("failures:", json.dumps(dict(sorted(fails.items(), key=lambda kv: -kv[1])[:6]), ensure_ascii=False))
    if samples:
        print("sample fresh comments:")
        for s in samples:
            print("  ", s)


if __name__ == "__main__":
    asyncio.run(main())
