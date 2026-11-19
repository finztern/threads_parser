"""Reddit throughput test (not a parser): the HTML listing https://www.reddit.com/r/<sub>/comments/ through the free vless nodes.

Each worker keeps one curl_cffi session (Chrome TLS impersonation) on one node and polls the latest-comments page of ~50 busy
subreddits round-robin (the least recently polled first). Comments are parsed from div.thing (t1_...): author, permalink, real <time>
datetime, text. Fresh unique comments are checked against the Exorde url-check API (direct from this host).
Usage: python3 scripts/reddit_html_test.py PORT_FIRST PORT_COUNT [seconds=180] [workers=40]
Direct mode (no nodes, e.g. on the home PC): DIRECT=1 RPS=2 python3 scripts/reddit_html_test.py 0 1 180 8
Env: URL_CHECK=<endpoint or "off">  VARIANTS="plain,limit100" (which URL forms are mixed)  DIRECT=1  RPS=<global requests/s limit, direct mode>
"""
from __future__ import annotations

import asyncio
import html as htmllib
import json
import os
import random
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from curl_cffi.requests import AsyncSession

SUBS = """AskReddit politics worldnews news funny gaming pics todayilearned movies memes interestingasfuck mildlyinteresting
Showerthoughts technology science aww music sports nba soccer nfl television Futurology space wallstreetbets CryptoCurrency Bitcoin
stocks investing personalfinance relationship_advice AmItheAsshole tifu unpopularopinion explainlikeimfive LifeProTips NoStupidQuestions
OutOfTheLoop pcmasterrace Fauxmoi Conservative PublicFreakout facepalm mildlyinfuriating AskMen AskWomen CasualConversation teenagers
food buildapc Android""".split()
HEADERS = {"Accept-Language": "en-US,en;q=0.9"}
VARIANT_URLS = {"plain": "https://www.reddit.com/r/{sub}/comments/", "limit100": "https://www.reddit.com/r/{sub}/comments/?limit=100"}
THING_RE = re.compile(r'<div[^>]*\bdata-fullname="(t1_[a-z0-9]+)"[^>]*>', re.I)
ATTR = {k: re.compile(rf'\bdata-{k}="([^"]*)"') for k in ("author", "permalink", "subreddit")}
TIME_RE = re.compile(r'<time[^>]*\bdatetime="([^"]+)"')
BODY_RE = re.compile(r'<div class="md">(.*?)</div>', re.S)
HREF_RE = re.compile(r'href="(/r/[^"]+/comments/[^"]+)"')
TAG_RE = re.compile(r"<[^>]+>")
BLOCK_MARKERS = ("blocked by network security", "whoa there, pardner", "captcha", "Please wait for verification", "verify you are human")
MIN_TEXT = 7


def parse_things(text: str) -> list[dict]:
    spots = list(THING_RE.finditer(text))
    out = []
    for i, m in enumerate(spots):
        chunk = text[m.end(): spots[i + 1].start() if i + 1 < len(spots) else m.end() + 8000][:8000]
        tag = m.group(0)
        a = {k: (r.search(tag).group(1) if r.search(tag) else "") for k, r in ATTR.items()}
        tm, body, href = TIME_RE.search(chunk), BODY_RE.search(chunk), HREF_RE.search(chunk)
        permalink = a["permalink"] or (href.group(1) if href else "")
        out.append({
            "id": m.group(1), "author": a["author"], "subreddit": a["subreddit"], "permalink": permalink,
            "created": tm.group(1) if tm else "", "text": htmllib.unescape(TAG_RE.sub("", body.group(1))).strip() if body else "",
        })
    return out


def age_seconds(created: str) -> float | None:
    try:
        return (datetime.now(timezone.utc) - datetime.fromisoformat(created.replace("Z", "+00:00"))).total_seconds()
    except ValueError:
        return None


async def main() -> None:
    first, count = int(sys.argv[1]), int(sys.argv[2])
    seconds = float(sys.argv[3]) if len(sys.argv) > 3 else 180
    workers = int(sys.argv[4]) if len(sys.argv) > 4 else 40
    check_endpoint = os.environ.get("URL_CHECK", "https://url-check.exorde.network/v1/check")
    variants = [v for v in os.environ.get("VARIANTS", "plain,limit100").split(",") if v in VARIANT_URLS]
    direct = os.environ.get("DIRECT") == "1"
    rps = float(os.environ.get("RPS", "2"))
    slot = [0.0]  # global pacing in direct mode: one IP must not hammer Reddit
    ports = [0] if direct else list(range(first, first + count))
    banned: dict[int, float] = {}
    c: Counter[str] = Counter()
    seen: set[str] = set()
    last_poll = {s: 0.0 for s in SUBS}
    samples: list[str] = []
    url_queue: list[str] = []
    lat: dict[str, list[float]] = {v: [] for v in variants}
    deadline = time.monotonic() + seconds
    t_start = time.monotonic()

    def pick_port() -> int:
        now = time.monotonic()
        live = [p for p in ports if banned.get(p, 0) < now]
        return random.choice(live or ports)

    async def pace() -> None:
        if direct:
            now = time.monotonic()
            slot[0] = max(slot[0], now) + 1.0 / rps
            await asyncio.sleep(max(0.0, slot[0] - 1.0 / rps - now))

    def proxies(port: int) -> dict | None:
        if direct:
            return None
        return {"http": f"socks5h://127.0.0.1:{port}", "https": f"socks5h://127.0.0.1:{port}"}

    def next_sub() -> str:
        sub = min(SUBS, key=lambda s: last_poll[s])
        last_poll[sub] = time.monotonic()
        return sub

    async def worker() -> None:
        port = pick_port()
        session = AsyncSession(impersonate="chrome", timeout=20, proxies=proxies(port))
        try:
            while time.monotonic() < deadline:
                await pace()
                sub, variant = next_sub(), random.choice(variants)
                t0 = time.monotonic()
                try:
                    r = await session.get(VARIANT_URLS[variant].format(sub=sub), headers=HEADERS)
                except Exception as exc:  # noqa: BLE001
                    text = str(exc).lower()
                    c["timeout" if "timed out" in text or "(28)" in text else "err"] += 1
                    c[f"fail_{str(exc)[:45]}"] += 1
                    banned[port] = time.monotonic() + 60
                    await session.close()
                    port = pick_port()
                    session = AsyncSession(impersonate="chrome", timeout=20, proxies=proxies(port))
                    continue
                lat[variant].append(time.monotonic() - t0)
                c[f"req_{variant}"] += 1
                c["bytes"] += len(r.content)
                if r.status_code != 200:
                    c[f"http_{r.status_code}"] += 1
                    banned[port] = time.monotonic() + (120 if r.status_code in (403, 429) else 30)
                    if c[f"http_{r.status_code}"] <= 2:
                        print(f"DEBUG http {r.status_code} sub={sub} variant={variant}: {r.text[:200]!r}", flush=True)
                    await session.close()
                    port = pick_port()
                    session = AsyncSession(impersonate="chrome", timeout=20, proxies=proxies(port))
                    continue
                body = r.text
                things = parse_things(body)
                if not things:
                    why = "blocked_page" if any(mk.lower() in body.lower() for mk in BLOCK_MARKERS) else "no_comments_in_html"
                    c[why] += 1
                    c[f"{why}_{variant}"] += 1
                    if c["debug_empty"] < 2:
                        c["debug_empty"] += 1
                        title = re.search(r"<title>(.*?)</title>", body, re.S)
                        print(f"DEBUG no comments sub={sub} variant={variant} len={len(body)} title={title.group(1)[:80] if title else None!r}"
                              f" t1_ occurrences={body.count('t1_')}\n  head: {body[:200]!r}", flush=True)
                    continue
                c["ok"] += 1
                c[f"ok_{variant}"] += 1
                c[f"comments_{variant}"] += len(things)
                if c["debug_sample"] < 1:
                    c["debug_sample"] += 1
                    m = THING_RE.search(body)
                    print(f"DEBUG first comment tag: {m.group(0)[:500]!r}\n  parsed: {things[0]}", flush=True)
                for t in things:
                    if not t["created"]:
                        c["no_datetime"] += 1
                        continue
                    if t["id"] in seen:
                        c["repeat"] += 1
                        continue
                    seen.add(t["id"])
                    if len(t["text"]) < MIN_TEXT:
                        c["too_short"] += 1
                        continue
                    c["unique_text"] += 1
                    age = age_seconds(t["created"])
                    if age is not None:
                        c["age_<1min" if age < 60 else "age_<5min" if age < 300 else "age_<1h" if age < 3600 else "age_<1d" if age < 86400 else "age_older"] += 1
                    if t["permalink"]:
                        url_queue.append("https://www.reddit.com" + t["permalink"])
                    if len(samples) < 4:
                        samples.append(f"[{t['created']}] r/{t['subreddit']} u/{t['author']}: {t['text'][:90]}")
        finally:
            await session.close()

    async def url_checker() -> None:
        if check_endpoint == "off":
            return
        async with AsyncSession(impersonate="chrome", timeout=15) as s:
            while time.monotonic() < deadline + 5:
                await asyncio.sleep(1.0)
                while url_queue:
                    batch, url_queue[:300] = url_queue[:300], []
                    try:
                        r = await s.post(check_endpoint, json={"urls": batch})
                        data = r.json() if r.status_code == 200 else None
                    except Exception:  # noqa: BLE001
                        data = None
                    if isinstance(data, list):
                        known = {u for u in data if isinstance(u, str)}
                        c["url_known"] += len(known)
                        c["url_new"] += len(batch) - len(known)
                    else:
                        c["url_unverified"] += len(batch)

    async def monitor() -> None:
        last = Counter(c)
        t_last = time.monotonic()
        while time.monotonic() < deadline:
            await asyncio.sleep(10)
            now = time.monotonic()
            dt = now - t_last
            print(f"  {now - t_start:4.0f}s | pages/s {(c['ok'] - last['ok']) / dt:5.1f} | unique text comments/s {(c['unique_text'] - last['unique_text']) / dt:6.1f}"
                  f" | new to Exorde/s {(c['url_new'] - last['url_new']) / dt:6.1f} | blocked {c['blocked_page'] - last['blocked_page']}"
                  f" | 403/429 {c['http_403'] + c['http_429'] - last['http_403'] - last['http_429']} | fail {c['err'] + c['timeout'] - last['err'] - last['timeout']}", flush=True)
            last, t_last = Counter(c), now

    await asyncio.gather(monitor(), url_checker(), *(worker() for _ in range(workers)))
    span = time.monotonic() - t_start

    def avg(v: list[float]) -> str:
        return f"{sum(v) / len(v):.1f}s" if v else "-"

    print("\n================ REDDIT HTML RESULT ================")
    print(f"{'DIRECT (own IP), rate limit ' + str(rps) + '/s' if direct else 'nodes: ' + str(count)} | workers: {workers} | duration: {span:.0f}s | subreddits: {len(SUBS)}")
    for v in variants:
        reqs = c[f"req_{v}"]
        print(f"variant {v:>8}: requests {reqs}, pages with comments {c[f'ok_{v}']} ({100 * c[f'ok_{v}'] / max(1, reqs):.0f}%),"
              f" comments per page {c[f'comments_{v}'] / max(1, c[f'ok_{v}']):.1f}, latency avg {avg(lat[v])}")
    print(f"pages OK {c['ok']} = {c['ok'] / span:.1f}/s | blocked pages {c['blocked_page']} | no comments in html {c['no_comments_in_html']}"
          f" | 403 {c['http_403']} | 429 {c['http_429']} | other http {sum(v for k, v in c.items() if k.startswith('http_') and k not in ('http_403', 'http_429'))}")
    print(f"unique text comments {c['unique_text']} = {c['unique_text'] / span:.1f}/s | repeats {c['repeat']} | too short {c['too_short']} | no datetime {c['no_datetime']}")
    ages = {k[4:]: v for k, v in c.items() if k.startswith("age_")}
    print("age of unique comments:", json.dumps(ages))
    if check_endpoint != "off":
        total = c["url_known"] + c["url_new"]
        print(f"url check: known to Exorde {c['url_known']}, NEW to Exorde {c['url_new']} = {c['url_new'] / span:.1f}/s"
              f" ({100 * c['url_new'] / max(1, total):.0f}% of checked) | unverified {c['url_unverified']}")
    print(f"traffic (decoded): {c['bytes'] / 1e6:.0f} MB = {c['bytes'] / 1e6 / span:.1f} MB/s | page avg {c['bytes'] / max(1, sum(c[f'req_{v}'] for v in variants)) / 1024:.0f} KB")
    print(f"timeouts {c['timeout']}, errors {c['err']}")
    fails = {k[5:]: v for k, v in c.items() if k.startswith("fail_")}
    if fails:
        print("failures:", json.dumps(dict(sorted(fails.items(), key=lambda kv: -kv[1])[:6]), ensure_ascii=False))
    for text in samples:
        print("  sample:", text)


if __name__ == "__main__":
    asyncio.run(main())
