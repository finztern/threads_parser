"""Arctic Shift test (not a parser): a public, account-free API with Reddit comments (https://arctic-shift.photon-reddit.com).
It is a volunteer project: this test is polite (default 3 requests/s, backs off on 429).

Phase A: quick probes - does a query without subreddit work (a Reddit-wide stream)? how old is the newest comment (ingest lag)?
Phase B: sustained polling of 50 busy subreddits round-robin: comments per second, lag at first sight, Exorde url-check dedup.
Run from the parser image (curl_cffi inside), directly from this host:
  docker compose run --rm --no-deps -v "$PWD/scripts:/app/scripts:ro" parser python3 /app/scripts/arctic_shift_test.py
Env: SECONDS_RUN=90  RPS=3  URL_CHECK=<endpoint or "off">
"""
from __future__ import annotations

import asyncio
import json
import os
import statistics
import time
from collections import Counter

from curl_cffi.requests import AsyncSession

BASE = "https://arctic-shift.photon-reddit.com/api/comments/search"
SUBS = """AskReddit politics worldnews news funny gaming pics todayilearned movies memes interestingasfuck mildlyinteresting
Showerthoughts technology science aww music sports nba soccer nfl television Futurology space wallstreetbets CryptoCurrency Bitcoin
stocks investing personalfinance relationship_advice AmItheAsshole tifu unpopularopinion explainlikeimfive LifeProTips NoStupidQuestions
OutOfTheLoop pcmasterrace Fauxmoi Conservative PublicFreakout facepalm mildlyinfuriating AskMen AskWomen CasualConversation teenagers
food buildapc Android""".split()
MIN_TEXT = 7


def comment_url(item: dict) -> str:
    if item.get("permalink"):
        link = item["permalink"]
        return link if link.startswith("http") else "https://www.reddit.com" + link
    link_id = str(item.get("link_id", "")).removeprefix("t3_")
    return f"https://www.reddit.com/r/{item.get('subreddit', '')}/comments/{link_id}/_/{item.get('id', '')}/"


async def get(session: AsyncSession, params: str) -> tuple[int, list[dict], dict, float, str]:
    t0 = time.monotonic()
    r = await session.get(f"{BASE}?{params}")
    data: list[dict] = []
    err = ""
    if r.status_code == 200:
        try:
            body = r.json()
            data = body.get("data") or []
            err = str(body.get("error") or "")
        except Exception as exc:  # noqa: BLE001
            err = f"bad json: {exc}"
    else:
        err = r.text[:160].replace("\n", " ")
    headers = {k: v for k, v in r.headers.items() if "ratelimit" in k.lower() or k.lower() == "retry-after"}
    return r.status_code, data, headers, time.monotonic() - t0, err


async def main() -> None:
    seconds = float(os.environ.get("SECONDS_RUN", "90"))
    rps = float(os.environ.get("RPS", "3"))
    check_endpoint = os.environ.get("URL_CHECK", "https://url-check.exorde.network/v1/check")
    async with AsyncSession(impersonate="chrome", timeout=30) as session:
        print("---- phase A: probes ----")
        for label, params in (
            ("no filter, newest first", "limit=100&sort=desc"),
            ("r/AskReddit newest", "subreddit=AskReddit&limit=100&sort=desc"),
            ("r/news newest", "subreddit=news&limit=100&sort=desc"),
            ("r/funny newest", "subreddit=funny&limit=100&sort=desc"),
            ("no filter, after=now-120s asc", f"after={int(time.time()) - 120}&limit=100&sort=asc"),
        ):
            try:
                status, data, headers, dt, err = await get(session, params)
            except Exception as exc:  # noqa: BLE001
                print(f"  {label:<32} error: {str(exc)[:80]}")
                continue
            if data:
                now = time.time()
                ages = sorted(now - float(i.get("created_utc", now)) for i in data)
                print(f"  {label:<32} http {status} | {len(data)} items | {dt:.1f}s | newest {ages[0]:.0f}s old, oldest {ages[-1]:.0f}s old"
                      f" | keys: {','.join(sorted(data[0])[:12])} {headers or ''}")
            else:
                print(f"  {label:<32} http {status} | 0 items | {dt:.1f}s | {err or 'empty'} {headers or ''}")

        print(f"\n---- phase B: {seconds:.0f}s, {rps:g} requests/s over {len(SUBS)} subreddits ----")
        c: Counter[str] = Counter()
        seen: set[str] = set()
        lags: list[float] = []
        urls: list[str] = []
        samples: list[str] = []
        statuses: Counter[str] = Counter()
        started = time.monotonic()
        deadline = started + seconds
        pause_until = 0.0

        async def poll(sub: str) -> None:
            nonlocal pause_until
            try:
                status, data, headers, dt, err = await get(session, f"subreddit={sub}&limit=100&sort=desc")
            except Exception as exc:  # noqa: BLE001
                statuses["error"] += 1
                c[f"fail_{str(exc)[:40]}"] += 1
                return
            statuses[f"http_{status}"] += 1
            c["latency_sum"] += dt
            if status == 429:
                pause_until = time.monotonic() + float(headers.get("retry-after", 10) or 10)
                return
            full = 0
            for item in data:
                cid = item.get("id")
                if not cid or cid in seen:
                    c["repeat"] += 1
                    continue
                seen.add(cid)
                full += 1
                body = (item.get("body") or "").strip()
                if len(body) < MIN_TEXT or body in ("[deleted]", "[removed]"):
                    c["too_short_or_deleted"] += 1
                    continue
                c["unique_text"] += 1
                if item.get("created_utc"):
                    lags.append(time.time() - float(item["created_utc"]))
                urls.append(comment_url(item))
                if len(samples) < 3:
                    samples.append(f"r/{item.get('subreddit')} u/{item.get('author')} {lags[-1] if lags else 0:.0f}s ago: {body[:80]}")
            if len(data) == 100 and full == 100:
                c["pages_all_new"] += 1  # the page was entirely new: more comments than we can see at this poll rate

        async def check_urls() -> None:
            if check_endpoint == "off":
                return
            async with AsyncSession(impersonate="chrome", timeout=15) as s:
                while time.monotonic() < deadline + 5:
                    await asyncio.sleep(1.0)
                    while urls:
                        batch, urls[:300] = urls[:300], []
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

        async def scheduler() -> None:
            i = 0
            tasks: set[asyncio.Task] = set()
            while time.monotonic() < deadline:
                if time.monotonic() >= pause_until:
                    t = asyncio.create_task(poll(SUBS[i % len(SUBS)]))
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
                    i += 1
                await asyncio.sleep(1.0 / rps)
                if i % 15 == 0 and i:
                    print(f"  {time.monotonic() - started:4.0f}s | requests {sum(statuses.values())} | unique text {c['unique_text']} | {dict(statuses)}", flush=True)
            await asyncio.gather(*tasks, return_exceptions=True)

        await asyncio.gather(scheduler(), check_urls())
        span = time.monotonic() - started
        reqs = sum(statuses.values())
        print("\n================ ARCTIC SHIFT RESULT ================")
        print(f"duration {span:.0f}s | requests {reqs} = {reqs / span:.1f}/s | statuses {dict(statuses)} | avg latency {c['latency_sum'] / max(1, reqs):.1f}s")
        print(f"unique text comments {c['unique_text']} = {c['unique_text'] / span:.1f}/s | repeats {c['repeat']} | short/deleted {c['too_short_or_deleted']}"
              f" | pages entirely new (more than 100 per poll interval): {c['pages_all_new']}")
        if lags:
            lags.sort()
            print(f"age at first sight (ingest lag + poll interval): median {statistics.median(lags):.0f}s, p90 {lags[int(len(lags) * 0.9)]:.0f}s, min {lags[0]:.0f}s")
        if check_endpoint != "off":
            total = c["url_known"] + c["url_new"]
            print(f"url check: known to Exorde {c['url_known']}, NEW to Exorde {c['url_new']} = {c['url_new'] / span:.1f}/s"
                  f" ({100 * c['url_new'] / max(1, total):.0f}% of checked) | unverified {c['url_unverified']}")
        fails = {k[5:]: v for k, v in c.items() if k.startswith("fail_")}
        if fails:
            print("failures:", json.dumps(fails, ensure_ascii=False))
        for text in samples:
            print("  sample:", text)


if __name__ == "__main__":
    asyncio.run(main())
