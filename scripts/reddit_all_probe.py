"""Site-wide Reddit comments probe (not a parser), run from a home IP: which "all comments" pages work and how fast does the stream move?

Polls every candidate URL slowly (one request every GAP seconds, one at a time) and reports per URL: status, comments per page, how old the
newest comment is, how many seconds one page spans (= stream speed: comments/page / span) and how many NEW comments each poll brought.
Usage: DIRECT run, e.g.  ~/rt/bin/python scripts/reddit_all_probe.py [polls_per_url=8] [gap_seconds=3]
"""
from __future__ import annotations

import asyncio
import sys
import time
from datetime import datetime, timezone

from curl_cffi.requests import AsyncSession

sys.path.insert(0, __file__.rsplit("/", 1)[0])
from reddit_html_test import THING_RE, age_seconds, parse_things  # noqa: E402,F401

URLS = [
    "https://www.reddit.com/r/all/comments/",
    "https://www.reddit.com/r/all/comments/?limit=50",
    "https://www.reddit.com/r/popular/comments/",
    "https://www.reddit.com/comments/",
    "https://old.reddit.com/r/all/comments/",
    "https://www.reddit.com/r/AskReddit/comments/?limit=100",  # control: known to work
]


async def main() -> None:
    polls = int(sys.argv[1]) if len(sys.argv) > 1 else 8
    gap = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
    print(f"{'url':<56} {'ok':>5} {'items':>5} {'newest s':>8} {'span s':>7} {'stream c/s':>10} {'new/poll':>8}  notes")
    for url in URLS:
        seen: set[str] = set()
        ok = items = new_total = 0
        newest, spans, rates = [], [], []
        notes: list[str] = []
        async with AsyncSession(impersonate="chrome", timeout=20) as session:
            for i in range(polls):
                try:
                    r = await session.get(url, headers={"Accept-Language": "en-US,en;q=0.9"})
                except Exception as exc:  # noqa: BLE001
                    notes.append(f"error {str(exc)[:50]}")
                    await asyncio.sleep(gap)
                    continue
                if r.status_code != 200:
                    notes.append(f"http {r.status_code}")
                    await asyncio.sleep(gap * 2)
                    continue
                things = parse_things(r.text)
                if not things:
                    notes.append("200 but no comments")
                    await asyncio.sleep(gap)
                    continue
                ok += 1
                items += len(things)
                ages = [a for a in (age_seconds(t["created"]) for t in things if t["created"]) if a is not None]
                if ages:
                    newest.append(min(ages))
                    span = max(ages) - min(ages)
                    spans.append(span)
                    if span > 0:
                        rates.append(len(things) / span)
                fresh = [t for t in things if t["id"] not in seen]
                if i > 0:  # the first poll only fills the set
                    new_total += len(fresh)
                seen.update(t["id"] for t in things)
                await asyncio.sleep(gap)
        avg = lambda v: sum(v) / len(v) if v else 0.0  # noqa: E731
        new_per_poll = new_total / max(1, ok - 1) if ok > 1 else 0.0
        print(f"{url.replace('https://', ''):<56} {ok:>2}/{polls:<2} {items / max(1, ok):5.0f} {avg(newest):8.0f} {avg(spans):7.0f} {avg(rates):10.1f} {new_per_poll:8.1f}  {'; '.join(dict.fromkeys(notes))[:70]}")
        await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())
