"""Reddit markup probe (not a parser): what does the /comments/ listing really contain?

Fetches a few listing pages (direct, IPv4, Chrome TLS), saves raw HTML to scripts/_samples/ and prints a compact report:
 - which data-* attributes exist on comment blocks (is there a parent / score / depth?)
 - any parent hints in the block (data-parent*, 'parent' links, 'context' links)
 - score markup, deleted/removed bodies, bot authors, time attribute format, permalink shape
 - one raw comment block, printed in full, for a human to read
Optional: PERMALINK_CHECK=N fetches the page of N sample comments to see whether the parent is visible THERE (cost of a fallback).
Usage (home PC): python3 scripts/reddit_sample_probe.py
Env: URLS=comma list (default popular + AskReddit)  PAGES=2  PAUSE=2.0  PERMALINK_CHECK=3  OUT=scripts/_samples
"""
from __future__ import annotations

import os
import re
import sys
import time
from collections import Counter
from pathlib import Path

from curl_cffi.requests import Session

try:
    from curl_cffi import CurlOpt

    IPV4 = {"curl_options": {CurlOpt.IPRESOLVE: 1}}
except ImportError:
    IPV4 = {}

URLS = [u for u in os.environ.get(
    "URLS", "https://www.reddit.com/r/popular/comments/?limit=100,https://www.reddit.com/r/AskReddit/comments/?limit=100"
).split(",") if u]
PAGES = int(os.environ.get("PAGES", "2"))
PAUSE = float(os.environ.get("PAUSE", "2.0"))
PERMALINK_CHECK = int(os.environ.get("PERMALINK_CHECK", "3"))
OUT = Path(os.environ.get("OUT", "scripts/_samples"))

THING_RE = re.compile(r'<div[^>]*\bdata-fullname="t1_[a-z0-9]+"[^>]*>', re.I)
ATTR_RE = re.compile(r'\b(data-[a-z0-9-]+)="([^"]*)"')
PARENT_HINT_RE = re.compile(r'(?i)(data-parent[a-z-]*="[^"]*"|data-event-action="(?:parent|context|permalink)"[^>]*>|rel="parent"|>\s*parent\s*<|>\s*context\s*<)')
SCORE_RE = re.compile(r'(?i)(class="score[^"]*"[^>]*>[^<]*<|data-score="[^"]*"|title="-?\d+"[^>]*>\s*-?\d+ points?)')


def blocks(html: str) -> list[str]:
    spots = list(THING_RE.finditer(html))
    return [html[m.start(): spots[i + 1].start() if i + 1 < len(spots) else m.start() + 8000][:8000] for i, m in enumerate(spots)]


def fetch(s: Session, url: str) -> tuple[int, str]:
    r = s.get(url, impersonate="chrome", timeout=20, headers={"Accept-Language": "en-US,en;q=0.9"}, **IPV4)
    return r.status_code, r.text


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    s = Session()
    attr_names: Counter = Counter()
    hints: Counter = Counter()
    n = scores = deleted = bots = 0
    time_fmts: Counter = Counter()
    permalinks: list[str] = []
    first_block = ""
    for url in URLS:
        for p in range(PAGES):
            try:
                code, html = fetch(s, url)
            except Exception as exc:  # noqa: BLE001
                print(f"ERR {url}: {type(exc).__name__} {exc}")
                continue
            name = re.sub(r"\W+", "_", url.split("reddit.com/")[1])[:40] + f"_{p}.html"
            (OUT / name).write_text(html, encoding="utf-8")
            bl = blocks(html)
            print(f"{code} {len(html):>7}B {len(bl):>3} comments  {url}  -> {OUT / name}")
            for b in bl:
                n += 1
                tag = THING_RE.match(b).group(0)
                for k, v in ATTR_RE.findall(tag):
                    attr_names[k] += 1
                for h in PARENT_HINT_RE.findall(b):
                    hints[h[:60]] += 1
                scores += bool(SCORE_RE.search(b))
                deleted += bool(re.search(r"\[(deleted|removed)\]", b))
                bots += bool(re.search(r'data-author="(AutoModerator|[A-Za-z0-9_-]*[Bb]ot)"', b))
                t = re.search(r'<time[^>]*datetime="([^"]+)"', b)
                if t:
                    time_fmts[re.sub(r"\d", "9", t.group(1))] += 1
                pm = re.search(r'data-permalink="([^"]+)"', tag)
                if pm and len(permalinks) < 20:
                    permalinks.append(pm.group(1))
                if not first_block and "class=\"md\"" in b and "[deleted]" not in b:
                    first_block = b
            time.sleep(PAUSE)
    print(f"\n== {n} comment blocks ==")
    print("data-* attributes on the block tag (count):")
    for k, c in attr_names.most_common():
        print(f"  {k:<28}{c}")
    print("parent/context hints inside blocks (count):", dict(hints.most_common(8)) or "NONE")
    print(f"with score markup: {scores}/{n}   deleted/removed: {deleted}   bot-like authors: {bots}")
    print("time formats:", dict(time_fmts))
    print("permalink samples:")
    for pl in permalinks[:5]:
        print("  ", pl)
    print("\n== one raw block (first 2500 chars) ==\n" + first_block[:2500])

    if PERMALINK_CHECK and permalinks:
        print(f"\n== permalink page check ({PERMALINK_CHECK}) ==")
        for pl in permalinks[:PERMALINK_CHECK]:
            time.sleep(PAUSE)
            try:
                code, html = fetch(s, "https://www.reddit.com" + pl)
            except Exception as exc:  # noqa: BLE001
                print("ERR", pl, type(exc).__name__)
                continue
            (OUT / ("perma_" + re.sub(r"\W+", "_", pl)[-40:] + ".html")).write_text(html, encoding="utf-8")
            bl = blocks(html)
            target = pl.rstrip("/").split("/")[-1]
            mine = next((b for b in bl if f"t1_{target}" in b[:600]), "")
            ph = PARENT_HINT_RE.findall(mine)
            print(f"{code} {len(html)}B blocks={len(bl)} target_found={bool(mine)} parent_hints={ph[:3]} {pl}")
            if mine:
                print("   attrs:", dict(ATTR_RE.findall(THING_RE.match(mine).group(0))))


if __name__ == "__main__":
    main()
