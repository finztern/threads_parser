"""Reddit access matrix (not a parser): which account-free ways to read the latest comments work, directly from this host and through
the free vless nodes? Every variant is tried with curl_cffi (Chrome TLS) on several subreddits; a page counts only when real comments
are inside (so a 200 block page does not pass).
Usage: python3 scripts/reddit_variants_probe.py PORT_FIRST PORT_COUNT [attempts_per_variant_via_nodes=12]
Env: IMPERSONATE=chrome   SUBS=AskReddit,news,worldnews
"""
from __future__ import annotations

import asyncio
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict

from curl_cffi.requests import AsyncSession

RE_THING = re.compile(r'data-fullname="t1_')
VARIANTS = {  # name: (url template, content kind)
    "www html /comments/": ("https://www.reddit.com/r/{s}/comments/", "html"),
    "www html /comments/?limit=100": ("https://www.reddit.com/r/{s}/comments/?limit=100", "html"),
    "www json /comments.json": ("https://www.reddit.com/r/{s}/comments.json?limit=100", "json"),
    "www json /comments/.json": ("https://www.reddit.com/r/{s}/comments/.json?limit=100", "json"),
    "www rss /comments/.rss": ("https://www.reddit.com/r/{s}/comments/.rss?limit=100", "rss"),
    "www rss multi a+b+c": ("https://www.reddit.com/r/{s}+funny+pics/comments/.rss?limit=50", "rss"),
    "www json /new.json (posts)": ("https://www.reddit.com/r/{s}/new.json?limit=100", "json_posts"),
    "old html /comments/": ("https://old.reddit.com/r/{s}/comments/", "html"),
    "old json /comments.json": ("https://old.reddit.com/r/{s}/comments.json?limit=100", "json"),
    "old rss /comments/.rss": ("https://old.reddit.com/r/{s}/comments/.rss?limit=100", "rss"),
    "api.reddit.com /comments": ("https://api.reddit.com/r/{s}/comments?limit=100", "json"),
    "m.reddit html /comments/": ("https://m.reddit.com/r/{s}/comments/", "html"),
    "www html /new/ (posts)": ("https://www.reddit.com/r/{s}/new/", "html_posts"),
    "shreddit partial new posts": ("https://www.reddit.com/svc/shreddit/community-more-posts/new/?name={s}", "html_posts"),
    "pullpush (3rd party) comments": ("https://api.pullpush.io/reddit/search/comment/?subreddit={s}&size=100&sort=desc", "json"),
    "arctic-shift (3rd party) comments": ("https://arctic-shift.photon-reddit.com/api/comments/search?subreddit={s}&limit=100&sort=desc", "json"),
}


def census(text: str) -> str:
    """What is inside a 200 page that had no comments: title, marker counts and the most common custom tags."""
    title = re.search(r"<title>(.*?)</title>", text, re.S)
    tags = Counter(re.findall(r"<([a-z]+-[a-z0-9-]+)", text)).most_common(6)
    marks = {m: text.count(m) for m in ("t1_", "t3_", "shreddit-post", "shreddit-comment", "Verify", "captcha", "login")}
    return f"title={title.group(1)[:60]!r} marks={ {k: v for k, v in marks.items() if v} } tags={tags}"


def count_items(text: str, kind: str) -> int:
    if kind == "html":
        return len(RE_THING.findall(text)) + text.count("<shreddit-comment ")
    if kind in ("json", "json_posts"):
        return text.count('"body"') if kind == "json" else text.count('"title"')
    if kind == "rss":
        return text.count("<entry")
    if kind == "html_posts":
        return text.count("<shreddit-post ") + text.count("<article") + text.count("data-fullname=\"t3_")
    return 0


async def main() -> None:
    first, count = int(sys.argv[1]), int(sys.argv[2])
    attempts = int(sys.argv[3]) if len(sys.argv) > 3 else 12
    imp = os.environ.get("IMPERSONATE", "chrome")
    subs = os.environ.get("SUBS", "AskReddit,news,worldnews").split(",")
    ports = list(range(first, first + count))
    results: dict[tuple[str, str], Counter] = defaultdict(Counter)
    sizes: dict[tuple[str, str], list[int]] = defaultdict(list)
    items: dict[tuple[str, str], list[int]] = defaultdict(list)
    notes: dict[tuple[str, str], str] = {}
    sem = asyncio.Semaphore(20)

    async def one(name: str, route: str, port: int | None, sub: str) -> None:
        url, kind = VARIANTS[name]
        key = (name, route)
        proxies = {"http": f"socks5h://127.0.0.1:{port}", "https": f"socks5h://127.0.0.1:{port}"} if port else None
        async with sem:
            try:
                async with AsyncSession(impersonate=imp, timeout=20, proxies=proxies) as s:
                    r = await s.get(url.format(s=sub), headers={"Accept-Language": "en-US,en;q=0.9"})
                text = r.text
                n = count_items(text, kind)
                results[key]["ok" if r.status_code == 200 and n > 0 else f"http_{r.status_code}" if r.status_code != 200 else "200_no_items"] += 1
                sizes[key].append(len(r.content))
                items[key].append(n)
                if key not in notes and not (r.status_code == 200 and n > 0):
                    notes[key] = f"{r.status_code}: {re.sub(r'\s+', ' ', text[:140])!r}" + (f"\n      census: {census(text)}" if r.status_code == 200 else "")
            except Exception as exc:  # noqa: BLE001
                results[key]["timeout" if "timed out" in str(exc).lower() or "(28)" in str(exc) else "conn_error"] += 1

    tasks = []
    for name in VARIANTS:
        for i in range(3):  # direct from this host (no VPN)
            tasks.append(one(name, "direct", None, subs[i % len(subs)]))
        for i in range(attempts):  # through different nodes
            tasks.append(one(name, "vpn", random.choice(ports), subs[i % len(subs)]))
    t0 = time.monotonic()
    await asyncio.gather(*tasks)
    print(f"\n================ REDDIT VARIANTS ({time.monotonic() - t0:.0f}s, impersonate={imp}, subs={','.join(subs)}) ================")
    print(f"{'variant':<36} {'route':<7} {'ok':>6}  codes                      avg KB  items/page")
    for name in VARIANTS:
        for route in ("direct", "vpn"):
            c = results[(name, route)]
            total = sum(c.values())
            ok = c["ok"]
            codes = " ".join(f"{k}:{v}" for k, v in sorted(c.items()) if k != "ok")
            sz = sizes[(name, route)]
            it = items[(name, route)]
            good = [n for n in it if n]
            print(f"{name:<36} {route:<7} {ok:>3}/{total:<3}  {codes:<26} {sum(sz) / len(sz) / 1024 if sz else 0:6.0f}  {sum(good) / len(good) if good else 0:6.1f}")
    print("\nwhat the non-working ones return (first example):")
    for (name, route), text in notes.items():
        if results[(name, route)]["ok"] == 0 and (text.startswith("200") or route == "direct"):
            print(f"  {name} [{route}] {text}")


if __name__ == "__main__":
    asyncio.run(main())
