"""Can the anonymous search query (BarcelonaSearchResultsQuery) with recent=1 give the NEWEST posts cheaply?

Research helper (nothing here is used by the scraper). Takes the request template (all form fields, variables, flags)
from a HAR of the web client (default: the `download2` file in the repo), swaps in a fresh lsd and the query words and
prints, per proxy and word (recent=1 only): status, KB, posts, age of posts (min/median/max minutes) and whether a next page exists.
Usage: without PROBE_PROXIES the first 4 local node ports (12000-12999) are used, else
  PROBE_PROXIES=socks5h://127.0.0.1:12001,socks5h://127.0.0.1:12003 python3 scripts/threads_search_probe.py download2 the a i news
"""
from __future__ import annotations

import base64  # noqa: F401
import json
import os
import statistics
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from curl_cffi import requests as cffi  # noqa: E402
from scraper import threads_graphql as g  # noqa: E402

HAR = sys.argv[1] if len(sys.argv) > 1 else "download2"
WORDS = sys.argv[2:] or ["the", "a", "news"]
PROXIES = [x for x in os.getenv("PROBE_PROXIES", os.getenv("PROBE_PROXY", "")).split(",") if x]


def local_ports(limit: int) -> list[str]:
    """Listening local SOCKS ports of the node bridge (12000-12999), read from /proc/net/tcp."""
    found: list[str] = []
    try:
        for line in open("/proc/net/tcp").read().splitlines()[1:]:
            parts = line.split()
            addr, port = parts[1].split(":")
            if parts[3] == "0A" and addr == "0100007F" and 12000 <= int(port, 16) <= 12999:
                found.append(f"socks5h://127.0.0.1:{int(port, 16)}")
    except OSError:
        pass
    return found[:limit]


def template() -> tuple[dict[str, str], dict]:
    for e in json.load(open(HAR))["log"]["entries"]:
        q = parse_qs((e["request"].get("postData") or {}).get("text", ""), keep_blank_values=True)
        if (q.get("fb_api_req_friendly_name") or [""])[0] == "BarcelonaSearchResultsQuery":
            body = {k: v[0] for k, v in q.items()}
            return body, json.loads(body["variables"])
    raise SystemExit("no BarcelonaSearchResultsQuery in the HAR")


def walk(o, out):
    if isinstance(o, dict):
        if o.get("taken_at") and "pk" in o:
            out.append(o)
        for v in o.values():
            walk(v, out)
    elif isinstance(o, list):
        for v in o:
            walk(v, out)


def run_one(proxy: str, body0: dict, vars0: dict, seen: dict[str, int]) -> None:
    kw = {"proxies": {"http": proxy, "https": proxy}} if proxy else {}
    s = cffi.Session(impersonate="chrome124", timeout=30, **kw)
    s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    print(f"--- proxy {proxy or 'direct'}")
    try:
        boot = s.get(g.BOOT_URL)
    except Exception as exc:  # noqa: BLE001
        print("  boot failed:", str(exc)[:80])
        return
    lsd = g.find_lsd(boot.text)
    print(f"  boot {boot.status_code} lsd={'yes' if lsd else 'NO'}")
    if not lsd:
        return
    for word in WORDS:
        vars_ = dict(vars0, query=word, recent=1)
        body = dict(body0, lsd=lsd, variables=json.dumps(vars_, separators=(",", ":")))
        headers = {
            "Content-Type": "application/x-www-form-urlencoded", "Origin": g.THREADS_BASE_URL,
            "Referer": f"{g.THREADS_BASE_URL}/search?q={word}&serp_type=default",
            "X-FB-LSD": lsd, "X-FB-Friendly-Name": "BarcelonaSearchResultsQuery", "X-IG-App-ID": g.APP_ID,
            "X-ASBD-ID": "359341", "X-Logged-Out-Threads-Migrated-Request": "true",
            "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
        }
        c = s.cookies.get("csrftoken")
        if c:
            headers["X-CSRFToken"] = c
        try:
            r = s.post(f"{g.THREADS_BASE_URL}/graphql/query", data=urlencode(body), headers=headers)
            d = json.loads(r.text.removeprefix("for (;;);"))
        except Exception as exc:  # noqa: BLE001
            print(f"  {word!r}: failed {str(exc)[:70]}")
            continue
        posts: list = []
        walk(d, posts)
        ages = sorted((time.time() - p["taken_at"]) / 60 for p in posts)
        new = sum(1 for p in posts if str(p["pk"]) not in seen)
        for p in posts:
            seen[str(p["pk"])] = seen.get(str(p["pk"]), 0) + 1
        replies = sum(1 for p in posts if (p.get("text_post_app_info") or {}).get("is_reply"))
        if ages:
            print(f"  {word!r}: {r.status_code} {len(r.content)//1024} KB posts={len(posts)} (new to probe={new}, replies={replies}) "
                  f"age_min min/med/max={ages[0]:.0f}/{statistics.median(ages):.0f}/{ages[-1]:.0f}")
        else:
            print(f"  {word!r}: {r.status_code} {len(r.content)//1024} KB no posts head={r.text[:90]!r}")


def main() -> None:
    body0, vars0 = template()
    proxies = PROXIES or local_ports(4) or [""]
    seen: dict[str, int] = {}
    for proxy in proxies:
        run_one(proxy, body0, vars0, seen)
    print(f"unique posts over all proxies and words: {len(seen)}")


main()
