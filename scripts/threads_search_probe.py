"""Can the anonymous search query (BarcelonaSearchResultsQuery) with recent=1 give the NEWEST posts cheaply?

Research helper (nothing here is used by the scraper). Takes the request template (all form fields, variables, flags)
from a HAR of the web client (default: the `download2` file in the repo), swaps in a fresh lsd and the query words and
prints, per word and per variant: status, KB, posts, age of posts (min/median/max minutes) and whether a next page exists.
Usage (PROBE_PROXY optional, e.g. socks5h://127.0.0.1:12001; the server's own IP may be rate limited):
  python3 scripts/threads_search_probe.py download2 the a "i" news
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
PROXY = os.getenv("PROBE_PROXY", "")


def template() -> tuple[dict[str, str], dict]:
    for e in json.load(open(HAR))["log"]["entries"]:
        q = parse_qs((e["request"].get("postData") or {}).get("text", ""), keep_blank_values=True)
        if (q.get("fb_api_req_friendly_name") or [""])[0] == "BarcelonaSearchResultsQuery":
            body = {k: v[0] for k, v in q.items()}
            return body, json.loads(body["variables"])
    raise SystemExit("no BarcelonaSearchResultsQuery in the HAR")


def walk(o, out):
    if isinstance(o, dict):
        if "taken_at" in o and "pk" in o:
            out.append(o)
        for v in o.values():
            walk(v, out)
    elif isinstance(o, list):
        for v in o:
            walk(v, out)


def main() -> None:
    body0, vars0 = template()
    kw = {"proxies": {"http": PROXY, "https": PROXY}} if PROXY else {}
    s = cffi.Session(impersonate="chrome124", timeout=30, **kw)
    s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    boot = s.get(g.BOOT_URL)
    lsd = g.find_lsd(boot.text)
    print(f"boot {boot.status_code} lsd={'yes' if lsd else 'NO'}")
    if not lsd:
        return
    for word in WORDS:
        for recent in (0, 1):
            vars_ = dict(vars0, query=word, recent=recent)
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
            r = s.post(f"{g.THREADS_BASE_URL}/graphql/query", data=urlencode(body), headers=headers)
            try:
                d = json.loads(r.text.removeprefix("for (;;);"))
                posts: list = []
                walk(d, posts)
                ages = sorted((time.time() - p["taken_at"]) / 60 for p in posts)
                info = ((d.get("data") or {}).get("searchResults") or {}).get("page_info") or {}
                print(f"{word!r} recent={recent}: {r.status_code} {len(r.content)//1024} KB posts={len(posts)} "
                      f"age_min min/med/max={ages[0]:.0f}/{statistics.median(ages):.0f}/{ages[-1]:.0f} next={info.get('has_next_page')}"
                      if ages else f"{word!r} recent={recent}: {r.status_code} {len(r.content)//1024} KB no posts head={r.text[:90]!r}")
            except ValueError:
                print(f"{word!r} recent={recent}: {r.status_code} not JSON head={r.text[:90]!r}")


main()
