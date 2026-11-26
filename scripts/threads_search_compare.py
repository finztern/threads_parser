"""Why does the parser's search request get "execution error" while the HAR-template probe works?

Research helper (nothing here is used by the scraper). In ONE session it sends (A) the request built by
scraper.threads_graphql.build_search_request and (B) the same word as a replay of the HAR request, then prints both
answers and every difference between the two requests (form fields, variables, headers).
Usage: python3 scripts/threads_search_compare.py download2 the
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from urllib.parse import parse_qsl, parse_qs, urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from curl_cffi import requests as cffi  # noqa: E402
from scraper import threads_graphql as g  # noqa: E402

HAR = sys.argv[1] if len(sys.argv) > 1 else "download2"
WORD = sys.argv[2] if len(sys.argv) > 2 else "the"
PROXY = os.getenv("PROBE_PROXY", "")


def har_request():
    for e in json.load(open(HAR))["log"]["entries"]:
        q = parse_qs((e["request"].get("postData") or {}).get("text", ""), keep_blank_values=True)
        if (q.get("fb_api_req_friendly_name") or [""])[0] == "BarcelonaSearchResultsQuery":
            return {k: v[0] for k, v in q.items()}
    raise SystemExit("no search request in the HAR")


def show(tag: str, r) -> None:
    print(f"{tag}: {r.status_code} {len(r.content)//1024} KB head={r.text[:110]!r}")


def main() -> None:
    kw = {"proxies": {"http": PROXY, "https": PROXY}} if PROXY else {}
    s = cffi.Session(impersonate="chrome124", timeout=30, **kw)
    s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    boot = s.get(g.BOOT_URL)
    lsd = g.find_lsd(boot.text)
    print("boot", boot.status_code, "lsd", bool(lsd), "proxy", PROXY or "direct")
    if not lsd:
        return
    har = har_request()
    # A: the parser's request
    data_a, headers_a = g.build_search_request(lsd, WORD, "x")
    body_a = dict(parse_qsl(data_a, keep_blank_values=True))
    # B: replay of the HAR with fresh lsd and the word (exactly what the probe did)
    body_b = dict(har, lsd=lsd)
    vars_b = json.loads(har["variables"])
    vars_b.update(query=WORD, recent=1)
    body_b["variables"] = json.dumps(vars_b, separators=(",", ":"))
    headers_b = dict(headers_a)  # same headers on purpose: only the body differs
    csrf = s.cookies.get("csrftoken")
    for h in (headers_a, headers_b):
        if csrf:
            h["X-CSRFToken"] = csrf
    ra = s.post(g.TAB_URL, data=urlencode(body_a), headers=headers_a)
    rb = s.post(g.TAB_URL, data=urlencode(body_b), headers=headers_b)
    show("A parser request", ra)
    show("B HAR replay   ", rb)
    print("\nform fields only in A:", sorted(set(body_a) - set(body_b)))
    print("form fields only in B:", sorted(set(body_b) - set(body_a)))
    for k in sorted(set(body_a) & set(body_b)):
        if k not in ("lsd", "variables") and body_a[k] != body_b[k]:
            print(f"differs {k}: A={body_a[k][:50]!r} B={body_b[k][:50]!r}")
    va, vb = json.loads(body_a["variables"]), json.loads(body_b["variables"])
    for k in sorted(set(va) | set(vb)):
        if va.get(k, "<none>") != vb.get(k, "<none>"):
            print(f"variable {k}: A={va.get(k, '<none>')!r} B={vb.get(k, '<none>')!r}")


main()
