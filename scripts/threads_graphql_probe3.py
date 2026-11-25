"""Why does the anonymous Replies-tab GraphQL query answer 401 on a session bootstrapped from /t/news?

Research helper (nothing here is used by the scraper). Loads an lsd token from several pages, sends the tab query
(scraper/threads_graphql.py) in a few variants and prints status + size + first bytes of each answer.
Usage (inside the running container; PROBE_PROXY optional, e.g. socks5h://127.0.0.1:12001):
  python3 scripts/threads_graphql_probe3.py zuck
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from urllib.parse import urlencode, parse_qsl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from curl_cffi import requests as cffi  # noqa: E402
from scraper import threads_graphql as g  # noqa: E402
from scraper.threads_parse import THREADS_BASE_URL, _extract_ssr_user_replies  # noqa: E402

USER = sys.argv[1] if len(sys.argv) > 1 else "zuck"
PROXY = os.getenv("PROBE_PROXY", "")


def session():
    kw = {"proxies": {"http": PROXY, "https": PROXY}} if PROXY else {}
    s = cffi.Session(impersonate="chrome124", timeout=30, **kw)
    s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    return s


def boot(s, path: str) -> tuple[str | None, str]:
    r = s.get(f"{THREADS_BASE_URL}{path}", allow_redirects=True)
    m = re.search(r'"jazoest"\s*[:=]\s*"?(\d+)', r.text) or re.search(r"jazoest=(\d+)", r.text)
    print(f"  boot {path}: {r.status_code} {len(r.content)//1024} KB lsd={'yes' if g.find_lsd(r.text) else 'NO'}", flush=True)
    return g.find_lsd(r.text), (m.group(1) if m else ""), r.text


def tab(s, lsd: str, uid: str, url: str, extra: dict[str, str], csrf: bool, first: int = 5) -> None:
    data, headers = g.build_tab_request(lsd, f"{THREADS_BASE_URL}/@{USER}/replies", uid, first)
    body = dict(parse_qsl(data, keep_blank_values=True))
    body.update(extra)
    c = s.cookies.get("csrftoken")
    if csrf and c:
        headers["X-CSRFToken"] = c
    r = s.post(url, data=urlencode(body), headers=headers)
    ok, posts = g.parse_graphql_tab(r.text, USER)
    print(f"    -> {r.status_code} {len(r.content)//1024} KB ok={ok} replies={len(posts)} head={r.text[:90]!r}", flush=True)


def main() -> None:
    s0 = session()
    r = s0.get(f"{THREADS_BASE_URL}/@{USER}/replies")
    posts = _extract_ssr_user_replies(r.text, USER)
    uid = next((p.author_pk for p in posts if p.author_pk), "")
    print(f"tab page: {r.status_code}, replies parsed={len(posts)}, userID={uid or 'NOT FOUND'}")
    if not uid:
        return
    for page in ("/t/news", f"/@{USER}", f"/@{USER}/replies"):
        for fresh in (True, False):
            s = session() if fresh else s0
            print(f"lsd from {page} ({'fresh session' if fresh else 'session that loaded the tab page'})")
            lsd, jaz, _ = boot(s, page)
            if not lsd:
                continue
            for url in (g.TAB_URL, g.GRAPHQL_URL):
                for label, extra, csrf in (("base", {}, True), ("+jazoest", {"jazoest": jaz} if jaz else {}, True), ("no csrf hdr", {}, False)):
                    print(f"  {url.rsplit('/',2)[-2]}/{url.rsplit('/',1)[-1]} {label}", flush=True)
                    tab(s, lsd, uid, url, extra, csrf)


main()
