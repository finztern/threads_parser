"""Is the anonymous GraphQL replies query (sortOrder=RECENT) usable as the cheap way to read the NEWEST replies of a post?

Research helper (nothing here is used by the scraper). Builds on scripts/threads_graphql_probe.py. It answers:
  1. bytes per comment for RECENT with first=10/30/50 (the post page gives ~14 ranked replies for ~1 MB) and how many of the
     returned comments are young (<15 min, <60 min) and NOT visible on the post page;
  2. does paging with the returned cursor keep going back in time (more new replies for the big posts);
  3. is the `lsd` token reusable through ANOTHER proxy / fresh session (needed to use a pool of exits) and which cheap page
     (instead of the 1 MB post page) also carries an lsd token.
Usage (inside the running container):
  PROBE_PROXIES=socks5h://127.0.0.1:12001,socks5h://127.0.0.1:12003,socks5h://127.0.0.1:12006 \
    python3 scripts/threads_graphql_probe2.py https://www.threads.com/@USER/post/CODE
Pick a post with many replies (100+) and fresh activity. First proxy = A (also loads the post page), the others = B, C.
"""
from __future__ import annotations

import json
import os
import re
import statistics
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

sys.path.insert(0, str(Path(__file__).resolve().parent))
import threads_graphql_probe as g  # noqa: E402
from curl_cffi import requests as cffi  # noqa: E402
from scraper.threads_parse import _extract_reply_posts, _extract_ssr_reply_posts  # noqa: E402

BOOT_PAGES = ["t/news", "explore", "topic/news", "login"]


def make_session(proxy: str):
    s = cffi.Session(impersonate="chrome124", proxies={"http": proxy, "https": proxy}, timeout=30)
    s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    return s


def gql(session, lsd: str, page_url: str, pk: int, first: int, after, sort: str = "RECENT") -> tuple[int, str]:
    variables = {"after": after, "filterType": None, "first": first, "postID": str(pk), "sortOrder": sort, **g.RELAY_FLAGS}
    body = {
        "lsd": lsd, "fb_api_caller_class": "RelayModern", "fb_api_req_friendly_name": g.QUERY_NAME,
        "variables": json.dumps(variables, separators=(",", ":")), "server_timestamps": "true", "doc_id": g.DOC_ID,
        "__a": "1", "__user": "0", "__comet_req": "29", "__crn": "comet.threads.BarcelonaPostColumnRoute",
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded", "Origin": g.THREADS_BASE_URL, "Referer": page_url,
        "X-FB-LSD": lsd, "X-FB-Friendly-Name": g.QUERY_NAME, "X-IG-App-ID": g.APP_ID, "X-ASBD-ID": "359341",
        "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
    }
    csrf = session.cookies.get("csrftoken")
    if csrf:
        headers["X-CSRFToken"] = csrf
    r = session.post(g.GRAPHQL_URL, data=urlencode(body), headers=headers)
    return r.status_code, r.text


def parse(text: str, pk: int):
    """-> (posts, next_cursor, has_next) or None when the answer is not a replies result."""
    try:
        data = json.loads(re.sub(r"^for \(;;\);", "", text))
    except ValueError:
        return None
    try:
        info = data["data"]["media"]["text_post_app_info"]["direct_replies"]["page_info"]
    except (KeyError, TypeError):
        return None
    posts = _extract_reply_posts(data, str(pk))
    return posts, info.get("end_cursor"), info.get("has_next_page")


def ages_min(posts) -> list[float]:
    now = time.time()
    return sorted((now - p.created_at.timestamp()) / 60 for p in posts)


def describe(posts) -> str:
    a = ages_min(posts)
    if not a:
        return "no comments"
    return f"newest={a[0]:.0f}m median={statistics.median(a):.0f}m oldest={a[-1]:.0f}m | <15m={sum(1 for x in a if x < 15)} <60m={sum(1 for x in a if x < 60)}"


def main() -> None:
    if len(sys.argv) < 2 or not re.search(r"/@[^/]+/post/[^/?#]+", sys.argv[1]):
        print(__doc__)
        sys.exit(1)
    page_url = sys.argv[1].split("?")[0]
    code = re.search(r"/post/([^/?#]+)", page_url).group(1)
    proxies = [p.strip() for p in os.environ.get("PROBE_PROXIES", "").split(",") if p.strip()]
    if not proxies:
        print("set PROBE_PROXIES=socks5h://127.0.0.1:12001,socks5h://127.0.0.1:12003,...")
        sys.exit(1)
    pk = g.shortcode_to_pk(code)

    sa = make_session(proxies[0])
    page = sa.get(page_url)
    lsd_a = g.find_lsd(page.text)
    ssr_ids = {p.external_id for p in _extract_ssr_reply_posts(page.text, str(pk))}
    print(f"[A page] http={page.status_code} {len(page.text) // 1024}KB lsd={'yes' if lsd_a else 'NO'} ssr_replies={len(ssr_ids)}")
    if not lsd_a:
        sys.exit(2)

    print("\n1) RECENT, no cursor, different page sizes (proxy A):")
    best = None
    for first in (10, 30, 50):
        status, text = gql(sa, lsd_a, page_url, pk, first, None)
        res = parse(text, pk)
        if not res:
            print(f"  first={first}: http={status} FAILED {text[:100]!r}")
            continue
        posts, cursor, has_next = res
        new_vs_page = sum(1 for p in posts if p.external_id not in ssr_ids)
        kb = len(text) / 1024
        print(f"  first={first}: http={status} comments={len(posts)} {kb:.0f}KB = {kb / max(len(posts), 1):.1f}KB/comment | "
              f"{describe(posts)} | not on post page: {new_vs_page} | has_next={has_next}")
        if first == 30:
            best = (cursor, has_next)
        time.sleep(1.0)

    print("\n2) paging with the cursor, RECENT first=20 (proxy A):")
    cursor = None
    for i in range(1, 5):
        status, text = gql(sa, lsd_a, page_url, pk, 20, cursor)
        res = parse(text, pk)
        if not res:
            print(f"  page {i}: http={status} FAILED {text[:100]!r}")
            break
        posts, cursor, has_next = res
        print(f"  page {i}: comments={len(posts)} {len(text) // 1024}KB | {describe(posts)} | has_next={has_next}")
        if not has_next or not cursor:
            break
        time.sleep(1.0)

    print("\n3) cheap pages that carry an lsd token (proxy A):")
    cheap = {}
    for path in BOOT_PAGES:
        try:
            r = sa.get(f"{g.THREADS_BASE_URL}/{path}")
            token = g.find_lsd(r.text)
            print(f"  {path}: http={r.status_code} {len(r.text) // 1024}KB lsd={'yes' if token else 'no'}")
            if token:
                cheap[path] = token
        except Exception as exc:  # noqa: BLE001 - probe output only
            print(f"  {path}: {type(exc).__name__}")

    print("\n4) lsd reuse through other exits:")
    for label, proxy in zip("BC", proxies[1:3]):
        try:
            sb = make_session(proxy)
            status, text = gql(sb, lsd_a, page_url, pk, 10, None)
            res = parse(text, pk)
            print(f"  [{label}] lsd of A, fresh session: http={status} {'OK comments=%d' % len(res[0]) if res else 'FAILED ' + text[:80]!r}")
            for path, token in list(cheap.items())[:1]:
                r = sb.get(f"{g.THREADS_BASE_URL}/{path}")
                own = g.find_lsd(r.text)
                status, text = gql(sb, own, page_url, pk, 10, None) if own else (0, "")
                res = parse(text, pk)
                print(f"  [{label}] own lsd from /{path} ({len(r.text) // 1024}KB): {'OK comments=%d' % len(res[0]) if res else 'FAILED ' + text[:80]!r}")
        except Exception as exc:  # noqa: BLE001
            print(f"  [{label}] {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
