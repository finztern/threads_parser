"""Can Threads profiles/replies be read by plain HTTP (no browser, no account)?

Usage: python3 scripts/threads_http_probe.py [username]
Env:   PROBE_PROXY (http://user:pass@host:port) - keep it in .env only.

Fetches the profile page with curl_cffi (Chrome TLS fingerprint), parses the
embedded SSR JSON with the same extractors the scraper uses, then opens the
newest post that has replies and parses its replies too. Prints counts and
timings so memory/speed can be judged against the browser variant.
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from curl_cffi import requests as cffi  # noqa: E402

from scraper.threads_parse import _extract_ssr_feed_posts, _extract_ssr_reply_posts  # noqa: E402
from scraper.constants import BASE_DIR  # noqa: E402

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def main() -> None:
    raw_proxy = os.environ.get("PROBE_PROXY")
    if not raw_proxy:
        print("PROBE_PROXY env var is required", file=sys.stderr)
        sys.exit(1)
    user = (sys.argv[1] if len(sys.argv) > 1 else "zuck").lstrip("@")

    out = BASE_DIR / "debug"
    out.mkdir(parents=True, exist_ok=True)
    s = cffi.Session(impersonate="chrome124", proxies={"http": raw_proxy, "https": raw_proxy}, timeout=30)
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

    t0 = time.time()
    r = s.get(f"https://www.threads.com/@{user}")
    posts = _extract_ssr_feed_posts(r.text, user)
    print(f"profile @{user}: http={r.status_code} len={len(r.text)} posts={len(posts)} time={time.time()-t0:.1f}s")
    (out / "threads_http_profile.html").write_text(r.text, encoding="utf-8")
    for p in posts[:3]:
        print("  ", p.created_at.date(), p.url, repr(p.content[:50]))

    target = next((p for p in posts if (p.extra or {}).get("direct_reply_count")), None)
    if not target:
        print("no post with replies to open")
        return
    code = target.url.rsplit("/post/", 1)[-1]
    t0 = time.time()
    r = s.get(f"https://www.threads.com/@{user}/post/{code}")
    replies = _extract_ssr_reply_posts(r.text, target.external_id)
    print(f"post page {code}: http={r.status_code} len={len(r.text)} replies={len(replies)} time={time.time()-t0:.1f}s")
    (out / "threads_http_post.html").write_text(r.text, encoding="utf-8")
    for p in replies[:3]:
        print("  ", p.created_at.date(), p.username, repr(p.content[:50]), "parent=", p.external_parent_id)


if __name__ == "__main__":
    main()
