"""Why does Reddit answer 403 through the nodes? For the first K nodes: exit IP, then a few URLs x TLS profiles, with status, size,
<title>, a text snippet of the answer and the interesting response headers.
Usage: python3 scripts/reddit_403_probe.py PORT_FIRST PORT_COUNT [K=4]   (env: PAUSE=1.5)
"""
from __future__ import annotations

import os
import re
import sys
import time

from curl_cffi.requests import Session

URLS = [
    "https://www.reddit.com/r/popular/comments/?limit=100",
    "https://www.reddit.com/r/AskReddit/comments/",
    "https://www.reddit.com/",
    "https://old.reddit.com/r/popular/comments/",
]
PROFILES = ["chrome", "safari", "firefox"]
HDRS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
KEEP = ("server", "x-reddit-", "cf-ray", "x-cache", "set-cookie", "retry-after", "via", "content-type")
PAUSE = float(os.environ.get("PAUSE", "1.5"))


def snippet(text: str) -> str:
    t = re.search(r"<title>(.*?)</title>", text, re.S | re.I)
    body = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", text, flags=re.S | re.I)
    body = re.sub(r"<[^>]+>", " ", body)
    body = re.sub(r"\s+", " ", body).strip()[:220]
    return f"title={t.group(1).strip()[:80]!r} text={body!r}" if t else f"text={body!r}"


def main() -> None:
    first, count = int(sys.argv[1]), int(sys.argv[2])
    k = int(sys.argv[3]) if len(sys.argv) > 3 else 4
    for i in range(min(k, count)):
        port = first + i
        px = {"http": f"socks5h://127.0.0.1:{port}", "https": f"socks5h://127.0.0.1:{port}"}
        s = Session(proxies=px)
        try:
            ip = s.get("https://api.ipify.org", timeout=15, impersonate="chrome").text.strip()
        except Exception as exc:  # noqa: BLE001
            ip = f"? ({type(exc).__name__})"
        print(f"\n##### node port {port} exit ip {ip}")
        for url in URLS:
            for prof in PROFILES:
                time.sleep(PAUSE)
                try:
                    r = s.get(url, impersonate=prof, headers=HDRS, timeout=20)
                except Exception as exc:  # noqa: BLE001
                    print(f"  {prof:<8} {url[12:60]:<48} ERR {type(exc).__name__} {str(exc)[:60]}")
                    continue
                hs = {k_: v[:60] for k_, v in r.headers.items() if k_.lower().startswith(KEEP)}
                print(f"  {prof:<8} {url[12:60]:<48} {r.status_code} {len(r.text):>7}B comments={r.text.count('data-fullname=\"t1_')}")
                if r.status_code != 200 or i == 0:
                    print(f"      {snippet(r.text)}")
                    print(f"      headers: {hs}")


if __name__ == "__main__":
    main()
