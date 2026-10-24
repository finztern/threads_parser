"""TikTok discovery probe, plain HTTP only (no browser, no accounts).

Question: is there any page that lists videos (ids) in its HTML, so we do
not depend on the blocked /api/post/item_list/ call?

Usage: python3 scripts/tiktok_discover.py [username]
Env:   PROBE_PROXY (http://user:pass@host:port) - keep it in .env only.
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from curl_cffi import requests as cffi  # noqa: E402

from scraper.constants import BASE_DIR  # noqa: E402

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
DATA_RE = re.compile(r'<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>(.*?)</script>', re.S)
VIDEO_ID_RE = re.compile(r"/video/(\d{15,22})")
ID_FIELD_RE = re.compile(r'"(?:id|itemId|aweme_id)"\s*:\s*"(\d{17,20})"')


def candidates(user: str) -> list[str]:
    return [
        f"/@{user}",
        f"/embed/@{user}",
        f"/@{user}?lang=en",
        "/explore",
        "/foryou",
        "/tag/fyp",
        "/tag/funny",
        "/discover",
        "/trending",
        "/music/original-sound-7000000000000000000",
        "/robots.txt",
        "/sitemap.xml",
        f"/node/share/user/@{user}",
        f"/api/user/detail/?uniqueId={user}&aid=1988",
    ]


def main() -> None:
    raw_proxy = os.environ.get("PROBE_PROXY")
    if not raw_proxy:
        print("PROBE_PROXY env var is required", file=sys.stderr)
        sys.exit(1)
    user = (sys.argv[1] if len(sys.argv) > 1 else "mrbeast").lstrip("@")

    out = BASE_DIR / "debug"
    out.mkdir(parents=True, exist_ok=True)
    s = cffi.Session(impersonate="chrome124", proxies={"http": raw_proxy, "https": raw_proxy}, timeout=30)
    s.headers.update({"User-Agent": UA, "Accept-Language": "en-US,en;q=0.9"})

    all_ids: set[str] = set()
    for path in candidates(user):
        url = "https://www.tiktok.com" + path
        try:
            r = s.get(url)
        except Exception as exc:
            print(f"{path:55} ERROR {exc!s:.80}")
            continue
        text = r.text
        ids = set(VIDEO_ID_RE.findall(text)) | set(ID_FIELD_RE.findall(text))
        all_ids |= ids
        scopes: list[str] = []
        m = DATA_RE.search(text)
        region = bot = None
        if m:
            try:
                sc = json.loads(m.group(1)).get("__DEFAULT_SCOPE__", {})
                scopes = [k for k in sc if k not in ("webapp.a-b", "webapp.biz-context", "webapp.i18n-translation", "seo.abtest", "webapp.app-context")]
                ctx = sc.get("webapp.app-context", {})
                region, bot = ctx.get("region"), ctx.get("botType")
            except ValueError:
                pass
        print(f"{path:55} http={r.status_code} len={len(text):>7} ids={len(ids):>3} scopes={scopes} region={region} bot={bot}")
        name = re.sub(r"[^a-z0-9]+", "_", path.lower()).strip("_") or "root"
        (out / f"tt_disc_{name}.txt").write_text(text[:2_000_000], encoding="utf-8")

    print("\nunique video ids found overall:", len(all_ids), sorted(all_ids)[:5])


if __name__ == "__main__":
    main()
