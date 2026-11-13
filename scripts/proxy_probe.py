"""Probe a list of proxies against the Threads Replies tab (research helper, sends nothing to the collector).

For every proxy it makes one request to https://www.threads.com/@USER/replies and classifies the result:
ok (HTML with data-sjs), login wall, 429, other HTTP status, timeout/connect error. Prints a summary
per protocol so you can see whether the list is better than Tor.

Usage (inside the container; the list file must NOT be committed):
  docker compose cp ~/proxies.txt parser:/tmp/proxies.txt
  docker compose exec parser python3 scripts/proxy_probe.py /tmp/proxies.txt [max_proxies=200] [user=zuck]
Lines: scheme://host:port or scheme://user:pass@host:port (http, socks4, socks5); '#' comments allowed.
"""
from __future__ import annotations

import asyncio
import sys
import time
from collections import Counter
from pathlib import Path

from curl_cffi import requests as cffi

HEADERS = {"Accept-Language": "en-US,en;q=0.9"}


def load(path: str, limit: int) -> list[str]:
    out: list[str] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or line == "...":
            continue
        if "://" not in line:
            line = "http://" + line
        out.append(line)
    return out[:limit]


async def probe(proxy: str, url: str, sem: asyncio.Semaphore) -> tuple[str, str, float]:
    async with sem:
        started = time.monotonic()
        try:
            async with cffi.AsyncSession(impersonate="chrome124", timeout=20) as session:
                response = await session.get(url, headers=HEADERS, allow_redirects=True, proxies={"http": proxy, "https": proxy})
            if "/login" in str(response.url):
                kind = "login"
            elif response.status_code == 200 and "data-sjs" in response.text:
                kind = "ok"
            else:
                kind = f"http_{response.status_code}"
        except Exception as exc:  # noqa: BLE001 - a probe: classify and go on
            text = str(exc).lower()
            kind = "timeout" if "timed out" in text or "timeout" in text else "conn_error"
        return proxy, kind, time.monotonic() - started


async def main(path: str, limit: int, user: str) -> None:
    proxies = load(path, limit)
    url = f"https://www.threads.com/@{user}/replies"
    sem = asyncio.Semaphore(30)
    results = await asyncio.gather(*(probe(p, url, sem) for p in proxies))
    by_scheme: dict[str, Counter] = {}
    for proxy, kind, _ in results:
        by_scheme.setdefault(proxy.split("://", 1)[0], Counter())[kind] += 1
    total = Counter(kind for _, kind, _ in results)
    print(f"probed {len(results)} proxies via {url}")
    for scheme, counter in sorted(by_scheme.items()):
        print(f"  {scheme}: {dict(counter.most_common())}")
    ok_times = sorted(t for _, kind, t in results if kind == "ok")
    print(f"TOTAL: {dict(total.most_common())}")
    if ok_times:
        print(f"ok latency: median {ok_times[len(ok_times) // 2]:.1f}s, max {ok_times[-1]:.1f}s")
    print(f"working share: {100.0 * total['ok'] / max(len(results), 1):.0f}%")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    asyncio.run(main(sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 200, sys.argv[3] if len(sys.argv) > 3 else "zuck"))
