"""Probe free public HTTP/SOCKS proxies against YouTube (research helper, sends nothing to the collector).

Downloads the proxy lists (default: proxy_sources of config.yaml), tests a random sample with one real
youtubei/v1/next request each and reports the pass rate, latency and the speed of the ones that pass.
Usage: python3 scripts/yt_proxy_probe.py [candidates=400] [concurrency=100] [timeout=10] [out_file=yt_good_proxies.txt]
Needs curl_cffi and pyyaml (pip install curl_cffi pyyaml). Run it on the machine that will run the parser.
"""
from __future__ import annotations

import asyncio
import random
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

import yaml
from curl_cffi.requests import AsyncSession

WEB_KEY = "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8"  # public key of the web client
WEB_VERSION = "2.20241120.01.00"
NEXT_URL = f"https://www.youtube.com/youtubei/v1/next?key={WEB_KEY}&prettyPrint=false"
HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Cookie": "SOCS=CAI; CONSENT=PENDING+987",
    "Origin": "https://www.youtube.com",
    "X-Youtube-Client-Name": "1",
    "X-Youtube-Client-Version": WEB_VERSION,
}
BODY = {"context": {"client": {"clientName": "WEB", "clientVersion": WEB_VERSION, "hl": "en", "gl": "US"}}, "videoId": "dQw4w9WgXcQ"}


def sources() -> list[tuple[str, str]]:
    cfg = yaml.safe_load((Path(__file__).resolve().parent.parent / "config.yaml").read_text(encoding="utf-8")) or {}
    out = []
    for item in cfg.get("proxy_sources") or []:
        scheme, _, url = str(item).partition("|")
        if scheme and url:
            out.append((scheme.strip(), url.strip()))
    return out


async def download(session: AsyncSession, scheme: str, url: str) -> list[str]:
    try:
        r = await session.get(url, timeout=30)
        lines = [ln.strip() for ln in r.text.splitlines() if ln.strip() and not ln.startswith("#")]
        return [f"{scheme}://{ln.split('://')[-1]}" for ln in lines if ":" in ln]
    except Exception as exc:  # a dead list must not stop the probe
        print(f"list failed {url}: {type(exc).__name__}")
        return []


async def probe(proxy: str, sem: asyncio.Semaphore, timeout: float) -> tuple[str, str, float]:
    async with sem:
        start = time.monotonic()
        try:
            async with AsyncSession(impersonate="chrome", timeout=timeout, proxies={"http": proxy, "https": proxy}) as s:
                r = await s.post(NEXT_URL, json=BODY, headers=HEADERS)
            took = time.monotonic() - start
            if r.status_code == 200 and "engagementPanels" in r.text:
                return proxy, "ok", took
            return proxy, f"http_{r.status_code}" if r.status_code != 200 else "bad_body", took
        except Exception as exc:
            return proxy, type(exc).__name__, time.monotonic() - start


async def main() -> None:
    candidates = int(sys.argv[1]) if len(sys.argv) > 1 else 400
    conc = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    timeout = float(sys.argv[3]) if len(sys.argv) > 3 else 10.0
    out_file = sys.argv[4] if len(sys.argv) > 4 else "yt_good_proxies.txt"
    async with AsyncSession(impersonate="chrome") as s:
        lists = await asyncio.gather(*(download(s, sc, u) for sc, u in sources()))
    pool = sorted({p for lst in lists for p in lst})
    random.shuffle(pool)
    pool = pool[:candidates]
    print(f"proxies in lists: {sum(len(x) for x in lists)} (unique sampled: {len(pool)})")
    sem = asyncio.Semaphore(conc)
    results = await asyncio.gather(*(probe(p, sem, timeout) for p in pool))
    kinds = Counter(r[1] for r in results)
    ok = [r for r in results if r[1] == "ok"]
    print("results:", dict(kinds.most_common()))
    by_proto = Counter(r[0].split("://")[0] for r in ok)
    print(f"ok {len(ok)}/{len(results)} = {100 * len(ok) / max(len(results), 1):.1f}% | by protocol: {dict(by_proto)}")
    if ok:
        lat = sorted(r[2] for r in ok)
        print(f"latency of ok: median {statistics.median(lat):.1f}s, p90 {lat[int(len(lat) * 0.9) - 1]:.1f}s")
        Path(out_file).write_text("\n".join(r[0] for r in ok) + "\n", encoding="utf-8")
        print(f"good proxies written to {out_file}")


if __name__ == "__main__":
    asyncio.run(main())
