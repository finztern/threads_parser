"""Which vless nodes does Reddit let through? Every node: ROUNDS rounds x profiles, one request at a time per node, GAP seconds between rounds.
Usage: python3 scripts/reddit_node_scan.py PORT_FIRST PORT_COUNT   (env: ROUNDS=3 GAP=6 PROFILES=chrome,safari URL=...)
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from collections import Counter

from curl_cffi.requests import AsyncSession

URL = os.environ.get("URL", "https://www.reddit.com/r/popular/comments/?limit=100")
ROUNDS = int(os.environ.get("ROUNDS", "3"))
GAP = float(os.environ.get("GAP", "6"))
PROFILES = [p for p in os.environ.get("PROFILES", "chrome,safari").split(",") if p]
HDRS = {"Accept-Language": "en-US,en;q=0.9"}


async def scan(port: int) -> dict:
    px = {"http": f"socks5h://127.0.0.1:{port}", "https": f"socks5h://127.0.0.1:{port}"}
    res = {"port": port, "ok": Counter(), "n": Counter(), "codes": Counter(), "pop": ""}
    async with AsyncSession(proxies=px) as s:
        for rnd in range(ROUNDS):
            for prof in PROFILES:
                res["n"][prof] += 1
                try:
                    r = await s.get(URL, impersonate=prof, headers=HDRS, timeout=20)
                    code = r.status_code
                    got = r.text.count('data-fullname="t1_')
                    m = re.search(r"p=([A-Z]{3})", r.headers.get("server-timing", "") + r.headers.get("x-reddit-ct", ""))
                    if m:
                        res["pop"] = m.group(1)
                except Exception as exc:  # noqa: BLE001
                    code, got = type(exc).__name__[:8], 0
                res["codes"][code] += 1
                if code == 200 and got >= 20:
                    res["ok"][prof] += 1
                await asyncio.sleep(1.0)
            await asyncio.sleep(GAP)
    return res


async def main() -> None:
    first, count = int(sys.argv[1]), int(sys.argv[2])
    out = await asyncio.gather(*(scan(first + i) for i in range(count)))
    print(f"{'node':>5} {'pop':<4}" + "".join(f"{p:>8}" for p in PROFILES) + "  codes")
    good = {p: 0 for p in PROFILES}
    anygood = 0
    for r in sorted(out, key=lambda x: -sum(x["ok"].values())):
        print(f"{r['port'] - first:>5} {r['pop']:<4}" + "".join(f"{r['ok'][p]}/{r['n'][p]:>6}" for p in PROFILES) + f"  {dict(r['codes'])}")
        for p in PROFILES:
            good[p] += r["ok"][p] > 0
        anygood += sum(r["ok"].values()) > 0
    print(f"\nnodes with at least one OK page: {anygood}/{count}; per profile: {good}")
    gf = os.environ.get("GOOD_FILE")
    if gf:
        with open(gf, "w") as fh:
            fh.write(",".join(str(r["port"]) for r in out if sum(r["ok"].values()) > 0))
    full = sum(1 for r in out if all(r["ok"][p] == r["n"][p] for p in PROFILES))
    print(f"nodes OK in every single request: {full}/{count}")


if __name__ == "__main__":
    asyncio.run(main())
