"""Does rotating the source address inside an IPv6 /64 work, and does Threads accept it?

Run on the VPS HOST network (so the kernel routing table is the host's), e.g.:
    sudo ip -6 route add local 2a01:4f9:aaaa:bbbb::/64 dev lo
    sudo sysctl -w net.ipv6.ip_nonlocal_bind=1
    docker compose run --rm --no-deps --network host parser \
        python scripts/ipv6_probe.py 2a01:4f9:aaaa:bbbb --count 300 --concurrency 30

Steps: (1) sanity - the address a public echo service sees must equal the random source address;
(2) Threads profile fetched from many random addresses vs. from the machine's normal address:
status codes, share of real pages (data-sjs), latency.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time
from collections import Counter

from curl_cffi.requests import AsyncSession

ECHO_URL = "https://ipv6.icanhazip.com"
THREADS_URL = "https://www.threads.com/@zuck"
IPRESOLVE_V6 = 2  # CURL_IPRESOLVE_V6


def random_address(prefix: str) -> str:
    """prefix = the first 4 groups of the /64 (e.g. 2a01:4f9:aaaa:bbbb); the last 64 bits are random."""
    groups = [f"{random.getrandbits(16):x}" for _ in range(4)]
    return f"{prefix}:{':'.join(groups)}"


async def fetch(url: str, source: str | None, timeout: float) -> tuple[int, int, float, str]:
    """(status, has_data_sjs, seconds, text start); status 0 = transport error."""
    t0 = time.monotonic()
    kwargs: dict = {"timeout": timeout, "impersonate": "chrome124"}
    if source:
        kwargs["interface"] = source
        kwargs["curl_options"] = {_resolve_opt(): IPRESOLVE_V6}
    try:
        async with AsyncSession(**kwargs) as session:
            response = await session.get(url, headers={"Accept-Language": "en-US,en;q=0.9"})
        return response.status_code, int("data-sjs" in response.text), time.monotonic() - t0, response.text[:80].strip()
    except Exception as exc:  # noqa: BLE001 - a probe: report anything
        return 0, 0, time.monotonic() - t0, str(exc)[:80]


def _resolve_opt():
    from curl_cffi.const import CurlOpt

    return CurlOpt.IPRESOLVE


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("prefix", help="first 4 groups of your /64, e.g. 2a01:4f9:aaaa:bbbb")
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--url", default=THREADS_URL)
    args = ap.parse_args()

    print("1) sanity: does the echo service see the random source address?")
    for _ in range(3):
        addr = random_address(args.prefix)
        status, _, secs, text = await fetch(ECHO_URL, addr, args.timeout)
        print(f"   bound {addr} -> status {status} in {secs:.1f}s, echo: {text}")

    async def run(label: str, make_source) -> None:
        sem = asyncio.Semaphore(args.concurrency)
        results: list[tuple[int, int, float, str]] = []

        async def one() -> None:
            async with sem:
                results.append(await fetch(args.url, make_source(), args.timeout))

        t0 = time.monotonic()
        await asyncio.gather(*(one() for _ in range(args.count)))
        wall = time.monotonic() - t0
        codes = Counter(r[0] for r in results)
        good = sum(r[1] for r in results)
        lat = sorted(r[2] for r in results if r[0] == 200)
        print(f"\n{label}: {args.count} requests in {wall:.0f}s = {args.count / wall:.1f} req/s")
        print(f"   status codes: {dict(codes)}   real pages (data-sjs): {good}/{args.count}")
        if lat:
            print(f"   latency of 200s: median {lat[len(lat) // 2]:.1f}s, p90 {lat[int(len(lat) * 0.9)]:.1f}s")
        errors = Counter(r[3] for r in results if r[0] == 0)
        if errors:
            print("   transport errors:", dict(errors.most_common(3)))

    print("\n2) Threads from random source addresses vs. the normal address")
    await run("random /64 addresses", lambda: random_address(args.prefix))
    await run("normal address (IPv4/IPv6 as the machine chooses)", lambda: None)


if __name__ == "__main__":
    asyncio.run(main())
