"""Do free public vless:// nodes open Threads, and how clean are they? (research helper, sends nothing.)

Downloads subscription lists (plain vless:// lines or base64), keeps the vless nodes, starts one Xray with a local
SOCKS5 inbound per node and fetches the Threads Replies tab of a profile through every node. Prints the shares of
real pages / 429 / login / timeouts and the latency, and can write the working links to a file (keep it out of git).

Usage inside the container (Xray is in the image):
  docker compose run --rm --no-deps parser python3 scripts/vpn_probe.py URL [URL ...] [--count 150] [--out /app/data/vpn_ok.txt]
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import random
import shutil
import statistics
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402
from curl_cffi import requests as cffi  # noqa: E402

from scraper.vpn import build_xray_config, parse_vless  # noqa: E402

THREADS_URL = "https://www.threads.com/@zuck/replies"
BASE_PORT = 12000


def decode_list(text: str) -> list[str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if any(l.startswith(("vless://", "vmess://", "trojan://", "ss://")) for l in lines):
        return lines
    try:  # base64 subscription
        raw = base64.b64decode(text.strip() + "=" * (-len(text.strip()) % 4)).decode("utf-8", "ignore")
        return [l.strip() for l in raw.splitlines() if l.strip()]
    except Exception:  # noqa: BLE001
        return []


async def download(urls: list[str]) -> list[str]:
    links: list[str] = []
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        for url in urls:
            try:
                response = await client.get(url)
                response.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                print(f"source failed: {url[:70]} ({str(exc)[:60]})")
                continue
            found = [l for l in decode_list(response.text) if l.startswith("vless://")]
            print(f"source ok: {url[:70]} -> {len(found)} vless links")
            links += found
    return list(dict.fromkeys(links))


async def probe(port: int, sem: asyncio.Semaphore) -> tuple[str, float]:
    async with sem:
        proxy = f"socks5h://127.0.0.1:{port}"
        started = time.monotonic()
        try:
            async with cffi.AsyncSession(impersonate="chrome124", timeout=20) as session:
                response = await session.get(THREADS_URL, proxies={"http": proxy, "https": proxy}, allow_redirects=True,
                                             headers={"Accept-Language": "en-US,en;q=0.9"})
            elapsed = time.monotonic() - started
            if "/login" in str(response.url):
                return "login", elapsed
            if response.status_code == 429:
                return "429", elapsed
            if response.status_code == 200 and b"data-sjs" in response.content:
                return "ok", elapsed
            return f"http_{response.status_code}", elapsed
        except Exception as exc:  # noqa: BLE001
            text = str(exc).lower()
            return ("timeout" if "timed out" in text or "timeout" in text else "conn_error"), time.monotonic() - started


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("urls", nargs="+")
    ap.add_argument("--count", type=int, default=150)
    ap.add_argument("--out", default="")
    args = ap.parse_args()
    xray = os.getenv("XRAY_BIN") or shutil.which("xray")
    if not xray:
        sys.exit("xray binary not found")
    links = await download(args.urls)
    random.shuffle(links)
    links = links[: args.count * 4]
    nodes, kept = [], []
    with tempfile.TemporaryDirectory() as tmp:
        # A single bad node makes Xray refuse the whole config: test every candidate on its own first.
        for link in links:
            if len(nodes) >= args.count:
                break
            try:
                node = parse_vless(link)
                one = Path(tmp) / "one.json"
                one.write_text(json.dumps(build_xray_config([node], BASE_PORT)), encoding="utf-8")
                check = await asyncio.create_subprocess_exec(
                    xray, "run", "-test", "-c", str(one),
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                if await check.wait() != 0:
                    continue
            except Exception:  # noqa: BLE001 - unsupported node: skip it
                continue
            nodes.append(node)
            kept.append(link)
        print(f"{len(nodes)} nodes accepted by xray (of {len(links)} links)")
        if not nodes:
            sys.exit("no usable vless nodes")
        path = Path(tmp) / "xray.json"
        path.write_text(json.dumps(build_xray_config(nodes, BASE_PORT)), encoding="utf-8")
        proc = await asyncio.create_subprocess_exec(xray, "run", "-c", str(path),
                                                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        await asyncio.sleep(3)
        if proc.returncode is not None:
            out = (await proc.stdout.read()).decode("utf-8", "ignore")[:600]
            sys.exit(f"xray exited ({proc.returncode}): {out}")
        try:
            sem = asyncio.Semaphore(20)
            results = await asyncio.gather(*(probe(BASE_PORT + i, sem) for i in range(len(nodes))))
        finally:
            proc.terminate()
    counts = Counter(kind for kind, _ in results)
    ok_times = sorted(t for kind, t in results if kind == "ok")
    print(f"\nprobed {len(results)} nodes via {THREADS_URL}")
    print(f"result: {dict(counts.most_common())}")
    if ok_times:
        print(f"ok latency: median {statistics.median(ok_times):.1f}s, p90 {ok_times[int(len(ok_times) * 0.9) - 1]:.1f}s")
    print(f"working share: {100.0 * counts['ok'] / len(results):.0f}%")
    if args.out:
        good = [kept[i] for i, (kind, _) in enumerate(results) if kind == "ok"]
        Path(args.out).write_text("\n".join(good) + "\n", encoding="utf-8")
        print(f"{len(good)} working links written to {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
