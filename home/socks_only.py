"""A local SOCKS5 proxy from ONE vless:// link (Xray inside this container, loopback only). No tunnels, no collector.

Used for things that must leave through a vless exit from the home PC, e.g. the Exorde url-check of the Reddit test:
  curl_cffi ... proxies={"https": "socks5h://127.0.0.1:10809"}
The link (it holds a user id) is read from a file that stays out of git (home/vless_urlcheck.txt).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import time
from pathlib import Path

from scraper.vpn import build_xray_config, load_nodes

LOG = logging.getLogger("socks")


def _env(name: str, default: str) -> str:
    return (os.getenv(name) or "").strip() or default


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    port = int(_env("SOCKS_PORT", "10809"))
    nodes = load_nodes(Path(_env("SOCKS_VLESS_FILE", "/data/vless.txt")))
    if not nodes:
        raise SystemExit("no usable vless:// link in the vless file")
    config = Path("/tmp/xray_socks.json")
    config.write_text(json.dumps(build_xray_config(nodes[:1], port)), encoding="utf-8")
    os.chmod(config, 0o600)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    delay = 3.0
    while not stop.is_set():
        proc = await asyncio.create_subprocess_exec("xray", "run", "-c", str(config))
        LOG.info("xray started: socks5 on 127.0.0.1:%d (%s)", port, nodes[0].label)
        started = time.monotonic()
        while not stop.is_set() and proc.returncode is None:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(proc.wait(), timeout=2.0)
        if stop.is_set():
            with contextlib.suppress(ProcessLookupError):
                proc.terminate()
            return
        delay = 3.0 if time.monotonic() - started > 60 else min(delay * 2, 60.0)
        LOG.warning("xray exited with code %s, restarting in %.0fs", proc.returncode, delay)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=delay)


if __name__ == "__main__":
    asyncio.run(main())
