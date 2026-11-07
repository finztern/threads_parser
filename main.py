"""Entry point: loads config, builds shared TokenBucket/SeenCache and runs
the HTTP runner (rotating proxy pool, no accounts, no browser).
"""
from __future__ import annotations

import asyncio
import signal

from dotenv import load_dotenv

from scraper.config import ConfigStore
from pathlib import Path

from scraper.constants import BASE_DIR, CONFIG_PATH, LOGGER
from scraper.http_worker import http_runner
from scraper.state import SeenCache, TokenBucket


async def run() -> None:
    load_dotenv()

    config_store = ConfigStore(CONFIG_PATH)
    config_store.load()
    config_store.start_hot_reload()

    cfg = config_store.get()
    token_bucket = TokenBucket(rate_per_second=cfg["target_rate_per_second"])
    # Confirmed ids survive restarts (data/seen_cache.json), so a restart does not re-send what was already sent.
    seen_file = cfg.get("seen_cache_file", "data/seen_cache.json")
    seen_cache = SeenCache(
        max_size=cfg["seen_cache_size"],
        state_file=(Path(seen_file) if Path(seen_file).is_absolute() else BASE_DIR / seen_file) if seen_file else None,
    )
    seen_cache.load()

    async def seen_saver() -> None:
        while True:
            await asyncio.sleep(120)
            await seen_cache.save()

    saver = asyncio.create_task(seen_saver(), name="seen_saver")

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows fallback: no add_signal_handler support.
            pass

    LOGGER.info("Starting (http mode: proxy pool, no browser, no accounts)")
    task = asyncio.create_task(
        http_runner(
            config_store=config_store,
            token_bucket=token_bucket,
            seen_cache=seen_cache,
            stop_event=stop_event,
        ),
        name="http_runner",
    )

    await stop_event.wait()
    LOGGER.info("Shutdown signal received, stopping...")
    task.cancel()
    saver.cancel()
    await asyncio.gather(task, saver, return_exceptions=True)
    await seen_cache.save()


if __name__ == "__main__":
    asyncio.run(run())
