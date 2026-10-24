"""Entry point: loads config/accounts, builds shared TokenBucket/SeenCache,
spawns one account_worker per enabled account.
"""
from __future__ import annotations

import asyncio
import os
import signal

from dotenv import load_dotenv

from scraper.config import ConfigStore, load_accounts
from scraper.constants import ACCOUNTS_PATH, CONFIG_PATH, LOGGER
from scraper.state import SeenCache, TokenBucket
from scraper.http_worker import http_runner
from scraper.worker import account_worker


async def run() -> None:
    load_dotenv()

    config_store = ConfigStore(CONFIG_PATH)
    config_store.load()
    config_store.start_hot_reload()

    cfg = config_store.get()
    # "http": browserless, rotating proxy pool, no accounts (default).
    # "browser": Playwright + account login + per-account proxy.
    mode = os.getenv("MODE") or cfg.get("mode", "http")

    enabled_accounts = []
    if mode == "browser":
        accounts = load_accounts(ACCOUNTS_PATH)
        enabled_accounts = [acc for acc in accounts if acc.enabled]
        if not enabled_accounts:
            LOGGER.error(
                "No enabled accounts in %s - nothing to do. "
                "Set enabled: true for at least one account after adding its cookie file.",
                ACCOUNTS_PATH,
            )
            return
    token_bucket = TokenBucket(rate_per_second=cfg["target_rate_per_second"])
    seen_cache = SeenCache(max_size=cfg["seen_cache_size"])

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows fallback: no add_signal_handler support.
            pass

    if mode == "http":
        LOGGER.info("Starting in http mode (no browser, no accounts)")
        tasks = [
            asyncio.create_task(
                http_runner(
                    config_store=config_store,
                    token_bucket=token_bucket,
                    seen_cache=seen_cache,
                    stop_event=stop_event,
                ),
                name="http_runner",
            )
        ]
    else:
        LOGGER.info("Starting %d account worker(s)", len(enabled_accounts))
        tasks = [
            asyncio.create_task(
                account_worker(
                    account=account,
                    config_store=config_store,
                    token_bucket=token_bucket,
                    seen_cache=seen_cache,
                    stop_event=stop_event,
                ),
                name=f"worker:{account.name}",
            )
            for account in enabled_accounts
        ]

    await stop_event.wait()
    LOGGER.info("Shutdown signal received, stopping workers...")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(run())
