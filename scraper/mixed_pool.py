"""MixedPool: Tor circuits plus a list of static proxies, picked per request by share.

`static_share` of the requests go to the static proxies (proxies.txt), the rest to Tor. A blocked or dead
static proxy is cooled down by ProxyPool; if all of them cool down, the request falls back to Tor.
"""
from __future__ import annotations

import random

from scraper.proxies import ProxyPool
from scraper.tor_pool import TorPool


class MixedPool:
    rotating = True  # same as Tor: a blocked answer says nothing about the next request

    def __init__(self, tor: TorPool, static: ProxyPool | None, static_share: float = 0.4, cooldown: float = 120.0) -> None:
        self._tor = tor
        self._static = static
        self._cooldown = cooldown
        self._static_set: set[str] = set(static._proxies) if static else set()  # noqa: SLF001 - same package
        self._share = min(1.0, max(0.0, static_share))

    def __len__(self) -> int:
        return len(self._tor) + (len(self._static) if self._static else 0)

    def update_static(self, proxies: list[str]) -> None:
        """Replace the static list (harvester); cool-downs of proxies that stay are kept."""
        if not proxies:
            return
        fresh = ProxyPool(proxies, self._cooldown)
        if self._static:
            for proxy in fresh._proxies:  # noqa: SLF001
                until = self._static._banned_until.get(proxy)  # noqa: SLF001
                if until:
                    fresh._banned_until[proxy] = until  # noqa: SLF001
        self._static, self._static_set = fresh, set(fresh._proxies)  # noqa: SLF001

    def set_share(self, share: float) -> None:
        self._share = min(1.0, max(0.0, share))

    def next(self) -> str:
        if self._static and self._share > 0 and random.random() < self._share and self._static.available() > 0:
            return self._static.next()
        return self._tor.next()

    def next_retry(self) -> str:
        """A retry after a failed attempt: static proxies are much cleaner than Tor exits, so prefer them."""
        if self._static and self._static.available() > 0 and random.random() < max(self._share, 0.8):
            return self._static.next()
        return self._tor.next()

    def source(self, proxy: str) -> str:
        return "static" if proxy in self._static_set else "tor"

    def _owner(self, proxy: str):
        return self._static if proxy in self._static_set else self._tor

    def ban(self, proxy: str, seconds: float | None = None) -> None:
        if proxy in self._static_set:
            # A free proxy that timed out or got 429 is not worth retrying soon.
            seconds = max(600.0, seconds or 0.0)
        self._owner(proxy).ban(proxy, seconds)

    def ok(self, proxy: str) -> None:
        self._owner(proxy).ok(proxy)

    def available(self) -> int:
        return self._tor.available() + (self._static.available() if self._static else 0)
