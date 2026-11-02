"""Ipv6Pool: every request leaves from a random address of your own IPv6 /64 (no proxy at all).

Needs the host to accept the whole prefix (see scripts/ipv6_probe.py and README): the "local" route
on lo, net.ipv6.ip_nonlocal_bind=1, and ndppd when the provider answers NDP per address. The parser
must run on the host network (docker-compose.ipv6.yml).

next() returns "ipv6://<address>"; scraper.proxies.transport_options turns it into the curl options
(bind to the address, resolve IPv6 only). Several prefixes (several machines/subnets) may be given.
"""
from __future__ import annotations

import random

from scraper.constants import LOGGER


class Ipv6Pool:
    # Unlike Tor, a block answer may mean "the whole /64 is limited": the autotune must see it.
    rotating = False

    def __init__(self, prefixes: str) -> None:
        self._prefixes = [p.strip().rstrip(":") for p in prefixes.split(",") if p.strip()]
        if not self._prefixes:
            raise ValueError("no IPv6 prefix given")
        for prefix in self._prefixes:
            if len(prefix.split(":")) != 4:
                raise ValueError(f"IPv6 prefix must be the first 4 groups of the /64, e.g. 2a01:ecc0:6000:1b6, got {prefix!r}")
        LOGGER.info("ipv6 pool: %d prefix(es), a random source address per request", len(self._prefixes))

    def __len__(self) -> int:
        return 1

    def next(self) -> str:
        prefix = random.choice(self._prefixes)
        return "ipv6://" + prefix + ":" + ":".join(f"{random.getrandbits(16):x}" for _ in range(4))

    def ban(self, proxy: str, seconds: float | None = None) -> None:
        """Nothing to ban: the next request simply uses another address."""

    def ok(self, proxy: str) -> None:
        pass

    def available(self) -> int:
        return 1
