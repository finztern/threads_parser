"""TorPool: many local Tor instances (docker service `tor`), a fresh circuit for every request.

Tor opens a separate circuit (= usually a different exit IP) for every distinct SOCKS username
(IsolateSOCKSAuth, on by default). So instead of the NEWNYM signal (limited to one per 10 s per
instance) next() returns a URL with a random user name: every request gets its own circuit.
The instance itself is banned only after a long run of failures (the instance is broken); a blocked
exit IP is simply not used again.
"""
from __future__ import annotations

import secrets
import time

from scraper.constants import LOGGER

_FAIL_STREAK_LIMIT = 15  # consecutive failed requests that mark one instance as broken
_BAN_SECONDS = 60.0


class TorPool:
    rotating = True

    def __init__(self, host: str, base_port: int, instances: int, circuit_reuse: int = 1) -> None:
        """`host` may list several machines, comma separated, each optionally with its own instance
        count: "host.docker.internal=20,1.2.3.4=70" (without "=N" the default `instances` is used).
        Machine k runs Tor clients on base_port .. base_port+count-1."""
        if instances < 1:
            raise ValueError("tor instances must be >= 1")
        counts: dict[str, int] = {}
        for item in host.split(","):
            name, _, count = item.strip().partition("=")
            if name:
                counts[name] = int(count) if count else instances
        if not counts or min(counts.values()) < 1:
            raise ValueError("tor host list is empty or has a zero instance count")
        # Interleave the machines so consecutive requests alternate between them.
        self._endpoints = [
            f"{name}:{base_port + i}" for i in range(max(counts.values())) for name, n in counts.items() if i < n
        ]
        # Building a circuit is the most expensive thing Tor does: reuse one for `circuit_reuse`
        # requests of an instance before switching to a new SOCKS user name (= new circuit).
        self._reuse = max(1, int(circuit_reuse))
        self._user: dict[str, list] = {}  # endpoint -> [user name, requests left on it]
        self._idx = 0
        self._streak: dict[str, int] = {}
        self._banned_until: dict[str, float] = {}

    def __len__(self) -> int:
        return len(self._endpoints)

    def next(self) -> str:
        now = time.monotonic()
        n = len(self._endpoints)
        for _ in range(n):
            endpoint = self._endpoints[self._idx % n]
            self._idx += 1
            if self._banned_until.get(endpoint, 0.0) <= now:
                break
        else:
            endpoint = self._endpoints[self._idx % n]
            self._idx += 1
        # socks5h: the exit node resolves the host name (no DNS leak, works for any exit).
        slot = self._user.get(endpoint)
        if slot is None or slot[1] <= 0:
            slot = self._user[endpoint] = [secrets.token_hex(6), self._reuse]
        slot[1] -= 1
        return f"socks5h://{slot[0]}:x@{endpoint}"

    @staticmethod
    def _endpoint(proxy: str) -> str:
        return proxy.rsplit("@", 1)[-1]

    def ban(self, proxy: str, seconds: float | None = None) -> None:
        endpoint = self._endpoint(proxy)
        streak = self._streak.get(endpoint, 0) + 1
        self._streak[endpoint] = streak
        if streak >= _FAIL_STREAK_LIMIT:
            self._streak[endpoint] = 0
            self._banned_until[endpoint] = time.monotonic() + _BAN_SECONDS
            LOGGER.warning("tor instance %s failed %d requests in a row, pausing it %.0fs", endpoint, streak, _BAN_SECONDS)

    def ok(self, proxy: str) -> None:
        self._streak[self._endpoint(proxy)] = 0

    def available(self) -> int:
        now = time.monotonic()
        return sum(1 for e in self._endpoints if self._banned_until.get(e, 0.0) <= now)
