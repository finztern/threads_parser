"""Shared state across all account workers: TokenBucket (rate limit) and
SeenCache (dedup with claim/confirm/release, so an item that fails to
send is not lost - it becomes claimable again).
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from collections import OrderedDict
from pathlib import Path

from scraper.constants import LOGGER


class TokenBucket:
    """Async token bucket shared by all accounts. `rate_per_second` is the
    total budget for the whole process, not per account.
    """

    def __init__(self, rate_per_second: float, burst: float | None = None) -> None:
        self._rate = max(rate_per_second, 0.01)
        self._capacity = burst if burst is not None else max(self._rate * 2, 1.0)
        self._tokens = self._capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def set_rate(self, rate_per_second: float) -> None:
        self._rate = max(rate_per_second, 0.01)

    async def acquire(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            await asyncio.sleep(min(0.1, remaining))

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._last_refill = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)


class SeenCache:
    """Bounded dedup cache shared by all accounts, keyed by external_id.

    States:
    - not present: never seen, claimable.
    - "pending": claimed by some worker, currently being sent.
    - "confirmed": server confirmed storage, permanently a duplicate
      (until evicted by size limit).
    """

    def __init__(self, max_size: int, state_file: "Path | None" = None) -> None:
        self._max_size = max_size
        self._store: OrderedDict[str, str] = OrderedDict()
        self._lock = asyncio.Lock()
        self._file = state_file
        self._dirty = False

    # ---- persistence: a restart must not re-send everything that was already confirmed ----------------
    def load(self) -> None:
        if not self._file or not self._file.is_file():
            return
        try:
            ids = json.loads(self._file.read_text(encoding="utf-8"))
            for external_id in ids[-self._max_size :]:
                self._store[str(external_id)] = "confirmed"
            LOGGER.info("seen cache: loaded %d confirmed ids from %s", len(self._store), self._file)
        except (OSError, ValueError, TypeError):
            LOGGER.exception("seen cache: could not read %s, starting empty", self._file)

    async def save(self) -> None:
        """Write the confirmed ids (oldest first). The snapshot is taken in the loop, the dump in a thread."""
        if not self._file or not self._dirty:
            return
        ids = [k for k, v in self._store.items() if v == "confirmed"]
        self._dirty = False
        file = self._file

        def write() -> None:
            file.parent.mkdir(parents=True, exist_ok=True)
            tmp = file.with_suffix(".tmp")
            tmp.write_text(json.dumps(ids, separators=(",", ":")), encoding="utf-8")
            os.replace(tmp, file)

        try:
            await asyncio.to_thread(write)
        except OSError:
            self._dirty = True
            LOGGER.exception("seen cache: could not write %s", file)

    async def try_claim(self, external_id: str) -> bool:
        async with self._lock:
            if external_id in self._store:
                return False
            self._store[external_id] = "pending"
            self._store.move_to_end(external_id)
            self._evict_if_needed()
            return True

    async def confirm(self, external_id: str) -> None:
        async with self._lock:
            self._store[external_id] = "confirmed"
            self._store.move_to_end(external_id)
            self._dirty = True

    async def release(self, external_id: str) -> None:
        """Undo a claim after a failed send, so it can be retried next cycle."""
        async with self._lock:
            if self._store.get(external_id) == "pending":
                del self._store[external_id]

    def _evict_if_needed(self) -> None:
        while len(self._store) > self._max_size:
            self._store.popitem(last=False)
