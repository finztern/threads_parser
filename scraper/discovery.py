"""TargetRegistry: self-growing list of profiles to scrape.

Seeds come from config.yaml / targets.txt and are always scraped. Every
other username seen as an author of a scraped post/reply is added as a
discovered target. Scheduling is adaptive per target:

- gave fresh own posts  -> re-check soon (min_interval)
- gave nothing          -> pause doubles, up to max_interval
- yield (posts that were NEW to Exorde) is tracked per target as a moving average: the most
  productive profiles are scraped first, and a scrape that produced nothing new backs off
- 404 / not found       -> dropped for good ("dead")
- transient failure     -> retried shortly, interval unchanged

State is persisted to a JSON file so restarts keep the list.
"""
from __future__ import annotations

import json
import os
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path

from scraper.constants import LOGGER

# Threads usernames: letters, digits, '.', '_' (up to 30 chars).
USERNAME_RE = re.compile(r"^[A-Za-z0-9._]{1,30}$")
RETRY_AFTER_FAILURE_SECONDS = 60.0


@dataclass(slots=True)
class TargetState:
    interval: float
    next_due: float
    seed: bool = False
    scrapes: int = 0
    fresh_total: int = 0
    yield_ema: float = 0.0  # moving average of items new to Exorde per scrape


class TargetRegistry:
    def __init__(
        self,
        state_file: Path | None,
        min_interval: float = 300.0,
        max_interval: float = 21600.0,
        seed_interval: float = 30.0,
        max_known: int = 200_000,
    ) -> None:
        self._file = state_file
        self._min = min_interval
        self._max = max_interval
        self._seed_interval = seed_interval
        self._max_known = max_known
        self._targets: dict[str, TargetState] = {}
        self._dead: set[str] = set()
        self._last_saved = 0.0
        self._dirty = False

    # ---- persistence -------------------------------------------------
    def load(self) -> None:
        if not self._file or not self._file.is_file():
            return
        try:
            raw = json.loads(self._file.read_text(encoding="utf-8"))
            for name, v in raw.get("targets", {}).items():
                self._targets[name] = TargetState(
                    interval=float(v[0]),
                    next_due=float(v[1]),
                    seed=False,
                    scrapes=int(v[2]),
                    fresh_total=int(v[3]),
                    yield_ema=float(v[4]) if len(v) > 4 else 0.0,
                )
            self._dead = set(raw.get("dead", []))
            LOGGER.info("discovery: loaded %d targets, %d dead from %s", len(self._targets), len(self._dead), self._file)
        except (OSError, ValueError, IndexError, TypeError):
            LOGGER.exception("discovery: could not read %s, starting empty", self._file)

    def save(self, force: bool = False) -> None:
        if not self._file or not self._dirty:
            return
        if not force and time.monotonic() - self._last_saved < 60:
            return
        data = {
            "targets": {
                n: [round(t.interval, 1), round(t.next_due, 1), t.scrapes, t.fresh_total, round(t.yield_ema, 2)]
                for n, t in self._targets.items()
                if not t.seed
            },
            "dead": sorted(self._dead),
        }
        try:
            self._file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._file.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
            os.replace(tmp, self._file)
            self._dirty = False
            self._last_saved = time.monotonic()
        except OSError:
            LOGGER.exception("discovery: could not write %s", self._file)

    # ---- membership ----------------------------------------------------
    def sync_seeds(self, names: list[str]) -> None:
        wanted = {n.lower(): n for n in names if USERNAME_RE.match(n)}
        now = time.time()
        for key, name in wanted.items():
            state = self._targets.get(key)
            if state is None:
                self._targets[key] = TargetState(interval=self._seed_interval, next_due=now, seed=True)
            else:
                state.seed = True
                state.interval = self._seed_interval
            self._dead.discard(key)
        for key, state in self._targets.items():
            if state.seed and key not in wanted:
                state.seed = False
                state.interval = self._min

    def observe(self, usernames: set[str]) -> int:
        """Add unknown usernames as discovered targets. Returns how many were new."""
        now = time.time()
        added = 0
        for name in usernames:
            key = name.lower()
            if key in self._targets or key in self._dead or not USERNAME_RE.match(name):
                continue
            if len(self._targets) >= self._max_known:
                self._evict()
                if len(self._targets) >= self._max_known:
                    break
            # Small random delay spreads new targets over time.
            self._targets[key] = TargetState(interval=self._min, next_due=now + random.uniform(0, min(30.0, self._min)))
            added += 1
        if added:
            self._dirty = True
        return added

    def _evict(self) -> None:
        """Drop the ~5% idlest non-seed targets (longest pause, already scraped)."""
        candidates = [(k, t) for k, t in self._targets.items() if not t.seed and t.scrapes > 0]
        candidates.sort(key=lambda kt: (-kt[1].interval, kt[1].fresh_total))
        for key, _ in candidates[: max(1, self._max_known // 20)]:
            del self._targets[key]
        self._dirty = True

    # ---- scheduling ----------------------------------------------------
    def due(self, limit: int, explore_ratio: float = 0.3) -> list[str]:
        """Targets to scrape now: seeds first, then mostly profiles known to
        post fresh content (they pay off), with `explore_ratio` of the batch
        kept for never-scraped / unproven ones so the list keeps growing.
        """
        now = time.time()
        seeds: list[tuple[str, TargetState]] = []
        proven: list[tuple[str, TargetState]] = []
        unproven: list[tuple[str, TargetState]] = []
        for k, t in self._targets.items():
            if t.next_due > now:
                continue
            (seeds if t.seed else proven if t.fresh_total > 0 else unproven).append((k, t))
        for group in (seeds, unproven):
            group.sort(key=lambda kt: kt[1].next_due)
        # Proven profiles: the ones that recently produced the most NEW items go first.
        proven.sort(key=lambda kt: (-kt[1].yield_ema, kt[1].next_due))

        batch = [k for k, _ in seeds[:limit]]
        room = limit - len(batch)
        if room <= 0:
            return batch
        explore_quota = int(room * explore_ratio)
        take_proven = proven[: room - min(explore_quota, len(unproven))]
        batch += [k for k, _ in take_proven]
        room = limit - len(batch)
        batch += [k for k, _ in unproven[:room]]
        room = limit - len(batch)
        if room > 0:  # not enough unproven: fill with more proven
            batch += [k for k, _ in proven[len(take_proven) : len(take_proven) + room]]
        return batch

    def count_due(self) -> int:
        now = time.time()
        return sum(1 for t in self._targets.values() if t.next_due <= now)

    def seconds_until_next_due(self) -> float:
        if not self._targets:
            return 30.0
        return max(0.0, min(t.next_due for t in self._targets.values()) - time.time())

    def report(self, name: str, label: str, fresh_own_posts: int) -> None:
        """Update scheduling after scraping `name`. label: ok/empty/not_found/failed."""
        key = name.lower()
        state = self._targets.get(key)
        if state is None:
            return
        now = time.time()
        self._dirty = True

        if label == "not_found":
            if state.seed:
                LOGGER.warning("seed @%s not found (404) - check the username", name)
            del self._targets[key]
            self._dead.add(key)
            return
        if label == "failed":
            state.next_due = now + RETRY_AFTER_FAILURE_SECONDS
            return

        state.scrapes += 1
        state.fresh_total += fresh_own_posts
        if state.seed:
            state.interval = self._seed_interval
        elif fresh_own_posts == 0:
            state.interval = min(self._max, state.interval * 2)
        # fresh own posts: the interval is left as it is; report_yield() sets it once we know
        # how much of it was new to Exorde.
        state.next_due = now + state.interval * random.uniform(0.8, 1.2)

    def report_yield(self, name: str, new_items: int) -> None:
        """Called after the dispatch of a scrape of `name` with the number of items that were NEW
        to Exorde (passed the url check). Productive -> poll again at min_interval and rank higher;
        nothing new -> the pause doubles, even if the author posts a lot (Exorde already has it).
        """
        state = self._targets.get(name.lower())
        if state is None or state.seed:
            return
        state.yield_ema = 0.6 * state.yield_ema + 0.4 * new_items
        if new_items > 0:
            state.interval = self._min
        else:
            state.interval = min(self._max, max(state.interval, self._min) * 2)
        state.next_due = time.time() + state.interval * random.uniform(0.8, 1.2)
        self._dirty = True

    # ---- stats ---------------------------------------------------------
    def stats(self) -> dict[str, int]:
        return {
            "known": len(self._targets),
            "seeds": sum(1 for t in self._targets.values() if t.seed),
            "dead": len(self._dead),
            "due": self.count_due(),
            "active": sum(1 for t in self._targets.values() if t.fresh_total > 0),
        }
