"""HotPosts: which post pages are worth opening again, judged by what they really gave us.

A profile page only tells which posts exist and how many replies they have; the replies come from
the post page. Instead of opening the pages of every profile's posts, the profile fetch only
*observes* posts (see observe) and this registry decides which post pages to open, directly
(no profile request in between):

- a new post (or one whose reply count grew by >= min_growth since we last opened it) is due at once;
- every opened page is scored by the number of replies that were NEW to Exorde (report); a post that
  keeps giving new replies is re-opened sooner, one that gives nothing backs off exponentially and
  is dropped after `max_misses` empty fetches in a row;
- due() hands out the posts with the best score first, so requests go where the data is.

State is a small JSON file so restarts do not start from zero.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from scraper.constants import LOGGER


class _Post:
    __slots__ = ("username", "code", "created", "seen", "fetched", "gain", "misses", "next_due")

    def __init__(self, username: str, code: str, created: float, seen: int, fetched: int, gain: float,
                 misses: int, next_due: float) -> None:
        self.username = username
        self.code = code
        self.created = created
        self.seen = seen  # reply count at the last profile observation
        self.fetched = fetched  # reply count when the post page was last opened
        self.gain = gain  # smoothed number of new replies per opened page
        self.misses = misses  # consecutive opened pages without a new reply
        self.next_due = next_due


class HotPosts:
    def __init__(
        self,
        state_file: Path | None,
        max_age: float = 3 * 86400,
        min_growth: int = 4,
        base_interval: float = 300.0,
        min_interval: float = 90.0,
        max_interval: float = 6 * 3600.0,
        max_misses: int = 5,
        max_posts: int = 100_000,
    ) -> None:
        self._file = state_file
        self._max_age = max_age
        self._min_growth = max(1, min_growth)
        self._base = base_interval
        self._min = min_interval
        self._max = max_interval
        self._max_misses = max_misses
        self._max_posts = max_posts
        self._posts: dict[str, _Post] = {}  # key: post pk
        self._dirty = False
        self._last_saved = 0.0

    def __len__(self) -> int:
        return len(self._posts)

    # ---- persistence ---------------------------------------------------------------------------
    def load(self) -> None:
        if not self._file or not self._file.is_file():
            return
        try:
            raw: dict[str, Any] = json.loads(self._file.read_text(encoding="utf-8"))
            for pk, v in raw.get("posts", {}).items():
                self._posts[pk] = _Post(v[0], v[1], float(v[2]), int(v[3]), int(v[4]), float(v[5]), int(v[6]), float(v[7]))
            LOGGER.info("hot posts: loaded %d posts from %s", len(self._posts), self._file)
        except (OSError, ValueError, IndexError, TypeError):
            LOGGER.exception("hot posts: could not read %s, starting empty", self._file)

    def save(self, force: bool = False) -> None:
        if not self._file or not self._dirty:
            return
        if not force and time.monotonic() - self._last_saved < 120:
            return
        data = {
            "posts": {
                pk: [p.username, p.code, round(p.created), p.seen, p.fetched, round(p.gain, 2), p.misses, round(p.next_due)]
                for pk, p in self._posts.items()
            }
        }
        try:
            self._file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._file.with_suffix(".tmp")
            tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
            os.replace(tmp, self._file)
            self._dirty = False
            self._last_saved = time.monotonic()
        except OSError:
            LOGGER.exception("hot posts: could not write %s", self._file)

    # ---- input from profile pages --------------------------------------------------------------
    def observe(self, posts: list, now: float | None = None) -> int:
        """Register top-level posts seen on a profile page. Returns how many are new here."""
        now = time.time() if now is None else now
        added = 0
        for post in posts:
            count = int((post.extra or {}).get("direct_reply_count") or 0)
            created = post.created_at.timestamp()
            if count <= 0 or now - created > self._max_age:
                continue
            known = self._posts.get(post.external_id)
            if known is None:
                code = post.url.rsplit("/post/", 1)[-1]
                # Optimistic prior until the first real fetch: a post with many replies is promising,
                # and a very fresh one is more likely not yet known to Exorde.
                age = now - created
                freshness = 2.0 if age < 1800 else 1.0 if age < 7200 else 0.5
                self._posts[post.external_id] = _Post(
                    post.username, code, created, count, 0, min(count, 10) * 0.3 * freshness, 0, now
                )
                added += 1
                self._dirty = True
                continue
            if count != known.seen:
                known.seen = count
                self._dirty = True
            if count - known.fetched >= self._min_growth and known.next_due > now:
                known.next_due = now  # enough new replies piled up: due immediately
        return added

    # ---- scheduling ------------------------------------------------------------------------------
    def due(self, limit: int, now: float | None = None) -> list[tuple[str, str, str]]:
        """(username, code, pk) of the posts to open now, best expected yield first."""
        now = time.time() if now is None else now
        eligible = [
            (max(p.gain, 0.5 * (p.seen - p.fetched)), pk, p)
            for pk, p in self._posts.items()
            if p.next_due <= now and p.seen - p.fetched >= 0
        ]
        eligible.sort(key=lambda e: -e[0])
        return [(p.username, p.code, pk) for _, pk, p in eligible[: max(0, limit)]]

    def count_due(self, now: float | None = None) -> int:
        now = time.time() if now is None else now
        return sum(1 for p in self._posts.values() if p.next_due <= now)

    def report(self, pk: str, new_items: int, now: float | None = None) -> None:
        """A post page was opened and `new_items` of its replies were new to Exorde."""
        p = self._posts.get(pk)
        if p is None:
            return
        now = time.time() if now is None else now
        first = p.fetched == 0  # never opened before: the first measurement replaces the optimistic prior
        p.gain = float(new_items) if first else 0.5 * p.gain + 0.5 * new_items
        p.misses = 0 if new_items > 0 else p.misses + 1
        p.fetched = p.seen
        if p.misses >= self._max_misses:
            del self._posts[pk]
        else:
            # Measured: re-opening a post whose page looks the same gives ~0.1 new replies. Only a post
            # that really keeps paying (gain >= 1.5) is re-opened on a timer; any other one waits long
            # and is re-opened earlier only when a profile fetch shows >= min_growth new replies.
            if p.misses == 0 and p.gain >= 1.5:
                interval = self._base / p.gain
            else:
                interval = self._base * 2 ** (p.misses + 1)
            p.next_due = now + min(self._max, max(self._min, interval))
        self._dirty = True

    def is_first_fetch(self, pk: str) -> bool:
        """True while the post page has never been opened (statistics only)."""
        p = self._posts.get(pk)
        return p is not None and p.fetched == 0

    def retry_later(self, pk: str, seconds: float = 60.0, now: float | None = None) -> None:
        """The fetch itself failed (proxy / transport): try again soon, without penalizing the post."""
        p = self._posts.get(pk)
        if p is not None:
            p.next_due = (time.time() if now is None else now) + seconds

    def drop(self, pk: str) -> None:
        if self._posts.pop(pk, None) is not None:
            self._dirty = True

    def expire(self, now: float | None = None) -> int:
        """Forget posts older than max_age; keep the best `max_posts` if the list grew too big."""
        now = time.time() if now is None else now
        before = len(self._posts)
        self._posts = {pk: p for pk, p in self._posts.items() if now - p.created <= self._max_age}
        if len(self._posts) > self._max_posts:
            keep = sorted(self._posts.items(), key=lambda kv: (-kv[1].gain, -kv[1].created))[: self._max_posts]
            self._posts = dict(keep)
        removed = before - len(self._posts)
        if removed:
            self._dirty = True
        return removed
