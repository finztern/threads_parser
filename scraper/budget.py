"""AutoBudget: the parser tunes its own request budget between profile pages and hot post pages.

Every cycle it measures what each kind of request really gave (items new to Exorde per request):
- profile class:  new top-level items from profile pages + new replies of FIRST opens of posts (those
                  posts were found by profile pages), per profile request;
- repeat class:   new replies per repeat open of an already opened post.
The better class gets a bigger share next cycle (small multiplicative steps, bounded, smoothed by an
exponential average), and the repeat interval of hot posts is stretched while repeats do not pay.
The tuned values are saved, so a restart continues where it stopped.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from scraper.constants import LOGGER


class AutoBudget:
    def __init__(
        self,
        state_file: Path | None,
        profiles: int,
        hot: int,
        base_interval: float,
        profiles_bounds: tuple[int, int] = (500, 6000),
        hot_bounds: tuple[int, int] = (300, 4000),
        interval_bounds: tuple[float, float] = (300.0, 3600.0),
        step: float = 0.12,
        min_requests: int = 100,
    ) -> None:
        self._file = state_file
        self.profiles = profiles
        self.hot = hot
        self.base_interval = base_interval
        self._pb, self._hb, self._ib = profiles_bounds, hot_bounds, interval_bounds
        self._step = step
        self._min_requests = min_requests
        self._ema_profile: float | None = None
        self._ema_repeat: float | None = None
        self._warmup = 1  # the first cycle after a start is not representative

    # ---- persistence ---------------------------------------------------------------------------
    def load(self) -> None:
        if not self._file or not self._file.is_file():
            return
        try:
            raw = json.loads(self._file.read_text(encoding="utf-8"))
            self.profiles = self._clamp(int(raw["profiles"]), self._pb)
            self.hot = self._clamp(int(raw["hot"]), self._hb)
            self.base_interval = float(min(max(float(raw["base_interval"]), self._ib[0]), self._ib[1]))
            LOGGER.info("budget: restored profiles=%d hot=%d repeat_interval=%.0fs", self.profiles, self.hot, self.base_interval)
        except (OSError, ValueError, KeyError, TypeError):
            LOGGER.exception("budget: could not read %s, using config values", self._file)

    def save(self) -> None:
        if not self._file:
            return
        try:
            self._file.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._file.with_suffix(".tmp")
            tmp.write_text(
                json.dumps({"profiles": self.profiles, "hot": self.hot, "base_interval": round(self.base_interval)}),
                encoding="utf-8",
            )
            os.replace(tmp, self._file)
        except OSError:
            LOGGER.exception("budget: could not write %s", self._file)

    @staticmethod
    def _clamp(value: int, bounds: tuple[int, int]) -> int:
        return max(bounds[0], min(bounds[1], value))

    @staticmethod
    def _ema(old: float | None, new: float) -> float:
        return new if old is None else 0.7 * old + 0.3 * new  # smooth: short cycles are noisy

    # ---- the decision ----------------------------------------------------------------------------
    def update(self, profile_requests: int, profile_new: int, first_new: int, repeat_pages: int, repeat_new: int) -> str | None:
        """Feed the cycle's results; returns a log message when something changed."""
        if self._warmup > 0:
            self._warmup -= 1
            return None
        if profile_requests >= self._min_requests:
            self._ema_profile = self._ema(self._ema_profile, (profile_new + first_new) / profile_requests)
        if repeat_pages >= self._min_requests:
            self._ema_repeat = self._ema(self._ema_repeat, repeat_new / repeat_pages)
        yp, yr = self._ema_profile, self._ema_repeat
        if yp is None or yr is None:
            return None

        old = (self.profiles, self.hot, self.base_interval)
        if yp > yr * 1.25:  # profiles pay better: move budget from repeat opens to profiles
            self.profiles = self._clamp(int(self.profiles * (1 + self._step)) + 1, self._pb)
            self.hot = self._clamp(int(self.hot * (1 - self._step)), self._hb)
        elif yr > yp * 1.25:
            self.hot = self._clamp(int(self.hot * (1 + self._step)) + 1, self._hb)
            self.profiles = self._clamp(int(self.profiles * (1 - self._step)), self._pb)
        # Re-opens of posts that stopped paying are made rarer while repeats do not pay (posts that keep paying
        # are never slowed down), and more frequent again when repeats pay well.
        if yr < 0.2:
            self.base_interval = min(self._ib[1], self.base_interval * 1.15)
        elif yr > 0.5:
            self.base_interval = max(self._ib[0], self.base_interval / 1.15)

        self.save()
        if (self.profiles, self.hot, round(self.base_interval)) == (old[0], old[1], round(old[2])):
            return None
        return (
            f"profile yield {yp:.2f} new/request vs repeat yield {yr:.2f}: profiles {old[0]} -> {self.profiles}, "
            f"hot {old[1]} -> {self.hot}, repeat interval {old[2]:.0f}s -> {self.base_interval:.0f}s"
        )
