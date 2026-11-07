"""AutoBudget: the parser tunes its own request budget between profile pages and hot post pages.

It does NOT guess which kind of request is "worth" more (profile pages find the posts, post pages give the
replies, so credit is ambiguous). It measures the real objective, items new to Exorde per second, and
hill-climbs: shift part of the budget from profiles to hot pages (or back), run `trial_cycles` cycles,
and keep the shift only if the rate rose; otherwise revert, flip the direction and take a smaller step.
Separately, re-opens of posts that stopped paying are made rarer while repeats yield little.
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
        step: float = 0.5,
        min_requests: int = 100,
        trial_cycles: int = 5,
        margin: float = 0.04,
    ) -> None:
        self._file = state_file
        self.profiles = profiles
        self.hot = hot
        self.base_interval = base_interval
        self._pb, self._hb, self._ib = profiles_bounds, hot_bounds, interval_bounds
        self._step = step
        self._min_step = 0.08
        self._min_requests = min_requests
        self._trial_cycles = max(2, trial_cycles)
        self._margin = margin
        self._ema_repeat: float | None = None
        self._warmup = 1  # the first cycle after a start is not representative
        self._direction = 1  # +1: move budget from profiles to hot pages, -1: back
        self._phase = "base"  # "base": measuring the current point, "trial": measuring a shifted point
        self._rates: list[float] = []
        self._skip = 0  # cycles to ignore after a change (the pipeline is still refilling)
        self._base_point = (profiles, hot)
        self._base_score = 0.0

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
    def _shift(self) -> tuple[int, int]:
        """Profiles/hot after moving part of the budget in the current direction (requests stay ~constant)."""
        p, h = self.profiles, self.hot
        if self._direction > 0:
            delta = max(50, int(h * self._step))
            delta = min(delta, p - self._pb[0], self._hb[1] - h)
        else:
            delta = -max(50, int(h * self._step))
            delta = max(delta, -(h - self._hb[0]), -(self._pb[1] - p))
        return p - delta, h + delta

    def update(
        self, profile_requests: int, profile_new: int, first_new: int, repeat_pages: int, repeat_new: int,
        new_items: int = 0, seconds: float = 0.0,
    ) -> str | None:
        """Feed the cycle's results; returns a log message when something changed."""
        message = self._tune_interval(repeat_pages, repeat_new)
        if self._warmup > 0:
            self._warmup -= 1
            return message
        if seconds <= 0:
            return message
        if self._skip > 0:
            self._skip -= 1
            return message
        self._rates.append(new_items / seconds)
        if len(self._rates) < self._trial_cycles:
            return message
        score = sum(self._rates) / len(self._rates)
        self._rates = []

        if self._phase == "base":
            self._base_score = score
            self._base_point = (self.profiles, self.hot)
            new_p, new_h = self._shift()
            if (new_p, new_h) == (self.profiles, self.hot):  # at a bound: turn around
                self._direction = -self._direction
                new_p, new_h = self._shift()
            if (new_p, new_h) == (self.profiles, self.hot):
                return message
            self.profiles, self.hot = new_p, new_h
            self._phase, self._skip = "trial", 1
            self.save()
            return f"trial: profiles {self._base_point[0]} -> {new_p}, hot {self._base_point[1]} -> {new_h} (baseline {score:.1f} new/s)"

        # trial finished
        old_p, old_h = self._base_point
        if score > self._base_score * (1 + self._margin):
            result = f"kept profiles={self.profiles} hot={self.hot}: {score:.1f} new/s vs {self._base_score:.1f}"
            self._base_score, self._base_point = score, (self.profiles, self.hot)
            new_p, new_h = self._shift()  # keep going the same way from the new point
            if (new_p, new_h) == (self.profiles, self.hot):
                self._direction = -self._direction
                self._phase = "base"
            else:
                self.profiles, self.hot = new_p, new_h
                self._skip = 1
                result += f"; next trial profiles={new_p} hot={new_h}"
        else:
            self.profiles, self.hot = old_p, old_h
            self._direction = -self._direction
            self._step = max(self._min_step, self._step * 0.6)
            self._phase, self._skip = "base", 1
            result = (f"reverted to profiles={old_p} hot={old_h}: {score:.1f} new/s vs {self._base_score:.1f}; "
                      f"next direction {'hot+' if self._direction > 0 else 'profiles+'}, step {self._step:.2f}")
        self.save()
        return result

    def _tune_interval(self, repeat_pages: int, repeat_new: int) -> str | None:
        """Re-opens of posts that stopped paying are made rarer while repeats yield little (and more frequent
        again when they pay well). Posts that keep paying are never slowed down by this."""
        if repeat_pages < self._min_requests:
            return None
        self._ema_repeat = self._ema(self._ema_repeat, repeat_new / repeat_pages)
        yr, old = self._ema_repeat, self.base_interval
        if yr < 0.2:
            self.base_interval = min(self._ib[1], old * 1.15)
        elif yr > 0.5:
            self.base_interval = max(self._ib[0], old / 1.15)
        if round(self.base_interval) == round(old):
            return None
        self.save()
        return f"repeat yield {yr:.2f} new/page: barren re-open interval {old:.0f}s -> {self.base_interval:.0f}s"
