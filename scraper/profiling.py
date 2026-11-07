"""On-demand profile of the event-loop thread (the one that is pinned at ~100% CPU).

PROFILE_SECONDS=90 in the environment: for that many seconds after the start of the HTTP runner cProfile
records the main thread, then the top functions by own time are written to the log ("profile top ...").
The profile itself slows the parser down (about 2x) while it runs.
"""
from __future__ import annotations

import asyncio
import cProfile
import io
import pstats

from scraper.constants import LOGGER


async def profile_main_thread(seconds: float, warmup: float = 60.0, top: int = 40) -> None:
    await asyncio.sleep(warmup)  # let the first cycle fill the pipeline
    LOGGER.info("profile: recording the event-loop thread for %.0fs", seconds)
    prof = cProfile.Profile()
    prof.enable()
    try:
        await asyncio.sleep(seconds)
    finally:
        prof.disable()
        out = io.StringIO()
        stats = pstats.Stats(prof, stream=out).strip_dirs().sort_stats("tottime")
        stats.print_stats(top)
        LOGGER.info("profile top %d by own time (cumulative columns include awaited work):", top)
        for line in out.getvalue().splitlines():
            if line.strip():
                LOGGER.info("profile | %s", line)
