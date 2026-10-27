"""Paths, timeouts, logger setup, and fixed constants."""
from __future__ import annotations

import logging
import os
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_PATH = BASE_DIR / "config.yaml"

# Exorde requires domain to always be "threads.net" for this parser.
DOMAIN = "threads.net"

DEFAULT_STORE_ENDPOINT = "http://127.0.0.1:9000/store_items"

HTTP_TIMEOUT_SECONDS = 15.0

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)


class _ThrottleFilter(logging.Filter):
    """Pass at most one WARNING+ record per distinct message template per interval.

    The next record that gets through says how many similar ones were suppressed.
    INFO records (cycle statistics) are never throttled.
    """

    def __init__(self, interval: float) -> None:
        super().__init__()
        self._interval = interval
        self._state: dict[tuple[int, str], list[float]] = {}  # key -> [last_emit_time, suppressed]

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.WARNING or self._interval <= 0 or not isinstance(record.msg, str):
            return True
        key = (record.levelno, record.msg)
        now = time.monotonic()
        entry = self._state.get(key)
        if entry is not None and now - entry[0] < self._interval:
            entry[1] += 1
            return False
        if len(self._state) > 1000:
            self._state.clear()
        if entry is not None and entry[1]:
            record.msg = f"{record.msg} [+{int(entry[1])} similar suppressed in last {self._interval:.0f}s]"
        self._state[key] = [now, 0]
        return True


# Seconds between repeats of the same warning/error text (0 disables throttling).
LOG_THROTTLE_SECONDS = float(os.getenv("LOG_THROTTLE_SECONDS", "30"))
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_ThrottleFilter(LOG_THROTTLE_SECONDS))

LOGGER = logging.getLogger("threads_parser")
# httpx logs every request at INFO (thousands of lines per cycle).
logging.getLogger("httpx").setLevel(logging.WARNING)
