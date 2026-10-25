"""ConfigStore: config.yaml with hot reload."""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

import yaml

from scraper.constants import DEFAULT_STORE_ENDPOINT, DOMAIN, LOGGER


class ConfigStore:
    """Loads config.yaml and reloads it on a background thread when its
    mtime changes, so tuning values can be edited without a restart.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._data: dict[str, Any] = {}
        self._mtime: float = 0.0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def load(self) -> None:
        with open(self._path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        raw.setdefault("store_endpoint", DEFAULT_STORE_ENDPOINT)
        raw.setdefault("domain", DOMAIN)
        # Env override for store_endpoint, useful in docker-compose.
        raw["store_endpoint"] = os.getenv("STORE_ENDPOINT", raw["store_endpoint"])
        with self._lock:
            self._data = raw
            self._mtime = self._path.stat().st_mtime
        LOGGER.info(
            "config.yaml loaded: target_rate=%s poll_interval=%s",
            raw.get("target_rate_per_second"),
            raw.get("poll_interval_seconds"),
        )

    def get(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def start_hot_reload(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _watch_loop(self) -> None:
        while not self._stop.is_set():
            interval = self.get().get("reload_interval_seconds", 5)
            time.sleep(interval)
            try:
                mtime = self._path.stat().st_mtime
                if mtime != self._mtime:
                    self.load()
            except OSError as exc:
                LOGGER.warning("Failed to check config.yaml mtime: %s", exc)
