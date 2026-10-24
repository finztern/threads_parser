"""ProxyPool: rotating pool of HTTP/SOCKS proxies loaded from a text file.

Accepted line formats (one proxy per line, '#' comments and blanks ignored):
    http://user:pass@host:port
    socks5://user:pass@host:port
    user:pass@host:port          (treated as http://)
    host:port:user:pass          (treated as http://)

The file itself is a secret: keep it out of git (see .gitignore).
"""
from __future__ import annotations

import random
import time
from pathlib import Path

from scraper.constants import LOGGER


def normalize_proxy(line: str) -> str | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "://" in line:
        return line
    if "@" in line:
        return "http://" + line
    parts = line.split(":")
    if len(parts) == 4:
        host, port, user, password = parts
        return f"http://{user}:{password}@{host}:{port}"
    return None


def mask_proxy(proxy: str) -> str:
    """host:port only - never log credentials."""
    return proxy.rsplit("@", 1)[-1].split("://", 1)[-1]


class ProxyPool:
    def __init__(self, proxies: list[str], cooldown_seconds: float = 120.0) -> None:
        if not proxies:
            raise ValueError("proxy list is empty")
        self._proxies = list(dict.fromkeys(proxies))  # dedupe, keep order
        random.shuffle(self._proxies)
        self._idx = 0
        self._cooldown = cooldown_seconds
        self._banned_until: dict[str, float] = {}

    def __len__(self) -> int:
        return len(self._proxies)

    @classmethod
    def from_file(cls, path: Path, cooldown_seconds: float = 120.0) -> "ProxyPool":
        if path.is_dir():
            raise FileNotFoundError(
                f"{path} is a directory - the proxies file is missing on the host "
                "(create it before starting; see proxies.txt.example)"
            )
        if not path.exists():
            raise FileNotFoundError(f"proxies file not found: {path} (see proxies.txt.example)")
        proxies: list[str] = []
        skipped = 0
        for raw in path.read_text(encoding="utf-8").splitlines():
            if not raw.strip() or raw.strip().startswith("#"):
                continue
            proxy = normalize_proxy(raw)
            if proxy:
                proxies.append(proxy)
            else:
                skipped += 1
        if skipped:
            LOGGER.warning("proxies file: %d unparseable line(s) skipped", skipped)
        return cls(proxies, cooldown_seconds)

    def next(self) -> str:
        """Round-robin over proxies that are not cooling down."""
        now = time.monotonic()
        n = len(self._proxies)
        for _ in range(n):
            proxy = self._proxies[self._idx % n]
            self._idx += 1
            if self._banned_until.get(proxy, 0.0) <= now:
                return proxy
        # Everything is cooling down: use the one that frees up first.
        return min(self._proxies, key=lambda p: self._banned_until.get(p, 0.0))

    def ban(self, proxy: str, seconds: float | None = None) -> None:
        self._banned_until[proxy] = time.monotonic() + (seconds if seconds is not None else self._cooldown)

    def available(self) -> int:
        now = time.monotonic()
        return sum(1 for p in self._proxies if self._banned_until.get(p, 0.0) <= now)
