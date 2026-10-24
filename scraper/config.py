"""ConfigStore (hot-reload config.yaml), load_accounts, load_cookies.

Cookie files are expected in the Cookie Editor "Export -> JSON" format:
a list of objects with at least "name" and "value" (other fields such as
domain/path/expirationDate/httpOnly/secure/sameSite are optional and, if
present, are mapped onto Playwright's cookie schema).

Each account can ALSO have a storage_state file (cookies + localStorage),
produced by scripts/interactive_login.py via a real headed login. If it
exists, browser.py prefers it over cookie_file.
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from scraper.constants import DEFAULT_STORE_ENDPOINT, DOMAIN, LOGGER, STORAGE_STATE_DIR


@dataclass
class ProxyConfig:
    server: str
    username: str | None = None
    password: str | None = None
    # True only if accounts.yaml names username_env/password_env for this
    # account. VPN-backed accounts (socks5://xray_account_N:1080, no auth)
    # leave it False, so no bogus "credentials missing" warning.
    auth_expected: bool = False


@dataclass
class AccountConfig:
    name: str
    enabled: bool
    cookie_file: Path
    storage_state_file: Path
    user_agent: str
    proxy: ProxyConfig


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


def load_accounts(path: Path) -> list[AccountConfig]:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    accounts: list[AccountConfig] = []
    for entry in raw.get("accounts", []):
        proxy_raw = entry.get("proxy", {})
        proxy = ProxyConfig(
            server=proxy_raw.get("server", ""),
            username=os.getenv(proxy_raw.get("username_env", ""), None),
            password=os.getenv(proxy_raw.get("password_env", ""), None),
            auth_expected=bool(proxy_raw.get("username_env") or proxy_raw.get("password_env")),
        )
        name = entry["name"]
        storage_state_file = Path(entry.get("storage_state_file", STORAGE_STATE_DIR / f"{name}.json"))
        accounts.append(
            AccountConfig(
                name=name,
                enabled=bool(entry.get("enabled", False)),
                cookie_file=Path(entry["cookie_file"]),
                storage_state_file=storage_state_file,
                user_agent=entry.get("user_agent", ""),
                proxy=proxy,
            )
        )

    for acc in accounts:
        if acc.enabled and not acc.proxy.server:
            LOGGER.warning(
                "[%s] enabled but proxy.server is empty - proxy is mandatory, "
                "this account will fail to start",
                acc.name,
            )
        if acc.enabled and acc.proxy.auth_expected and (not acc.proxy.username or not acc.proxy.password):
            LOGGER.warning(
                "[%s] enabled but proxy credentials are missing - check "
                "PROXY_*_USER/PROXY_*_PASS in .env",
                acc.name,
            )
        if acc.enabled and not acc.storage_state_file.exists() and not acc.cookie_file.exists():
            LOGGER.warning(
                "[%s] neither storage_state_file (%s) nor cookie_file (%s) exists - "
                "run the login container for this account first",
                acc.name,
                acc.storage_state_file,
                acc.cookie_file,
            )

    return accounts


def load_cookies(cookie_file: Path) -> list[dict[str, Any]]:
    """Parse a Cookie Editor JSON export into Playwright's add_cookies() schema."""
    if not cookie_file.exists():
        raise FileNotFoundError(
            f"Cookie file not found: {cookie_file}. Export cookies with "
            "Cookie Editor (Export -> JSON) for this account first."
        )

    with open(cookie_file, "r", encoding="utf-8") as f:
        raw_cookies = json.load(f)

    playwright_cookies: list[dict[str, Any]] = []
    for c in raw_cookies:
        if "name" not in c or "value" not in c:
            continue
        cookie: dict[str, Any] = {
            "name": c["name"],
            "value": c["value"],
            "domain": c.get("domain", ".threads.com"),  # real site host; Exorde's DOMAIN constant stays "threads.net"
            "path": c.get("path", "/"),
        }
        if "expirationDate" in c and c["expirationDate"]:
            cookie["expires"] = c["expirationDate"]
        if "httpOnly" in c:
            cookie["httpOnly"] = c["httpOnly"]
        if "secure" in c:
            cookie["secure"] = c["secure"]
        same_site = c.get("sameSite")
        if same_site:
            # Cookie Editor uses "no_restriction"/"lax"/"strict"/"unspecified";
            # Playwright expects "Strict"/"Lax"/"None".
            mapping = {
                "no_restriction": "None",
                "lax": "Lax",
                "strict": "Strict",
                "unspecified": "Lax",
            }
            cookie["sameSite"] = mapping.get(same_site.lower(), "Lax")
        playwright_cookies.append(cookie)

    if not playwright_cookies:
        raise ValueError(f"No usable cookies parsed from {cookie_file}")

    return playwright_cookies