"""Interactive login helper: opens a REAL headed browser (through the same
VPN proxy the account uses in production) so you log in by hand, then saves
context.storage_state() (cookies + localStorage) to storage_state/<account>.json.

A Cookie Editor export has cookies only; Threads/Instagram's web client also
keeps session state in localStorage, so a real login is more reliable.

Runs inside the login_account_N container (docker-compose profile "login").
Env: ACCOUNT_NAME, PROXY_SERVER. User agent is taken from accounts.yaml so
it matches the one the scraper uses later.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import yaml
from playwright.async_api import async_playwright

LOGIN_URL = "https://www.threads.com/login"
CHECK_INTERVAL_SECONDS = 3
TIMEOUT_SECONDS = 15 * 60


def _user_agent_for(account_name: str) -> str | None:
    path = Path("/app/accounts.yaml")
    if not path.exists():
        return None
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for entry in raw.get("accounts", []):
        if entry.get("name") == account_name:
            return entry.get("user_agent") or None
    return None


async def main() -> None:
    account_name = os.environ.get("ACCOUNT_NAME")
    proxy_server = os.environ.get("PROXY_SERVER")
    if not account_name or not proxy_server:
        print("ACCOUNT_NAME and PROXY_SERVER env vars are required", file=sys.stderr)
        sys.exit(1)

    out_dir = Path("/app/storage_state")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{account_name}.json"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=False,
            proxy={"server": proxy_server},
            args=["--start-maximized", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(no_viewport=True, user_agent=_user_agent_for(account_name))
        page = await context.new_page()
        await page.goto(LOGIN_URL)

        print(f"[{account_name}] browser open via {proxy_server}; log in by hand in noVNC.", flush=True)

        elapsed = 0
        logged_in = False
        while elapsed < TIMEOUT_SECONDS:
            cookies = await context.cookies()
            # Must be the THREADS session: logging in via Instagram sets an
            # instagram.com sessionid first, before returning to threads.com.
            if any(
                c["name"] == "sessionid" and c["value"] and "threads." in c.get("domain", "")
                for c in cookies
            ):
                logged_in = True
                break
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)
            elapsed += CHECK_INTERVAL_SECONDS

        if not logged_in:
            print(f"[{account_name}] timed out, nothing saved.", file=sys.stderr)
            await browser.close()
            sys.exit(1)

        # Let post-login redirects / localStorage writes finish.
        await page.wait_for_timeout(5000)
        await context.storage_state(path=str(out_path))
        os.chmod(out_path, 0o600)
        print(f"[{account_name}] saved storage_state -> {out_path}", flush=True)

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
