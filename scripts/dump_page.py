"""Dump any Threads page as the given account (its VPN proxy + saved login).

Usage (inside the parser image):
    python3 scripts/dump_page.py <account_name> <url>

Writes debug/<account>_dump.html and debug/<account>_dump.png, and prints
the GraphQL request names the page made (helps to see what is loaded via
GraphQL vs. embedded in the HTML).
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.browser import ThreadsBrowserSession, _friendly_name, _is_graphql_response  # noqa: E402
from scraper.config import load_accounts  # noqa: E402
from scraper.constants import ACCOUNTS_PATH, BASE_DIR  # noqa: E402


async def main() -> None:
    if len(sys.argv) != 3:
        print("usage: dump_page.py <account_name> <url>", file=sys.stderr)
        sys.exit(1)
    account_name, url = sys.argv[1], sys.argv[2]

    account = next((a for a in load_accounts(ACCOUNTS_PATH) if a.name == account_name), None)
    if account is None:
        print(f"unknown account {account_name!r}", file=sys.stderr)
        sys.exit(1)

    session = ThreadsBrowserSession(account)
    await session.start()
    page = session._page
    assert page is not None

    seen: list[str] = []

    def on_response(response) -> None:
        if _is_graphql_response(response):
            seen.append(_friendly_name(response) or "?")

    page.on("response", on_response)
    try:
        await page.goto(url, wait_until="networkidle")
        await page.wait_for_timeout(3000)

        out = BASE_DIR / "debug"
        out.mkdir(parents=True, exist_ok=True)
        html_path = out / f"{account_name}_dump.html"
        png_path = out / f"{account_name}_dump.png"
        html_path.write_text(await page.content(), encoding="utf-8")
        await page.screenshot(path=str(png_path), full_page=True)

        print("landed on:", page.url)
        print("GraphQL calls:", seen or "none")
        print("saved:", html_path, png_path)
    finally:
        await session.close()


if __name__ == "__main__":
    asyncio.run(main())
