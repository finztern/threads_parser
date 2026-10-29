"""Check TCP reachability of proxy servers listed in proxies.txt.

This only connects to each proxy host and port. It does not send proxy
protocol commands or make requests through the proxies to another service.
"""
from __future__ import annotations

import argparse
import socket
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper.proxies import normalize_proxy  # noqa: E402

BASE_DIR = Path(__file__).resolve().parent.parent


def proxy_endpoint(proxy: str) -> tuple[str, int]:
    parsed = urlsplit(proxy)
    if not parsed.scheme or not parsed.hostname or parsed.port is None:
        raise ValueError("missing scheme, host, or port")
    if not 1 <= parsed.port <= 65535:
        raise ValueError("port must be between 1 and 65535")
    return parsed.hostname, parsed.port


def check_tcp(endpoint: tuple[int, str, int], timeout: float) -> tuple[int, str, int, str | None]:
    line_number, host, port = endpoint
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        reason = exc.strerror or str(exc)
        return line_number, host, port, f"{type(exc).__name__}: {reason}"
    return line_number, host, port, None


def main() -> int:
    parser = argparse.ArgumentParser(description="Check TCP reachability of proxies from a file")
    parser.add_argument("--file", type=Path, default=BASE_DIR / "proxies.txt")
    parser.add_argument("--timeout", type=float, default=3.0, help="TCP timeout per proxy in seconds")
    parser.add_argument("--workers", type=int, default=32, help="maximum parallel TCP connections (1-128)")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if not 1 <= args.workers <= 128:
        parser.error("--workers must be between 1 and 128")

    try:
        lines = args.file.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        parser.error(f"cannot read {args.file}: {exc}")

    endpoints: list[tuple[int, str, int]] = []
    invalid_lines: list[int] = []
    for line_number, raw in enumerate(lines, start=1):
        if not raw.strip() or raw.strip().startswith("#"):
            continue
        proxy = normalize_proxy(raw)
        try:
            if proxy is None:
                raise ValueError("unrecognized proxy format")
            host, port = proxy_endpoint(proxy)
        except ValueError:
            invalid_lines.append(line_number)
            continue
        endpoints.append((line_number, host, port))

    if not endpoints and not invalid_lines:
        print(f"No proxies found in {args.file}")
        return 1

    check = partial(check_tcp, timeout=args.timeout)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        results = list(executor.map(check, endpoints))

    reachable = 0
    for line_number, host, port, error in results:
        label = f"{host}:{port}"
        if error is None:
            reachable += 1
            print(f"[OK]   line {line_number}: {label}")
        else:
            print(f"[FAIL] line {line_number}: {label} ({error})")

    for line_number in invalid_lines:
        print(f"[SKIP] line {line_number}: invalid proxy format")

    failed = len(results) - reachable
    print(
        f"Summary: {reachable} TCP reachable, {failed} failed, "
        f"{len(invalid_lines)} invalid; {len(results)} proxy endpoint(s) tested"
    )
    return 1 if failed or invalid_lines else 0


if __name__ == "__main__":
    raise SystemExit(main())