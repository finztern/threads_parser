"""Check proxies from a file, optionally through a user-provided health URL."""
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


def check_proxy(
    endpoint: tuple[int, str, str, int], timeout: float, url: str | None
) -> tuple[int, str, str, int, str | None]:
    line_number, proxy, host, port = endpoint
    try:
        if url is None:
            with socket.create_connection((host, port), timeout=timeout):
                pass
            result = None
        else:
            from curl_cffi import requests as cffi

            response = cffi.get(
                url,
                proxies={"http": proxy, "https": proxy},
                timeout=timeout,
                allow_redirects=False,
            )
            result = None if 200 <= response.status_code < 300 else f"HTTP {response.status_code}"
    except OSError as exc:
        result = type(exc).__name__
    except Exception as exc:
        result = type(exc).__name__
    return line_number, proxy, host, port, result


def main() -> int:
    parser = argparse.ArgumentParser(description="Check TCP reachability of proxies from a file")
    parser.add_argument("--file", type=Path, default=BASE_DIR / "proxies.txt")
    parser.add_argument("--url", help="HTTP(S) health URL you own; tests each proxy with one GET")
    parser.add_argument("--output", type=Path, help="write successful proxy lines (requires --url)")
    parser.add_argument("--timeout", type=float, default=5.0, help="timeout per proxy in seconds")
    parser.add_argument("--workers", type=int, default=8, help="maximum concurrent checks (1-32)")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if not 1 <= args.workers <= 5000:
        parser.error("--workers must be between 1 and 5000")
    if args.url:
        try:
            parsed_url = urlsplit(args.url)
            if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
                raise ValueError
        except ValueError:
            parser.error("--url must be an absolute HTTP(S) URL")
        if args.output is None:
            args.output = BASE_DIR / "proxy-results" / "proxies.working.txt"
    elif args.output is not None:
        parser.error("--output requires --url; TCP checks do not verify proxy functionality")

    try:
        lines = args.file.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        parser.error(f"cannot read {args.file}: {exc}")

    endpoints: list[tuple[int, str, str, int]] = []
    normalized_lines: dict[int, str] = {}
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
        endpoints.append((line_number, proxy, host, port))
        normalized_lines[line_number] = proxy

    if not endpoints and not invalid_lines:
        print(f"No proxies found in {args.file}")
        return 1

    if args.output is not None and args.output.resolve() == args.file.resolve():
        parser.error("--output must not overwrite the input proxy file")

    check = partial(check_proxy, timeout=args.timeout, url=args.url)
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        results = list(executor.map(check, endpoints))

    working: list[str] = []
    for line_number, _proxy, host, port, error in results:
        label = f"{host}:{port}"
        if error is None:
            working.append(normalized_lines[line_number])
            detail = "HTTP proxy request succeeded" if args.url else "TCP reachable"
            print(f"[OK]   line {line_number}: {label} ({detail})")
        else:
            print(f"[FAIL] line {line_number}: {label} ({error})")

    for line_number in invalid_lines:
        print(f"[SKIP] line {line_number}: invalid proxy format")

    failed = len(results) - len(working)
    check_kind = "HTTP working" if args.url else "TCP reachable"
    print(
        f"Summary: {len(working)} {check_kind}, {failed} failed, "
        f"{len(invalid_lines)} invalid; {len(results)} proxy endpoint(s) tested"
    )
    if args.output is not None:
        try:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text("\n".join(working) + ("\n" if working else ""), encoding="utf-8")
        except OSError as exc:
            parser.error(f"cannot write {args.output}: {exc}")
        print(f"Working proxies written to {args.output}")
    return 1 if failed or invalid_lines else 0


if __name__ == "__main__":
    raise SystemExit(main())