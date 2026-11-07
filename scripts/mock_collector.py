#!/usr/bin/env python3
"""Mock store_items collector for parser development (standard library only).

POST /store_items  {"items": [...]}  ->  {"count": N, "results": {"to_gpu": a, "duplicate": b, ...}}
POST /v1/check     {"urls": [...]} -> JSON list of urls already known: Exorde's answer (forwarded to the real
                   url-check API) UNION everything this mock has accepted. This stands in for "Exorde learns what the
                   real collector uploaded"; without it every restart/eviction makes already-stored items look new.
GET  /stats        -> JSON counters      GET /health -> "ok"

It validates the Exorde item schema, drops duplicates (by url) and short texts, counts what would be
accepted and logs the rate every few seconds. It does NOT upload anywhere.

Env: MOCK_PORT=9000  MOCK_MIN_LEN=7  MOCK_DOMAIN=threads.net  MOCK_STATS_SECONDS=10
     MOCK_SEEN_FILE=data/mock_collector_seen.txt (url hashes, survives restarts; empty = in memory only)
     MOCK_SAVE_FILE= (optional JSONL with accepted items; grows without limit, use for short debugging)
     MOCK_UPSTREAM_CHECK=https://url-check.exorde.network/v1/check (empty = answer from the mock's own memory only)
Parser side (config.yaml): url_check_endpoint: "http://collector:9000/v1/check"
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import threading
import time
import urllib.request
from collections import Counter, deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.getenv("MOCK_PORT", "9000"))
MIN_LEN = int(os.getenv("MOCK_MIN_LEN", "7"))
DOMAIN = os.getenv("MOCK_DOMAIN", "threads.net")
STATS_SECONDS = float(os.getenv("MOCK_STATS_SECONDS", "10"))
SEEN_FILE = os.getenv("MOCK_SEEN_FILE", "data/mock_collector_seen.txt")
SAVE_FILE = os.getenv("MOCK_SAVE_FILE", "")
UPSTREAM_CHECK = os.getenv("MOCK_UPSTREAM_CHECK", "https://url-check.exorde.network/v1/check")

CREATED_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
REQUIRED = ("content", "external_id", "created_at", "url", "title", "author", "username", "external_parent_id", "domain", "summary")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [mock_collector] %(message)s")
LOG = logging.getLogger("mock_collector")

_lock = threading.Lock()
_seen: set[str] = set()
_new_hashes: list[str] = []
_totals: Counter[str] = Counter()
_window: Counter[str] = Counter()
_invalid_samples: dict[str, str] = {}
_recent_accepted: deque[float] = deque()  # timestamps, for the 60 s rate


def _hash(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()[:16]


def validate(item: object) -> str | None:
    """Reason why the item breaks the schema, or None."""
    if not isinstance(item, dict):
        return "not_an_object"
    for key in REQUIRED:
        if key not in item:
            return f"missing_{key}"
    for key in REQUIRED:
        if not isinstance(item[key], str):
            return f"{key}_not_string"
    if not item["content"].strip():
        return "empty_content"
    if not CREATED_RE.match(item["created_at"]):
        return "created_at_not_utc_z"
    if not item["url"].startswith("https://"):
        return "url_not_https"
    if item["domain"] != DOMAIN:
        return "wrong_domain"
    try:
        summary = json.loads(item["summary"])
    except ValueError:
        return "summary_not_json_string"
    if not isinstance(summary, dict):
        return "summary_not_json_object"
    return None


def process(items: list) -> dict[str, int]:
    results: Counter[str] = Counter()
    accepted: list[dict] = []
    with _lock:
        for item in items:
            reason = validate(item)
            if reason:
                results["invalid"] += 1
                if reason not in _invalid_samples:
                    _invalid_samples[reason] = json.dumps(item, ensure_ascii=False)[:300] if isinstance(item, dict) else str(item)[:300]
                continue
            if len(item["content"].strip()) < MIN_LEN:
                results["skipped_short"] += 1
                continue
            h = _hash(item["url"])
            if h in _seen:
                results["duplicate"] += 1
                continue
            _seen.add(h)
            _new_hashes.append(h)
            results["to_gpu"] += 1
            accepted.append(item)
        _totals.update(results)
        _totals["received"] += len(items)
        _window.update(results)
        _window["received"] += len(items)
        now = time.monotonic()
        _recent_accepted.extend([now] * results["to_gpu"])
    if SAVE_FILE and accepted:
        with open(SAVE_FILE, "a", encoding="utf-8") as fh:
            for item in accepted:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
    return dict(results)


def upstream_known(urls: list[str]) -> list[str]:
    """Which urls the real Exorde url-check already knows (raises on failure: the parser then does not send)."""
    request = urllib.request.Request(
        UPSTREAM_CHECK, json.dumps({"urls": urls}).encode(), {"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        data = json.loads(response.read())
    if not isinstance(data, list):
        raise ValueError("unexpected upstream answer")
    return [u for u in data if isinstance(u, str)]


def check(urls: list[str]) -> list[str]:
    known: set[str] = set()
    with _lock:
        known.update(u for u in urls if _hash(u) in _seen)
        _totals["check_urls"] += len(urls)
    if UPSTREAM_CHECK:
        upstream = upstream_known(urls)
        known.update(upstream)
        with _lock:
            _totals["check_upstream_known"] += len(upstream)
    with _lock:
        _totals["check_known"] += len(known)
    return list(known)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "mock-collector"

    def log_message(self, fmt, *args):  # no per-request access log
        pass

    def _send(self, code: int, body: bytes, ctype: str = "application/json") -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send(200, b"ok", "text/plain")
        elif self.path == "/stats":
            self._send(200, json.dumps(snapshot()).encode())
        else:
            self._send(404, b'{"detail":"Not Found"}')

    def do_POST(self):
        if self.path == "/v1/check":
            try:
                length = int(self.headers.get("Content-Length") or 0)
                data = json.loads(self.rfile.read(length) or b"{}")
                urls = data.get("urls") if isinstance(data, dict) else None
                if not isinstance(urls, list) or not all(isinstance(u, str) for u in urls):
                    raise ValueError('body must be {"urls": [...]}')
            except (ValueError, OSError) as exc:
                self._send(422, json.dumps({"detail": str(exc)}).encode())
                return
            try:
                self._send(200, json.dumps(check(urls)).encode())
            except Exception as exc:  # upstream down: fail closed, the parser will not send these urls
                LOG.warning("url check upstream failed: %s", exc)
                self._send(503, json.dumps({"detail": "upstream check failed"}).encode())
            return
        if self.path != "/store_items":
            self._send(404, b'{"detail":"Not Found"}')
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            data = json.loads(self.rfile.read(length) or b"{}")
            items = data.get("items") if isinstance(data, dict) else None
            if not isinstance(items, list):
                raise ValueError("body must be {\"items\": [...]}")
        except (ValueError, OSError) as exc:
            self._send(422, json.dumps({"detail": str(exc)}).encode())
            return
        results = process(items)
        self._send(200, json.dumps({"count": len(items), "results": results}).encode())


class Server(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 1024  # a burst of parallel connects must not be refused


def snapshot() -> dict:
    with _lock:
        now = time.monotonic()
        while _recent_accepted and now - _recent_accepted[0] > 60:
            _recent_accepted.popleft()
        return {
            "totals": dict(_totals),
            "unique_urls": len(_seen),
            "accepted_per_second_60s": round(len(_recent_accepted) / 60.0, 2),
            "invalid_samples": dict(_invalid_samples),
        }


def load_seen() -> None:
    if not SEEN_FILE or not os.path.isfile(SEEN_FILE):
        return
    with open(SEEN_FILE, encoding="utf-8") as fh:
        _seen.update(line.strip() for line in fh if line.strip())
    LOG.info("loaded %d known urls from %s", len(_seen), SEEN_FILE)


def flush_seen() -> None:
    if not SEEN_FILE:
        return
    with _lock:
        chunk, _new_hashes[:] = list(_new_hashes), []
    if chunk:
        os.makedirs(os.path.dirname(SEEN_FILE) or ".", exist_ok=True)
        with open(SEEN_FILE, "a", encoding="utf-8") as fh:
            fh.write("\n".join(chunk) + "\n")


def reporter() -> None:
    last = time.monotonic()
    shown_invalid: set[str] = set()
    while True:
        time.sleep(STATS_SECONDS)
        flush_seen()
        with _lock:
            window = dict(_window)
            _window.clear()
            total = dict(_totals)
            new_reasons = {k: v for k, v in _invalid_samples.items() if k not in shown_invalid}
        shown_invalid.update(new_reasons)
        elapsed = max(time.monotonic() - last, 0.001)
        last = time.monotonic()
        LOG.info(
            "last %.0fs: received=%d accepted=%d (%.1f/s) duplicate=%d short=%d invalid=%d | total accepted=%d unique=%d | 60s avg %.1f/s",
            elapsed,
            window.get("received", 0),
            window.get("to_gpu", 0),
            window.get("to_gpu", 0) / elapsed,
            window.get("duplicate", 0),
            window.get("skipped_short", 0),
            window.get("invalid", 0),
            total.get("to_gpu", 0),
            len(_seen),
            snapshot()["accepted_per_second_60s"],
        )
        for reason, sample in new_reasons.items():
            LOG.warning("invalid item (%s), sample: %s", reason, sample)


def main() -> None:
    load_seen()
    threading.Thread(target=reporter, daemon=True).start()
    server = Server(("0.0.0.0", PORT), Handler)
    LOG.info("mock collector on :%d (min_len=%d domain=%s) - nothing is uploaded anywhere", PORT, MIN_LEN, DOMAIN)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        flush_seen()


if __name__ == "__main__":
    main()
