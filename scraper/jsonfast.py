"""Fast JSON dump for the big state files (targets, hot posts, seen ids).

The standard encoder holds the GIL for the whole dump, so an 8 MB file stalls the event loop for
~0.5 s even when written from a thread. orjson does the same job ~10x faster.
"""
from __future__ import annotations

import json
from typing import Any

try:
    import orjson as _orjson

    def dump_bytes(data: Any) -> bytes:
        return _orjson.dumps(data)
except ImportError:  # pragma: no cover - optional dependency
    def dump_bytes(data: Any) -> bytes:
        return json.dumps(data, separators=(",", ":")).encode("utf-8")
