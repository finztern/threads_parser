"""Sharding of targets between several parser instances (one per machine).

PARSER_SHARD="i/N" (e.g. "0/2" and "1/2"): an instance owns the usernames whose crc32 % N == i, so
two parsers never fetch the same profile or post. Unset / "0/1" = no sharding (owns everything).
"""
from __future__ import annotations

import zlib
from typing import Callable


def make_owner(spec: str) -> Callable[[str], bool] | None:
    spec = (spec or "").strip()
    if not spec:
        return None
    index_s, _, total_s = spec.partition("/")
    index, total = int(index_s), int(total_s or 1)
    if total < 1 or not 0 <= index < total:
        raise ValueError(f"bad PARSER_SHARD {spec!r}: expected i/N with 0 <= i < N")
    if total == 1:
        return None
    return lambda username: zlib.crc32(username.lower().encode("utf-8")) % total == index
