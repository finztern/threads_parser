"""Change one top-level `key: value` line of config.yaml IN PLACE (same inode).

config.yaml is bind-mounted into the container as a single file: `sed -i` replaces the file with a new one and the
container keeps seeing the old content, so the hot reload never fires. This script rewrites the content of the same file.

A key missing from the file is appended (so code defaults can be overridden).

Usage:  python3 scripts/setcfg.py static_proxy_share 0.75 [more_key more_value ...]
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

path = Path(__file__).resolve().parent.parent / "config.yaml"
args = sys.argv[1:]
if len(args) < 2 or len(args) % 2:
    sys.exit(__doc__)
with path.open("r+", encoding="utf-8") as handle:
    text = handle.read()
    for key, value in zip(args[0::2], args[1::2]):
        text, count = re.subn(rf"(?m)^{re.escape(key)}:.*$", f"{key}: {value}", text, count=1)
        if not count:  # a key that is not in the file yet (code default): append it, same inode
            text = text + ("" if text.endswith("\n") else "\n") + f"{key}: {value}\n"
        print(f"{key}: {value}")
    handle.seek(0)
    handle.write(text)
    handle.truncate()
