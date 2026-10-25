"""Paths, timeouts, logger setup, and fixed constants."""
from __future__ import annotations

import logging
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_PATH = BASE_DIR / "config.yaml"

# Exorde requires domain to always be "threads.net" for this parser.
DOMAIN = "threads.net"

DEFAULT_STORE_ENDPOINT = "http://127.0.0.1:9000/store_items"

HTTP_TIMEOUT_SECONDS = 15.0

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
LOGGER = logging.getLogger("threads_parser")
# httpx logs every request at INFO (thousands of lines per cycle).
logging.getLogger("httpx").setLevel(logging.WARNING)
