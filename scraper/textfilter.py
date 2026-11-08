"""Text-only filter: the collector wants text. Posts with images or videos are dropped. Posts with
links are KEPT (Exorde wants every outgoing link inside the content, see build_payload). Emoji and
URLs do not count towards the minimum length.
"""
from __future__ import annotations

import re

from scraper.pipeline import RawThreadsPost

_URL_RE = re.compile(
    r"(?i)(?:https?://|www\.|\b[a-z0-9-]+\.(?:com|net|org|io|ru|ly|co|me|tv|gg|app|link|xyz|info|site|online)\b)"
)
# Emoji, pictographs, dingbats, flags, skin tones, variation selector, zero-width joiner, keycap.
_EMOJI_RE = re.compile(
    "[\U0001F000-\U0001FAFF☀-➿⬀-⯿←-⇿⌀-⏿■-◿"
    "⁉‼™ℹ〰〽㊗㊙©®︎️‍⃣]"
)
_SPACE_RE = re.compile(r"\s+")
_URL_STRIP_RE = re.compile(r"(?i)(?:https?://|www\.)\S+")


def plain_length(text: str) -> int:
    """Characters left after removing URLs and emoji and collapsing whitespace."""
    return len(_SPACE_RE.sub(" ", _EMOJI_RE.sub("", _URL_STRIP_RE.sub("", text or ""))).strip())


def skip_reason(post: RawThreadsPost, min_len: int, text_only: bool = True, drop_links: bool = False) -> str:
    """"" = send it, "non_text" = has an image/video (or a link when drop_links), "too_short" = not
    enough plain characters.
    """
    content = post.content or ""
    extra = post.extra or {}
    if text_only and extra.get("has_media"):
        return "non_text"
    if drop_links and (extra.get("links") or _URL_RE.search(content)):
        return "non_text"
    if min_len > 0 and plain_length(content) < min_len:
        return "too_short"
    return ""
