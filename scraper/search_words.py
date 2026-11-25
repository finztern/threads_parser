"""Words for the newest-first search (BarcelonaSearchResultsQuery, recent=1).

Each word returns the ~12 newest posts that contain it, so very common words of many languages give a near
chronological sample of what is being posted right now. Overridable with `search_words:` in config.yaml.
"""
from __future__ import annotations

import random

DEFAULT_WORDS = """
the a i you and to of is it that in for my me so but just love good time now this we are not be have what
like day today new people life too out up go see want know think need happy home work night morning friend
de que el la y en un no es lo me para con por una se mi te si pero muy hoy bien gracias amor
nao que um uma eu de em com para voce mais hoje bom dia obrigado ele ela
yang dan di ini itu aku ga nggak gue lu udah banget aja sama kalau buat
bir bu ve ben çok de da ne için ama var yok bugün
le la et je pas un une est que pour dans ce il vous mais
der die und ich nicht das ist ein es zu mit auf den
di che il non per un una è sono mi ho
и в не что я это на с как но все так
في من أنا لا ما هذا على مع يا الله
ที่ ไม่ ผม ฉัน เป็น ได้ มี แล้ว ก็ วันนี้
です の は を に が と で も ます
이 는 가 을 에 도 그 나 오늘
的 我 是 不 了 在 有 你 今天
है के में को की हम आप नहीं
là của tôi không và có được một
""".split()


class SearchWords:
    """Endless rotation over the words, a different shuffled order every pass; `shard` = (index, total) takes every
    total-th word so that parser instances do not search the same words."""

    def __init__(self, words: list[str] | None = None, shard: tuple[int, int] = (0, 1)) -> None:
        pool = list(dict.fromkeys(w for w in (words or DEFAULT_WORDS) if w))
        index, total = shard
        self._words = pool[index::total] if total > 1 else pool
        self._queue: list[str] = []

    def __len__(self) -> int:
        return len(self._words)

    def take(self, n: int) -> list[str]:
        out: list[str] = []
        while len(out) < n and self._words:
            if not self._queue:
                self._queue = self._words[:]
                random.shuffle(self._queue)
            out.append(self._queue.pop())
        return out


def parse_shard(spec: str) -> tuple[int, int]:
    index_s, _, total_s = (spec or "").strip().partition("/")
    if not index_s:
        return 0, 1
    return int(index_s), max(1, int(total_s or 1))
