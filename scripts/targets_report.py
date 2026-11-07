#!/usr/bin/env python3
"""Summary of data/targets_state.json: how many targets were scraped, productive, with which pauses."""
import json
import sys
from collections import Counter

path = sys.argv[1] if len(sys.argv) > 1 else "data/targets_state.json"
raw = json.load(open(path, encoding="utf-8"))
targets = raw.get("targets", {})
# value layout: [interval, next_due, scrapes, fresh_total, yield_ema]
scraped = [v for v in targets.values() if v[2] > 0]
print(f"targets: {len(targets)}  dead: {len(raw.get('dead', []))}  scraped at least once: {len(scraped)}")
print(f"with fresh posts (active): {sum(1 for v in scraped if v[3] > 0)}   with yield_ema > 0: {sum(1 for v in scraped if v[4] > 0)}   yield_ema >= 1: {sum(1 for v in scraped if v[4] >= 1)}")
print("scrapes per target:", dict(sorted(Counter(min(v[2], 10) for v in targets.values()).items())))
print("pause (s), scraped targets:", dict(sorted(Counter(int(v[0]) for v in scraped).most_common(8))))
top = sorted(scraped, key=lambda v: -v[4])[:5]
print("top yield_ema:", [round(v[4], 2) for v in top])
