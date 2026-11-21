#!/usr/bin/env bash
# YouTube hot-videos test (not a parser): fresh comments per second through the free vless nodes + Exorde url-check dedup
#
# Run inside the parser image (it has xray and the scraper package; requests go through curl_cffi, Chrome TLS impersonation):
#   docker compose run --rm --no-deps -v "$PWD/scripts:/app/scripts:ro" parser bash /app/scripts/yt_hot_test.sh
# Optional env: NODES=40  CONC=80 (workers)  SECONDS_RUN=240  HOT_INTERVAL=30  HOT_MIN=1.0  EXPLORE_SHARE=0.7  MAX_PAGES=3  URL_CHECK=off|<endpoint>  VPN_SOURCE=<subscription url or a local file path>
#
set -u

NODES="${NODES:-40}"
CONC="${CONC:-80}"
SECONDS_RUN="${SECONDS_RUN:-240}"
PAGES="${PAGES:-6}"
BOOT="${BOOT:-next}"
SORT="${SORT:-new}"
VPN_SOURCE="${VPN_SOURCE:-https://github.com/Au1rxx/free-vpn-subscriptions/raw/main/output/v2ray-base64.txt}"
BASE_PORT=13000
WORK="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$WORK"' EXIT
export PYTHONPATH="/app:${PYTHONPATH:-}"
cd /app 2>/dev/null || true

python3 -c "import curl_cffi" 2>/dev/null || { echo "curl_cffi is not available (run this inside the parser image)"; exit 1; }
command -v xray >/dev/null 2>&1 || { echo "xray is not available (run this inside the parser image)"; exit 1; }

# ---- 1. nodes: download the list, test every node on its own (one bad node breaks the whole xray config) ----
python3 - "$VPN_SOURCE" "$WORK/sub.txt" <<'PY' || { echo "cannot download $VPN_SOURCE"; exit 1; }
import os
import shutil
import sys
if os.path.isfile(sys.argv[1]):  # a local file (a subscription saved on the host, e.g. when the container cannot reach the site)
    shutil.copy(sys.argv[1], sys.argv[2])
    sys.exit(0)
from curl_cffi import requests
r = requests.get(sys.argv[1], impersonate="chrome124", timeout=30)
r.raise_for_status()
open(sys.argv[2], "wb").write(r.content)
PY
python3 - "$WORK" "$BASE_PORT" <<'PY'
import base64, json, random, sys
from pathlib import Path
from scraper.vpn import build_xray_config, parse_vless

work, base = Path(sys.argv[1]), int(sys.argv[2])
text = (work / "sub.txt").read_text(errors="ignore")
lines = [l.strip() for l in text.splitlines() if l.strip()]
if not any(l.startswith("vless://") for l in lines):
    raw = base64.b64decode(text.strip() + "=" * (-len(text.strip()) % 4)).decode("utf-8", "ignore")
    lines = [l.strip() for l in raw.splitlines() if l.strip()]
links = list(dict.fromkeys(l for l in lines if l.startswith("vless://")))
random.shuffle(links)
kept = []
for link in links[:400]:
    try:
        node = parse_vless(link)
        (work / f"c{len(kept)}.json").write_text(json.dumps(build_xray_config([node], base)))
        kept.append(link)
    except Exception:  # unsupported node: skip
        continue
(work / "cands.txt").write_text("\n".join(kept) + "\n")
print(f"{len(links)} vless links in the list, {len(kept)} parseable")
PY

: > "$WORK/good.txt"
good=0
idx=0
while IFS= read -r link; do
  [ "$good" -ge "$NODES" ] && break
  if xray run -test -c "$WORK/c$idx.json" >/dev/null 2>&1; then
    echo "$link" >> "$WORK/good.txt"
    good=$((good + 1))
  fi
  idx=$((idx + 1))
done < "$WORK/cands.txt"
[ "$good" -gt 0 ] || { echo "no usable nodes"; exit 1; }

python3 - "$WORK" "$BASE_PORT" <<'PY'
import json, sys
from pathlib import Path
from scraper.vpn import build_xray_config, parse_vless

work, base = Path(sys.argv[1]), int(sys.argv[2])
nodes = [parse_vless(l) for l in (work / "good.txt").read_text().splitlines() if l.strip()]
(work / "xray.json").write_text(json.dumps(build_xray_config(nodes, base)))
PY
xray run -c "$WORK/xray.json" >"$WORK/xray.log" 2>&1 &
XRAY_PID=$!
sleep 3
kill -0 "$XRAY_PID" 2>/dev/null || { echo "xray exited:"; head -5 "$WORK/xray.log"; exit 1; }
echo "$good nodes started (SOCKS ports $BASE_PORT..$((BASE_PORT + good - 1)))"

# ---- 2. the test ----
echo
echo "running ${SECONDS_RUN}s: $CONC parallel videos through $good nodes ..."
# real (wire) traffic of this container: counters of the non-loopback interfaces before and after
wire() { awk -F'[: ]+' '$2 != "lo" && NR > 2 {rx += $3; tx += $11} END {print rx, tx}' /proc/net/dev; }
read -r RX0 TX0 < <(wire)
T0=$(date +%s)
python3 /app/scripts/yt_hot_test.py "$BASE_PORT" "$good" "$SECONDS_RUN" "$CONC"
read -r RX1 TX1 < <(wire)
SPAN=$(( $(date +%s) - T0 ))
echo "wire traffic of the whole run (incl. node start): received $(( (RX1 - RX0) / 1000000 )) MB, sent $(( (TX1 - TX0) / 1000000 )) MB in ${SPAN}s = $(( (RX1 - RX0) * 8 / 1000000 / SPAN )) Mbit/s in, $(( (TX1 - TX0) * 8 / 1000000 / SPAN )) Mbit/s out"
