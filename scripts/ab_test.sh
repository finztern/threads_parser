#!/bin/bash
# Unattended A/B/A/B test of one config.yaml value (run on the host, from the repo directory):
#   nohup setsid bash scripts/ab_test.sh static_proxy_share 0.75 0.9 8 30 > ~/ab_results.txt 2>&1 &
# args: key valueA valueB hours block_minutes
# Each block: set the value (in place, hot reload), wait 6 min (warm-up, not measured), then measure the growth of the mock
# collector's "total accepted" counter until the block ends. At the end the better value stays in config.yaml.
# Guard: a block below MIN_RATE new/s puts value A (the baseline) back for the next block.
set -u
KEY="${1:?key}"; A="${2:?valueA}"; B="${3:?valueB}"; HOURS="${4:-8}"; BLOCK_MIN="${5:-30}"
MIN_RATE="${MIN_RATE:-4}"
WARM=360
cd "$(dirname "$0")/.." || exit 1

total() {  # current "total accepted" of the mock collector
  docker compose logs collector --tail 40 2>/dev/null | grep -oE 'total accepted=[0-9]+' | tail -1 | grep -oE '[0-9]+'
}
share_line() {  # share of requests that left through vpn+static in the last by-source line
  docker compose logs parser --since 5m 2>&1 | grep 'by source' | tail -1 | awk '
    { for (i = 1; i <= NF; i++) if ($i ~ /^(tor|static|vpn)$/ && $(i+1) ~ /^req=/) { split($(i+1), a, "="); n[$i] = a[2] } }
    END { t = n["tor"] + n["static"] + n["vpn"]; if (t > 0) printf "%.0f%%", 100 * (n["static"] + n["vpn"]) / t; else printf "n/a" }'
}
cfg_in_container() {  # value of the key as the PARSER CONTAINER sees it (a single-file bind mount can go stale)
  docker compose exec -T parser grep -E "^${KEY}:" /app/config.yaml 2>/dev/null | head -1 | awk '{print $2}'
}
set_and_verify() {  # set the value on the host, then make sure the container really got it (recreate it if not)
  python3 scripts/setcfg.py "$KEY" "$1" > /dev/null
  sleep 20
  local seen; seen=$(cfg_in_container)
  if [ "$seen" != "$1" ]; then
    echo "  container sees $KEY=$seen, expected $1: stale bind mount, recreating the parser"
    docker compose up -d --force-recreate parser > /dev/null 2>&1
    sleep 30
    seen=$(cfg_in_container)
    [ "$seen" = "$1" ] || echo "  WARNING: container still sees $KEY=$seen, this block is NOT valid"
  fi
}
declare -A SUM CNT
SUM[$A]=0; SUM[$B]=0; CNT[$A]=0; CNT[$B]=0
blocks=$(( HOURS * 60 / BLOCK_MIN ))
current="$A"
echo "A/B test: $KEY $A vs $B, $blocks blocks of $BLOCK_MIN min, started $(date -u +%H:%M) UTC"
for ((i = 0; i < blocks; i++)); do
  if (( i % 2 == 0 )); then value="$A"; else value="$B"; fi
  [ "$current" = "$A" ] && [ "$value" != "$A" ] || true
  set_and_verify "$value"
  sleep $(( WARM - 50 ))
  t0=$(total); s0=$(date +%s)
  sleep $(( BLOCK_MIN * 60 - WARM ))
  t1=$(total); s1=$(date +%s)
  if [ -z "$t0" ] || [ -z "$t1" ] || [ "$s1" -le "$s0" ]; then echo "block $i $KEY=$value: no counter"; continue; fi
  rate=$(awk -v a="$t0" -v b="$t1" -v s="$((s1 - s0))" 'BEGIN { printf "%.2f", (b - a) / s }')
  share=$(share_line)
  cpu=$(docker stats --no-stream --format '{{.Name}}={{.CPUPerc}}' 2>/dev/null | grep -E 'parser|tor' | tr '\n' ' ')
  echo "$(date -u +%H:%M) block $i $KEY=$value: $rate new/s, requests via vpn+static $share, $cpu"
  SUM[$value]=$(awk -v a="${SUM[$value]}" -v r="$rate" 'BEGIN { print a + r }')
  CNT[$value]=$(( CNT[$value] + 1 ))
  if awk -v r="$rate" -v m="$MIN_RATE" 'BEGIN { exit !(r < m) }'; then
    echo "  rate below $MIN_RATE: baseline $A restored for the next block"
    set_and_verify "$A"
  fi
done
meanA=$(awk -v s="${SUM[$A]}" -v c="${CNT[$A]}" 'BEGIN { if (c > 0) printf "%.2f", s / c; else print 0 }')
meanB=$(awk -v s="${SUM[$B]}" -v c="${CNT[$B]}" 'BEGIN { if (c > 0) printf "%.2f", s / c; else print 0 }')
echo "mean new/s: $KEY=$A -> $meanA (${CNT[$A]} blocks), $KEY=$B -> $meanB (${CNT[$B]} blocks)"
if awk -v a="$meanA" -v b="$meanB" 'BEGIN { exit !(b > a * 1.03) }'; then best="$B"; else best="$A"; fi
set_and_verify "$best"
echo "kept $KEY=$best (B must beat A by more than 3%); finished $(date -u +%H:%M) UTC"
