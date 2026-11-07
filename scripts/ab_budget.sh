#!/usr/bin/env bash
# A/B test of the request budget (profiles vs hot post pages), measured on the mock collector's accepted counter.
# The regulator (auto_budget) is noisy (the same budget gave 10..35 new/s), a fixed alternating test is not.
#
# Prerequisites (once): auto_budget: false in config.yaml, then `docker compose restart parser`.
# Run in the background so closing the terminal does not stop it (about 2 h with the defaults):
#   nohup bash scripts/ab_budget.sh > data/ab_budget.log 2>&1 &
#   tail -f data/ab_budget.log
#
# Env: VARIANTS="2800:1800 800:4000"  (profiles:hot per cycle)  MINUTES=20 (per slot)  ROUNDS=3  SETTLE=4 (minutes ignored after a switch)
set -euo pipefail
cd "$(dirname "$0")/.."

VARIANTS=${VARIANTS:-"2800:1800 800:4000"}
MINUTES=${MINUTES:-20}
ROUNDS=${ROUNDS:-3}
SETTLE=${SETTLE:-4}

set_value() { # key value [nested]
  local key=$1 value=$2 nested=${3:-}
  if [ -n "$nested" ]; then
    grep -q "^  ${key}:" config.yaml && sed -i "s/^  ${key}:.*/  ${key}: ${value}/" config.yaml || { echo "no '  ${key}:' in config.yaml"; exit 1; }
  else
    if grep -q "^${key}:" config.yaml; then sed -i "s/^${key}:.*/${key}: ${value}/" config.yaml; else echo "${key}: ${value}" >> config.yaml; fi
  fi
}

accepted() { # total items the mock has accepted so far
  docker compose exec -T collector python3 -c "import urllib.request,json;print(json.load(urllib.request.urlopen('http://127.0.0.1:9000/stats'))['totals'].get('to_gpu',0))"
}

if grep -q "^auto_budget: *true" config.yaml || ! grep -q "^auto_budget: *false" config.yaml; then
  echo "set 'auto_budget: false' in config.yaml and restart the parser first"; exit 1
fi

declare -A SUM COUNT
for round in $(seq 1 "$ROUNDS"); do
  for variant in $VARIANTS; do
    profiles=${variant%%:*}; hot=${variant##*:}
    set_value max_targets_per_cycle "$profiles" nested
    set_value hot_posts_per_cycle "$hot"
    echo "$(date '+%F %T') round $round: profiles=$profiles hot=$hot (settling ${SETTLE} min)"
    sleep $((SETTLE * 60))
    start=$(accepted); t0=$(date +%s)
    sleep $(((MINUTES - SETTLE) * 60))
    end=$(accepted); t1=$(date +%s)
    rate=$(python3 -c "print(round(($end - $start) / ($t1 - $t0), 2))")
    echo "$(date '+%F %T')   -> accepted ${rate}/s over $((t1 - t0)) s"
    SUM[$variant]=$(python3 -c "print(${SUM[$variant]:-0} + $rate)")
    COUNT[$variant]=$(( ${COUNT[$variant]:-0} + 1 ))
  done
done

echo; echo "=== SUMMARY (mean accepted items/s per variant) ==="
for variant in $VARIANTS; do
  python3 -c "print('profiles:hot = ${variant}  ->  ' + str(round(${SUM[$variant]} / ${COUNT[$variant]}, 2)) + '/s  (' + '${COUNT[$variant]}' + ' slots)')"
done
echo "Put the winner into config.yaml (max_targets_per_cycle / hot_posts_per_cycle); auto_budget stays false."
