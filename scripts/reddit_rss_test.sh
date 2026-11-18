#!/usr/bin/env bash
# Quick test (not a parser): does Reddit return the comment RSS feeds of ~50 busy subreddits through the free vless nodes?
#
# Run inside the parser image (it has xray and the scraper package; requests go through curl_cffi, Chrome TLS impersonation):
#   docker compose run --rm --no-deps -v "$PWD/scripts:/app/scripts:ro" parser bash /app/scripts/reddit_rss_test.sh
# Optional env: NODES=30 (nodes to start)  PAR=8 (parallel requests)  LIMIT=100 (comments per feed)  VPN_SOURCE=<subscription url>
#
# Pass 1 sends every subreddit through its own node, pass 2 retries the failures on another node, so you can tell
# "Reddit blocks everything" from "some nodes are burned".
set -u

NODES="${NODES:-30}"
PAR="${PAR:-8}"
LIMIT="${LIMIT:-100}"
VPN_SOURCE="${VPN_SOURCE:-https://github.com/Au1rxx/free-vpn-subscriptions/raw/main/output/v2ray-base64.txt}"
BASE_PORT=13000
SUBS="AskReddit politics worldnews news funny gaming pics todayilearned movies memes interestingasfuck mildlyinteresting
Showerthoughts technology science aww music sports nba soccer nfl television Futurology space wallstreetbets
CryptoCurrency Bitcoin stocks investing personalfinance relationship_advice AmItheAsshole tifu unpopularopinion
explainlikeimfive LifeProTips NoStupidQuestions OutOfTheLoop pcmasterrace Fauxmoi Conservative PublicFreakout facepalm
mildlyinfuriating AskMen AskWomen CasualConversation teenagers food buildapc Android"

WORK="$(mktemp -d)"
trap 'kill $(jobs -p) 2>/dev/null; rm -rf "$WORK"' EXIT
export PYTHONPATH="/app:${PYTHONPATH:-}"
cd /app 2>/dev/null || true

python3 -c "import curl_cffi" 2>/dev/null || { echo "curl_cffi is not available (run this inside the parser image)"; exit 1; }
command -v xray >/dev/null 2>&1 || { echo "xray is not available (run this inside the parser image)"; exit 1; }

# ---- 1. nodes: download the list, test every node on its own (one bad node breaks the whole xray config) ----
python3 - "$VPN_SOURCE" "$WORK/sub.txt" <<'PY' || { echo "cannot download $VPN_SOURCE"; exit 1; }
import sys
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

# ---- 2. requests ----
# Every request is curl_cffi with Chrome TLS/HTTP2 impersonation (chrome124), same as the parser itself.
cat > "$WORK/fetch.py" <<'PY'
import sys, time
from curl_cffi import requests

sub, port, out, limit = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
proxies = None if port == "direct" else {"http": f"socks5h://127.0.0.1:{port}", "https": f"socks5h://127.0.0.1:{port}"}
urls = [f"https://www.reddit.com/r/{sub}/comments/.rss?limit={limit}", f"https://www.reddit.com/r/{sub}/comments.rss?limit={limit}"]
headers = {"Accept": "application/atom+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
t0 = time.time()
code, body = 0, b""
for url in urls:  # second form is tried only on 404
    try:
        r = requests.get(url, impersonate="chrome", proxies=proxies, headers=headers, timeout=25)
        code, body = r.status_code, r.content
    except Exception:
        code, body = 0, b""
    if code != 404:
        break
open(out + ".body", "wb").write(body)
open(out + ".meta", "w").write(f"{code:03d} {time.time() - t0:.2f} {len(body)} {port}\n")
PY

# fetch SUB [NODE_INDEX|direct] OUT_PREFIX -> OUT_PREFIX.body and OUT_PREFIX.meta ("code seconds bytes port")
fetch() {
  local sub="$1" node="$2" out="$3" port="direct"
  [ "$node" != "direct" ] && port=$((BASE_PORT + node))
  python3 "$WORK/fetch.py" "$sub" "$port" "$out" "$LIMIT" 2>/dev/null
  [ -s "$out.meta" ] || echo "000 0 0 $port" > "$out.meta"
}

classify() {  # classify OUT_PREFIX -> ok | http_NNN | timeout_or_conn | 200_not_a_feed
  local code
  read -r code _ < "$1.meta"
  if [ "$code" = "200" ]; then
    grep -q "<feed" "$1.body" 2>/dev/null && echo ok || echo "200_not_a_feed"
  elif [ "$code" = "000" ]; then
    echo "timeout_or_conn"
  else
    echo "http_$code"
  fi
}

run_pass() {  # run_pass NAME NODE_SHIFT SUB...
  local pass="$1" shift_n="$2" i=0 sub pids=()
  shift 2
  for sub in "$@"; do
    fetch "$sub" "$(((i + shift_n) % good))" "$WORK/$pass.$sub" &
    pids+=($!)
    i=$((i + 1))
    while [ "$(jobs -rp | wc -l)" -gt "$PAR" ]; do sleep 0.2; done  # xray is one of the jobs
  done
  wait "${pids[@]}" 2>/dev/null
}

echo
echo "direct from this machine (no VPN), multi-feed AskReddit+news+worldnews+funny+gaming:"
fetch "AskReddit+news+worldnews+funny+gaming" direct "$WORK/direct"
echo "  -> $(classify "$WORK/direct") ($(cut -d' ' -f1,2 "$WORK/direct.meta" | tr ' ' '/'))"

read -r -a ALL_SUBS <<< "$(echo $SUBS)"
echo
echo "pass 1: ${#ALL_SUBS[@]} subreddits, each through its own node ..."
run_pass p1 0 "${ALL_SUBS[@]}"

FAILED=()
for sub in "${ALL_SUBS[@]}"; do
  [ "$(classify "$WORK/p1.$sub")" = "ok" ] || FAILED+=("$sub")
done
if [ "${#FAILED[@]}" -gt 0 ]; then
  echo "pass 2: retrying ${#FAILED[@]} failed ones on another node ..."
  run_pass p2 11 "${FAILED[@]}"
fi

# ---- 3. summary ----
ok1=0; ok2=0; total_entries=0; feeds=0; sum_time=0; sum_bytes=0
declare -A reasons1=() reasons2=()
OK_LIST=()
for sub in "${ALL_SUBS[@]}"; do
  pre="$WORK/p1.$sub"
  kind="$(classify "$pre")"
  if [ "$kind" = "ok" ]; then
    ok1=$((ok1 + 1))
  else
    reasons1[$kind]=$(( ${reasons1[$kind]:-0} + 1 ))
    if [ -f "$WORK/p2.$sub.meta" ]; then
      pre="$WORK/p2.$sub"
      kind2="$(classify "$pre")"
      if [ "$kind2" = "ok" ]; then ok2=$((ok2 + 1)); kind="ok"; else reasons2[$kind2]=$(( ${reasons2[$kind2]:-0} + 1 )); fi
    fi
  fi
  if [ "$kind" = "ok" ]; then
    n="$(grep -o '<entry>' "$pre.body" | wc -l)"
    total_entries=$((total_entries + n))
    feeds=$((feeds + 1))
    read -r _ t b _ < "$pre.meta"
    sum_time="$(awk -v a="$sum_time" -v b="$t" 'BEGIN{print a + b}')"
    sum_bytes=$((sum_bytes + b))
    OK_LIST+=("$pre.body|$sub")
  fi
done

echo
echo "================ RESULT ================"
echo "nodes: $good | subreddits: ${#ALL_SUBS[@]} | comments per feed requested: $LIMIT"
printf "pass 1 OK: %d/%d" "$ok1" "${#ALL_SUBS[@]}"
for k in "${!reasons1[@]}"; do printf " | %s:%d" "$k" "${reasons1[$k]}"; done; echo
if [ "${#FAILED[@]}" -gt 0 ]; then
  printf "pass 2 (retry on another node): recovered %d of %d" "$ok2" "${#FAILED[@]}"
  for k in "${!reasons2[@]}"; do printf " | still %s:%d" "$k" "${reasons2[$k]}"; done; echo
fi
if [ "$feeds" -gt 0 ]; then
  awk -v f="$feeds" -v e="$total_entries" -v t="$sum_time" -v b="$sum_bytes" -v s="${#ALL_SUBS[@]}" \
    'BEGIN{printf "feeds OK: %d/%d | comments received: %d (avg %.0f per feed) | avg %.1f s, %.0f KB per feed\n", f, s, e, e/f, t/f, b/f/1024}'
else
  echo "no feed came back: Reddit blocks these nodes"
fi

# ---- 4. a few sample comments so you can see the data is real ----
if [ "${#OK_LIST[@]}" -gt 0 ]; then
  echo
  echo "sample comments (author | text):"
  shown=0
  for item in "${OK_LIST[@]}"; do
    body="${item%%|*}"; sub="${item##*|}"
    tr '\n' ' ' < "$body" | sed 's/<entry>/\n<entry>/g' | tail -n +2 | head -2 | while IFS= read -r entry; do
      author="$(printf '%s' "$entry" | sed -n 's#.*<author><name>/\{0,1\}\(u/\)\{0,1\}\([^<]*\)</name>.*#\2#p')"
      text="$(printf '%s' "$entry" | sed -n 's#.*<content type="html">\(.*\)</content>.*#\1#p' \
        | sed 's/&lt;/</g; s/&gt;/>/g; s/<[^>]*>//g; s/&amp;/\&/g; s/&quot;/"/g; s/&#39;/'"'"'/g; s/&#32;/ /g; s/  */ /g' | cut -c1-110)"
      echo "  r/$sub | ${author:-?} | $text"
    done
    shown=$((shown + 1))
    [ "$shown" -ge 4 ] && break
  done
fi
