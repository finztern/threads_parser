#!/usr/bin/env bash
# Put ONE vless:// link (read from stdin) into home/vless_urlcheck.txt (not in git, mode 600), start the local SOCKS service
# `urlcheck-socks` (127.0.0.1:10809) and check that the exit works and url-check.exorde.network answers through it.
#   bash home/urlcheck_up.sh <<'LINK'
#   vless://...
#   LINK
set -euo pipefail
cd "$(dirname "$0")"
read -r LINK || true
case "${LINK:-}" in vless://*) ;; *) echo "stdin must contain a vless:// link"; exit 1 ;; esac
(umask 077; printf '%s\n' "$LINK" > vless_urlcheck.txt)
docker compose up -d --build urlcheck-socks
echo "waiting for the SOCKS port 10809 ..."
for _ in $(seq 1 30); do
  (echo > /dev/tcp/127.0.0.1/10809) 2>/dev/null && break
  sleep 1
done
(echo > /dev/tcp/127.0.0.1/10809) 2>/dev/null || { echo "port 10809 is not open: docker compose logs urlcheck-socks"; exit 1; }
echo "exit IP through the vless: $(curl -s -m 20 -x socks5h://127.0.0.1:10809 https://api.ipify.org || echo 'no answer')"
echo "url-check through the vless: $(curl -s -m 20 -x socks5h://127.0.0.1:10809 -X POST https://url-check.exorde.network/v1/check \
  -H 'content-type: application/json' -d '{"urls":["https://www.reddit.com/r/test/comments/aaaaaa/x/bbbbbb/"]}' | cut -c1-200 || echo 'no answer')"
