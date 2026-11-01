#!/bin/bash
# Runs TOR_INSTANCES independent Tor clients: SOCKS ports TOR_BASE_PORT .. TOR_BASE_PORT+N-1.
# Each instance is restarted by its own loop if it dies. State lives in /var/lib/tor (a volume),
# so a restart reuses the cached consensus instead of downloading it again.
set -u
N="${TOR_INSTANCES:-20}"
BASE="${TOR_BASE_PORT:-9050}"
EXIT_NODES="${TOR_EXIT_NODES:-}"   # e.g. "{de},{nl},{se}" (empty = any exit)

mkdir -p /var/lib/tor /etc/tor/instances
chown debian-tor:debian-tor /var/lib/tor

for ((i = 0; i < N; i++)); do
    port=$((BASE + i))
    dir="/var/lib/tor/i${i}"
    conf="/etc/tor/instances/${i}.torrc"
    mkdir -p "$dir"
    chown debian-tor:debian-tor "$dir"
    chmod 700 "$dir"
    {
        echo "User debian-tor"
        echo "DataDirectory $dir"
        echo "SocksPort 0.0.0.0:${port} IsolateSOCKSAuth"
        echo "SocksPolicy accept *:*"
        echo "ControlPort 0"
        echo "ClientOnly 1"
        echo "AvoidDiskWrites 1"
        echo "Log warn stderr"
        if [ -n "$EXIT_NODES" ]; then
            echo "ExitNodes ${EXIT_NODES}"
            echo "StrictNodes 1"
        fi
    } > "$conf"
    (
        while true; do
            tor -f "$conf" 2>&1 | sed -u "s/^/[tor ${port}] /"
            sleep 5
        done
    ) &
    sleep 0.3  # stagger the bootstrap so that the directory servers are not hit by N clients at once
done
echo "started ${N} tor instances on ports ${BASE}-$((BASE + N - 1)); bootstrap takes a few minutes"
wait
