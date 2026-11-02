#!/bin/bash
# Make the host accept every address of your IPv6 /64 (run once as root, survives reboots):
#   sudo bash scripts/ipv6_anyip.sh 2a01:ecc0:6000:1b6
# Install: a oneshot systemd unit adds the "local" route on lo at boot; sysctl.d keeps ip_nonlocal_bind.
# If the provider answers NDP per address, install ndppd as well (see README).
set -euo pipefail
PREFIX="${1:?usage: $0 <first 4 groups of the /64, e.g. 2a01:ecc0:6000:1b6>}"
cat > /etc/sysctl.d/99-ipv6-anyip.conf <<CONF
net.ipv6.ip_nonlocal_bind = 1
CONF
sysctl --system >/dev/null
cat > /etc/systemd/system/ipv6-anyip.service <<UNIT
[Unit]
Description=Accept the whole IPv6 /64 locally (source address rotation)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/sbin/ip -6 route replace local ${PREFIX}::/64 dev lo
ExecStop=/sbin/ip -6 route del local ${PREFIX}::/64 dev lo

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now ipv6-anyip.service
echo "ok: ${PREFIX}::/64 is local now"
