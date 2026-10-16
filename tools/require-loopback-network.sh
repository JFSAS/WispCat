#!/bin/sh
# Fail closed when Linux network inspection is unavailable or not loopback-only.
set -eu
test "$(uname -s)" = Linux
command -v ip >/dev/null 2>&1 || { echo 'Offline checks require iproute2.' >&2; exit 1; }
offline_links=$(ip -j link show) || { echo 'Network link inspection failed.' >&2; exit 1; }
offline_addresses=$(ip -o address show scope global) || { echo 'Network address inspection failed.' >&2; exit 1; }
offline_ipv4=$(ip route show) || { echo 'IPv4 route inspection failed.' >&2; exit 1; }
offline_ipv6=$(ip -6 route show default) || { echo 'IPv6 route inspection failed.' >&2; exit 1; }
python3 - "$offline_links" "$offline_addresses" "$offline_ipv4" "$offline_ipv6" <<'PY'
import json, sys
try:
    links = json.loads(sys.argv[1])
    loopback = [link for link in links if link['ifname'] == 'lo']
    # Some Linux kernels create unconfigured fallback tunnels in every new
    # namespace. They are not active links and cannot provide an external route.
    fallback = {'tunl0': 'ipip', 'sit0': 'sit', 'ip6tnl0': 'tunnel6'}
    other = [link for link in links if link['ifname'] != 'lo']
    valid = (len(loopback) == 1 and 'LOOPBACK' in loopback[0]['flags']
             and 'UP' in loopback[0]['flags']
             and all(link['ifname'] in fallback and link.get('link_type') == fallback[link['ifname']]
                     and 'UP' not in link['flags'] and link.get('operstate') == 'DOWN'
                     and link.get('link') is None for link in other)
             and all(not value.strip() for value in sys.argv[2:]))
except (ValueError, TypeError, KeyError):
    valid = False
if not valid:
    raise SystemExit('Offline checks require only active loopback and no external routes/addresses.')
PY
