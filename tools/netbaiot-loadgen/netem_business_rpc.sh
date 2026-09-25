#!/usr/bin/env bash
# Linux-only packet delay/loss gate. Run inside an isolated network namespace.
set -euo pipefail
if [[ $(uname -s) != Linux || $# -lt 4 || $3 != -- ]]; then
  echo 'usage: netem_business_rpc.sh RTT_MS LOSS_PERCENT -- command [args...]' >&2
  exit 2
fi
if [[ ${EUID} -ne 0 ]]; then
  echo 'tc netem requires root or CAP_NET_ADMIN in an isolated namespace' >&2
  exit 2
fi
rtt_ms=$1
loss_percent=$2
shift 3
if [[ ! $rtt_ms =~ ^[0-9]+$ || ! $loss_percent =~ ^([0-9]+)(\.[0-9]+)?$ ]]; then
  echo 'invalid RTT or loss' >&2
  exit 2
fi
device=${NETBAIOT_NETEM_DEVICE:-lo}
if tc qdisc show dev "$device" | grep -Eq 'netem| tbf | htb '; then
  echo "existing qdisc on $device; refusing to replace it" >&2
  exit 2
fi
cleanup() { tc qdisc del dev "$device" root 2>/dev/null || true; }
trap cleanup EXIT INT TERM
tc qdisc add dev "$device" root netem delay "$((rtt_ms / 2))ms" loss "${loss_percent}%"
"$@"
