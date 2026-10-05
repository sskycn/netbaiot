#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/../.." && pwd)
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/netbaiot-demo.XXXXXX")
SINK_PID=

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [[ -n "$SINK_PID" ]]; then
    kill "$SINK_PID" 2>/dev/null || true
    wait "$SINK_PID" 2>/dev/null || true
  fi
  rm -rf "$RUN_DIR"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

command -v cargo >/dev/null || { echo "cargo is required" >&2; exit 1; }
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }

cd "$ROOT_DIR"
python3 - "$ROOT_DIR/configs/tutorial.json" "$RUN_DIR/config.json" "$RUN_DIR/spool" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    config = json.load(source)
config["spool_directory"] = sys.argv[3]
with open(sys.argv[2], "w", encoding="utf-8") as output:
    json.dump(config, output)
PY

# This credential is for the loopback tutorial only; do not inherit an operator token.
export NETBAIOT_ADMIN_SECRET=abababababababababababababababababababababababababababababababab
python3 -u examples/business_http_sink.py --listen 127.0.0.1 --port 18080 &
SINK_PID=$!

SINK_READY=0
for _ in {1..50}; do
  if ! kill -0 "$SINK_PID" 2>/dev/null; then
    echo "demo webhook did not start; check whether port 18080 is already in use" >&2
    exit 1
  fi
  if python3 -c 'import socket; s=socket.create_connection(("127.0.0.1", 18080), timeout=0.1); s.close()' 2>/dev/null; then
    SINK_READY=1
    break
  fi
  sleep 0.1
done
if [[ "$SINK_READY" != 1 ]]; then
  echo "timed out waiting for the demo webhook on 127.0.0.1:18080" >&2
  exit 1
fi

printf '%s\n' \
  'NetbaIoT demo uses loopback listeners and tutorial credentials.' \
  'MQTT/TCP/UDP: 127.0.0.1:8080   management HTTP: 127.0.0.1:9090' \
  'Webhook:       127.0.0.1:18080/events' \
  'Wait for the runtime ready log, then publish from another terminal; Ctrl-C stops the demo.'

cargo run --locked -p netbaiot-server -- "$RUN_DIR/config.json"
