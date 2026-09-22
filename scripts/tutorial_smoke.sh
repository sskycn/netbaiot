#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/netbaiot-tutorial.XXXXXX")
ADMIN=abababababababababababababababababababababababababababababababab
SECRET=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
UP=v1/t/demo/p/sensor/d/device-1/up
DOWN=v1/t/demo/p/sensor/d/device-1/down
SERVER_PID=
WEBHOOK_PID=
SUB_PID=
BUSINESS_PID=

cleanup() {
  [[ -z "$SUB_PID" ]] || kill "$SUB_PID" 2>/dev/null || true
  [[ -z "$BUSINESS_PID" ]] || kill "$BUSINESS_PID" 2>/dev/null || true
  [[ -z "$SERVER_PID" ]] || kill "$SERVER_PID" 2>/dev/null || true
  [[ -z "$WEBHOOK_PID" ]] || kill "$WEBHOOK_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  rm -rf "$RUN_DIR"
}
trap cleanup EXIT

cd "$ROOT_DIR"
command -v mosquitto_pub >/dev/null
command -v mosquitto_sub >/dev/null
cargo build --locked -p netbaiot-server -p netbaiot-cli

read -r HTTP_PORT MGMT_PORT MQTT_PORT TCP_PORT UDP_PORT WEBHOOK_PORT BUSINESS_PORT <<<"$(python3 -c '
import socket
sockets=[]
for kind in [socket.SOCK_STREAM]*4+[socket.SOCK_DGRAM,socket.SOCK_STREAM,socket.SOCK_STREAM]:
    sock=socket.socket(socket.AF_INET,kind); sock.bind(("127.0.0.1",0)); sockets.append(sock)
print(*(sock.getsockname()[1] for sock in sockets))
')"
python3 -c '
import json,sys
c=json.load(open(sys.argv[1]))
ports=list(map(int,sys.argv[4:]))
for field,port in zip(("device_http","management_http","mqtt","tcp","udp"),ports[:5]):
    c[field]=f"127.0.0.1:{port}"
c["delivery_url"]=f"http://127.0.0.1:{ports[5]}/events"
c["spool_directory"]=sys.argv[3]
json.dump(c,open(sys.argv[2],"w"))
' configs/tutorial.json "$RUN_DIR/config.json" "$RUN_DIR/spool" \
  "$HTTP_PORT" "$MGMT_PORT" "$MQTT_PORT" "$TCP_PORT" "$UDP_PORT" "$WEBHOOK_PORT" "$BUSINESS_PORT"

python3 examples/business_http_sink.py --port "$WEBHOOK_PORT" >"$RUN_DIR/webhook.log" 2>&1 &
WEBHOOK_PID=$!
NETBAIOT_ADMIN_SECRET=$ADMIN RUST_LOG=info \
  ./target/debug/netbaiot-server "$RUN_DIR/config.json" >"$RUN_DIR/server.log" 2>&1 &
SERVER_PID=$!

for _ in $(seq 1 100); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    sed -n '1,200p' "$RUN_DIR/server.log" >&2
    exit 1
  fi
  if curl --noproxy '*' -fsS "http://127.0.0.1:$MGMT_PORT/api/v1/ready" \
    -H "Authorization: Bearer $ADMIN" >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done
curl --noproxy '*' -fsS "http://127.0.0.1:$MGMT_PORT/api/v1/ready" \
  -H "Authorization: Bearer $ADMIN" | rg '"ready":true'
NETBAIOT_ENDPOINT="http://127.0.0.1:$MGMT_PORT" NETBAIOT_TOKEN="$ADMIN" \
  ./target/debug/netbaiot --output json server status | rg '"lifecycle":"running"'

# Exercise the repository's real Mosquitto client matrix after readiness.
python3 tests/mosquitto_cli_interop.py --port "$MQTT_PORT" | rg '"results":'

for qos in 0 1 2; do
  mosquitto_pub -h 127.0.0.1 -p "$MQTT_PORT" -V mqttv311 \
    -u demo-device -P "$SECRET" -i "tutorial-smoke-qos-$qos" \
    -t "$UP" -q "$qos" \
    -m "{\"schema_version\":1,\"source_message_id\":\"smoke:qos:$qos\",\"kind\":\"heartbeat\",\"data\":{\"sequence\":$qos}}"
done

curl --noproxy '*' -fsS "http://127.0.0.1:$HTTP_PORT/v1/device/data" \
  -H "Authorization: Bearer demo-device:$SECRET" \
  --data '{"schema_version":1,"source_message_id":"smoke:http","kind":"heartbeat","data":{"sequence":10}}' \
  | rg '"event_id"'
python3 examples/device_tcp.py --address "127.0.0.1:$TCP_PORT" | rg 'event_id'
python3 examples/device_udp.py --address "127.0.0.1:$UDP_PORT" --sequence 11 | rg 'intentionally has no response'

mosquitto_sub -h 127.0.0.1 -p "$MQTT_PORT" -V mqttv311 \
  -u demo-device -P "$SECRET" -i tutorial-smoke-command -t "$DOWN" -q 1 -C 1 \
  >"$RUN_DIR/command.json" &
SUB_PID=$!
sleep 0.2
curl --noproxy '*' -fsS "http://127.0.0.1:$MGMT_PORT/api/v1/devices/commands" \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  --data '{"command_id":"00000000-0000-0000-0000-000000000123","device":{"tenant_id":"demo","product_id":"sensor","device_id":"device-1"},"expires_at":null,"payload":{"name":"smoke","arguments":{}}}' \
  | rg '"state":"queued"'
wait "$SUB_PID"
SUB_PID=
rg '"name":"smoke"' "$RUN_DIR/command.json"
curl --noproxy '*' -fsS -X POST "http://127.0.0.1:$MGMT_PORT/api/v1/auth/invalidate" \
  -H "Authorization: Bearer $ADMIN" -H 'Content-Type: application/json' \
  --data '{"scope":"device","device":{"tenant_id":"demo","product_id":"sensor","device_id":"device-1"}}' \
  | rg '"invalidated_cache_entries"'

for _ in $(seq 1 100); do
  if [[ $(rg -c '"event"' "$RUN_DIR/webhook.log" || true) -ge 6 ]]; then
    break
  fi
  sleep 0.05
done
[[ $(rg -c '"event"' "$RUN_DIR/webhook.log" || true) -ge 6 ]]

curl --noproxy '*' -fsS -X POST "http://127.0.0.1:$MGMT_PORT/api/v1/drain" \
  -H "Authorization: Bearer $ADMIN" | rg '"draining":true'
wait "$SERVER_PID"
SERVER_PID=
rg 'shutdown complete' "$RUN_DIR/server.log"

# A second composition verifies the confirmed stream and all official device SDK examples.
python3 -c '
import json,sys
c=json.load(open(sys.argv[1])); c["delivery_url"]=None
c["business_tcp"]=f"127.0.0.1:{sys.argv[3]}"; c["spool_directory"]=sys.argv[4]
json.dump(c,open(sys.argv[2],"w"))
' "$RUN_DIR/config.json" "$RUN_DIR/stream-config.json" "$BUSINESS_PORT" "$RUN_DIR/stream-spool"
NETBAIOT_ADMIN_SECRET=$ADMIN NETBAIOT_BUSINESS_STREAM_TOKEN=business-stream-demo-token RUST_LOG=info \
  ./target/debug/netbaiot-server "$RUN_DIR/stream-config.json" >"$RUN_DIR/server-stream.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    sed -n '1,200p' "$RUN_DIR/server-stream.log" >&2
    exit 1
  fi
  if curl --noproxy '*' -fsS "http://127.0.0.1:$MGMT_PORT/api/v1/ready" \
    -H "Authorization: Bearer $ADMIN" >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done
python3 examples/business_tcp_client.py --address "127.0.0.1:$BUSINESS_PORT" \
  --token business-stream-demo-token --count 3 >"$RUN_DIR/business-stream.log" &
BUSINESS_PID=$!
sleep 0.2
NETBAIOT_DEVICE_CREDENTIAL_ID=demo-device NETBAIOT_DEVICE_SECRET=$SECRET \
  NETBAIOT_DEVICE_HTTP_ENDPOINT="http://127.0.0.1:$HTTP_PORT" \
  cargo run --quiet -p netbaiot-device-sdk --example device_http_upload
NETBAIOT_DEVICE_CREDENTIAL_ID=demo-device NETBAIOT_DEVICE_SECRET=$SECRET \
  NETBAIOT_DEVICE_HTTP_ENDPOINT="http://127.0.0.1:$HTTP_PORT" \
  cargo run --quiet -p netbaiot-device-sdk --example device_config_pull
NETBAIOT_DEVICE_CREDENTIAL_ID=demo-device NETBAIOT_DEVICE_SECRET=$SECRET \
  NETBAIOT_MQTT_ENDPOINT="mqtt://127.0.0.1:$MQTT_PORT" \
  cargo run --quiet -p netbaiot-device-sdk --example device_mqtt
wait "$BUSINESS_PID"
BUSINESS_PID=
[[ $(rg -c '"delivery"' "$RUN_DIR/business-stream.log") -eq 3 ]]
curl --noproxy '*' -fsS -X POST "http://127.0.0.1:$MGMT_PORT/api/v1/drain" \
  -H "Authorization: Bearer $ADMIN" | rg '"draining":true'
wait "$SERVER_PID"
SERVER_PID=
rg 'shutdown complete' "$RUN_DIR/server-stream.log"
printf '%s\n' 'tutorial smoke: PASS'
