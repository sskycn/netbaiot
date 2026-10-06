# 5-minute Quick Start

The one-command demo below needs only the NetbaIoT binary. The manual walkthrough starts a webhook receiver, sends a
real MQTT publish, and shows the normalized `DeviceEvent`. It uses the checked-in
tutorial device credential on loopback. Do not reuse that credential or the
bootstrap management token outside local development.

## One-command demo

A binary archive needs no Python, Rust or external broker for this path:

```bash
./netbaiot demo
./netbaiot demo --once
```

Windows PowerShell uses `.\netbaiot.exe demo` or `.\netbaiot.exe demo --once`.
For source use `cargo run --locked -p netbaiot-cli -- demo --once` (Rust 1.88+).
The demo uses dynamic loopback ports and a private temporary recovery directory.
It proves MQTT authentication, QoS1 PUBACK/EventAccepted, normalized heartbeat
delivery, and the configured HTTP 204 sink ACK. The receiver is development-only,
keeps no durable business history, and prints only event metadata. The sample
credentials must never be reused for production.

Without `--once`, the demo keeps running for your own MQTT client and prints a
`mosquitto_pub` command using its actual device port. Ctrl-C (or SIGTERM on Unix)
uses the same drain/spool lifecycle as `netbaiot serve` and `netbaiot-server`.
The sink stays alive through gateway drain; temporary files are removed afterward.

```bash
./netbaiot --help
./netbaiot version
./netbaiot config limits
./netbaiot config check --config configs/development.json
./netbaiot serve --config configs/development.json
```

`config check` performs local JSON, static, environment-secret-source and PEM
checks without binding ports, reading/rewriting recovery snapshots, starting workers
or calling remote providers/sinks. Passing it does not reserve a port or prove
remote reachability. See [CLI diagnostics](cli.md).

## Manual MQTT example

The remaining walkthrough uses the preserved shell/manual integration helper. It
requires Python and Mosquitto clients; these are optional for the one-command demo.

## Requirements

- Rust 1.88 or newer **only for a source checkout**
- Python 3.9 or newer
- Mosquitto client tools (`mosquitto_pub`)

Mosquitto is only a client in this walkthrough. NetbaIoT implements its own MQTT
3.1.1 and MQTT 5.0 broker. No database or external broker is used.
The first Cargo build may take longer than five minutes if dependencies are not
cached yet.

## Start the gateway and webhook

On macOS/Linux, from the repository root or extracted binary package directory,
in Terminal 1:

```bash
./scripts/demo/start.sh
```

The script uses the packaged `netbaiot-server` when present; in a source checkout
it builds `target/debug/netbaiot-server` with Cargo. It starts
[`examples/business_http_sink.py`](../examples/business_http_sink.py), binds all
listeners to `127.0.0.1`, and puts restart files under a temporary directory. The
webhook prints each accepted JSON event and returns HTTP 204. The tutorial's fixed
ports are `8080` (device TCP and UDP), `9090` (management HTTP), and `18080`
(webhook). Stop anything already using those ports before starting the demo.
Wait for the `runtime ready` log before publishing.

The included development device is:

```text
tenant / product / device: demo / sensor / device-1
credential id:             demo-device
credential secret:         000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
```

The gateway also sets a predictable local bootstrap management token for this
demo. It is not needed to publish an MQTT event. Production should use a scoped
management identity, provisioned device credentials, and TLS on non-loopback
listeners. See [security](security.md) and the [operations guide](operations-guide.md).

## Publish an event

In Terminal 2, publish with a standard MQTT 3.1.1 client:

```bash
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  -i quickstart -t v1/t/demo/p/sensor/d/device-1/up -q 1 \
  -m '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'
```

The webhook terminal prints an event with this shape (the gateway-generated ID
and receive time vary). This is the HTTP webhook envelope; the server maps the
public Rust `DeviceEvent` into these flattened fields:

```json
{
  "event_id": "2b13d944-bb18-40df-8043-a636807fc023",
  "source_message_id": "demo:1",
  "tenant_id": "demo",
  "product_id": "sensor",
  "device_id": "device-1",
  "event_type": "heartbeat",
  "received_at": 1791203077827,
  "occurred_at": null,
  "payload": { "kind": "heartbeat", "data": { "sequence": 1 } }
}
```

QoS 1 PUBACK means NetbaIoT accepted the event at the `EventAccepted` boundary.
It does not mean a business database committed it. The demo webhook's HTTP 204 is
the configured sink acknowledgement. Read [delivery semantics](delivery-semantics.md)
before relying on those receipts.

To use MQTT 5.0, run the same command with `-V mqttv5`. The supported MQTT 5.0
properties and exclusions are listed in the [protocol support matrix](protocol-support.md).

## Windows manual start

The Bash script above is the macOS/Linux entry point. On Windows, extract the
Windows archive and open PowerShell terminals in its package directory. Install
Python 3.9+ and Mosquitto client tools. Rust is only needed if building from source.

Terminal 1 runs the webhook:

```powershell
py -3 -u examples/business_http_sink.py --listen 127.0.0.1 --port 18080
```

Terminal 2 runs the packaged server with the same loopback tutorial config:

```powershell
$env:NETBAIOT_ADMIN_SECRET = "abababababababababababababababababababababababababababababababab"
$env:RUST_LOG = "info"
.\netbaiot-server.exe configs/tutorial.json
```

For a source checkout, first run `cargo build --locked -p netbaiot-server`, then
use `.\target\debug\netbaiot-server.exe configs/tutorial.json` instead. Wait for
`runtime ready`. Terminal 3 publishes using a file so Windows shell argument
quoting does not change the JSON:

```powershell
'{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}' | Set-Content -Encoding ascii demo-event.json
mosquitto_pub.exe -h 127.0.0.1 -p 8080 -V mqttv311 -u demo-device -P 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f -i quickstart -t v1/t/demo/p/sensor/d/device-1/up -q 1 -f demo-event.json
```

The webhook prints the event in Terminal 1. Stop the server with Ctrl-C and wait
for graceful shutdown, then stop the webhook. This manual configuration puts
restart files in `var/tutorial-restart-spool`; unlike the Bash demo, it does not
remove that directory automatically. These credentials are local tutorial values.

## Try framed TCP or authenticated UDP

The same development listener accepts the repository's sample clients:

```bash
python3 examples/device_tcp.py --address 127.0.0.1:8080
python3 examples/device_udp.py --address 127.0.0.1:8080 --sequence 2
```

Both messages reach the same webhook as MQTT events. TCP uses a length-prefixed
frame and an authentication handshake. UDP signs each NBI1 datagram with HMAC and
validates the signed NBA1 acceptance receipt. **UDP authentication is not
encryption**: its payload is visible to observers on the network. UDP has no
session or command downlink. These examples use the development credential and
must stay on loopback.

## Stop

Press Ctrl-C in Terminal 1 and wait for the server to finish graceful shutdown.
The demo deletes its temporary configuration and spool directory on exit. A
graceful restart can drain or spool already accepted required deliveries; an
arbitrary process or machine crash can still lose work held only in memory.

For device commands, client APIs, persistent MQTT sessions, TLS setup, and
operations, continue with the [10-minute end-to-end tutorial](getting-started.md),
[device protocol](device-protocol.md), and [operations guide](operations-guide.md).
