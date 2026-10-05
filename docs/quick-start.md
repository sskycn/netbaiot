# 5-minute Quick Start

This walkthrough starts NetbaIoT and a dependency-free webhook receiver, sends a
real MQTT publish, and shows the normalized `DeviceEvent`. It uses the checked-in
tutorial device credential on loopback. Do not reuse that credential or the
bootstrap management token outside local development.

## Requirements

- Rust 1.88 or newer
- Python 3
- Mosquitto client tools (`mosquitto_pub`)

Mosquitto is only a client in this walkthrough. NetbaIoT implements its own MQTT
3.1.1 and MQTT 5.0 broker. No database or external broker is used.
The first Cargo build may take longer than five minutes if dependencies are not
cached yet.

## Start the gateway and webhook

From the repository root, in Terminal 1:

```bash
./scripts/demo/start.sh
```

The script builds and starts the server, starts
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
