# NetbaIoT

[中文](README.zh-CN.md) · [Documentation](docs/README.md) · [Releases](https://github.com/sskycn/netbaiot/releases)

NetbaIoT is a database-free, memory-first IoT ingress gateway and real-time event
router written in Rust. It implements MQTT directly and normalizes MQTT, framed
TCP and authenticated UDP uplinks into `DeviceEvent`s for your business services.
Business systems own durable data, workflows, device configuration and offline commands.

```text
Device → transport → authentication → versioned codec → DeviceEvent
       → bounded router → independently bounded business sinks
```

## Quick Start

From a source checkout with Rust 1.88+:

```bash
cargo run --locked -p netbaiot-cli -- demo --once
```

From an extracted release archive:

```bash
./netbaiot demo --once
```

The demo authenticates a real MQTT device, publishes a heartbeat, waits for the
business sink ACK, then drains and exits. It needs no external broker, Python or
Mosquitto. Demo credentials are for local development only. Omit `--once` for an
interactive demo with a printed standard MQTT client command.

For your own configuration:

```bash
./netbaiot init my-gateway
cd my-gateway
../netbaiot config check --config netbaiot.json
../netbaiot serve --config netbaiot.json
```

See [Quick Start](docs/quick-start.md) for manual MQTT examples and
[configuration](docs/configuration.md) for production setup. Linux x86_64/ARM64
are production platforms; macOS is for development/testing and Windows is
experimental. See [platform support](docs/platform-support.md).

## Supported protocols

| Boundary | Support |
| --- | --- |
| MQTT | Embedded 3.1.1 and bounded 5.0 profile; QoS0/1/2, persistent sessions, retained messages, Will, exact/`+`/`#` subscriptions |
| TCP | Length-prefixed frames and binary EventAccepted ACK v1; shares the device TCP listener with MQTT |
| UDP | NBI1 HMAC uplink, timestamp/replay checks and signed NBA1 receipt; no encryption or downlink |
| Business | Confirmed HTTP webhook or Business RPC V4 |
| Management | Independent authenticated HTTP listener under `/api/v1` |

Payloads support JSON V1, CBOR V1, MessagePack V1, Protobuf V1 and explicitly
registered [vendor codecs](docs/vendor-codecs.md). Public device TCP requires TLS;
UDP authentication does not encrypt payloads. Full contracts are in the
[protocol matrix](docs/protocol-support.md) and [device protocol](docs/device-protocol.md).

## Delivery and commands

A device receipt means `EventAccepted`: validation, routing, count/byte admission
and atomic enqueue to every required sink have completed. It does not mean business
persistence. Required sinks explicitly ACK; retries and planned-restart replay may
duplicate the stable `event_id`, so consumers must process it idempotently.

Planned shutdown drains required work or durably commits it to the bounded local
restart spool. Commit failure keeps the process alive and unready. Abrupt crashes
can lose recent in-memory work. See [delivery semantics](docs/delivery-semantics.md)
and [restart recovery](docs/restart-spool.md).

Commands target currently connected local MQTT/TCP sessions. Offline devices return
`DEVICE_OFFLINE`; no offline commands are stored. Transport SENT, device receipt
and device execution ACK are separate. See [business integration](docs/business-integration.md).

## Current limitations

NetbaIoT is single-node: clustering and cross-node live-session routing are not
implemented. It provides no dashboard, telemetry database, OTA or configuration
reconciliation. MQTT over WebSocket, shared subscriptions, MQTT-SN, Topic Alias,
Subscription Identifier, Enhanced Authentication, bridge mode and `$SYS` services
are outside the supported profile. HTTP is management-only, not device ingress.
Current production capacity has not been established; see [measurement scope](docs/benchmarks.md).

Documentation follows `main`. The workspace version is `0.3.3`, but `main` includes
unreleased integer Decimal and diagnostic changes after the `v0.3.3` tag. Review
[version and upgrade notes](docs/README.md#versions-and-upgrades) before upgrading.

## Development and license

[CONTRIBUTING.md](CONTRIBUTING.md) covers setup and validation;
[AGENTS.md](AGENTS.md) defines architecture constraints. Read the
[security guide](docs/security.md) before deployment and use [SECURITY.md](SECURITY.md)
to report vulnerabilities privately.

Licensed under [AGPL-3.0-or-later](LICENSE). The proposed CLA is available in
[English](CLA.md) and [中文](CLA.zh-CN.md); its draft status and signing requirements
are described in [CONTRIBUTING.md](CONTRIBUTING.md).
