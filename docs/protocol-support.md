# Protocol support

[中文](protocol-support.zh-CN.md)

This page summarizes the profiles implemented by the current workspace. MQTT
details live in [MQTT profile](mqtt.md); the JSON uplink and TCP/UDP wire formats
are in [device protocol](device-protocol.md).

## Device ingress

| Protocol | Supported behavior | Boundaries |
| --- | --- | --- |
| MQTT 3.1.1 | QoS 0/1/2, CleanSession 0/1, persistent sessions, retained messages, Will, exact/`+`/`#` subscriptions, planned-restart recovery | Embedded broker; no WebSocket, shared subscriptions, MQTT-SN, bridge, `$SYS`, or clustering |
| MQTT 5.0 | QoS 0/1/2, Clean Start, session/message expiry, Will Delay, Receive Maximum, Maximum Packet Size, No Local, Retain As Published, Retain Handling, bounded supported metadata | Topic Alias, Subscription Identifier, shared subscriptions, Enhanced Authentication, and WebSocket are not supported |
| Framed TCP | Four-byte big-endian length followed by one bounded JSON v1 frame; first frame authenticates the connection; uplinks and online command path | TCP is a byte stream and must be TLS protected off loopback; no HTTP device ingress |
| Authenticated UDP | NBI1 HMAC-SHA256 uplink; credential version, timestamp and replay checks; signed 64-byte NBA1 receipt after `EventAccepted` | Authenticated but not encrypted; no long-lived session, command downlink, or fragmentation |

MQTT 3.1.1 and MQTT 5.0 run on the same device TCP listener. A version change
does not resume a session across protocol levels. The supported MQTT 5 user
properties and metadata are bounded; see the detailed profile before depending on
specific properties.

All three device uplinks use the authenticated device identity and configured
versioned codec, then converge on the public `DeviceEvent` model. The transport
protocol does not become part of the event's business payload.

## MQTT 5.0 property matrix

| MQTT 5 capability | Status |
| --- | --- |
| Session Expiry Interval and Clean Start | Supported with bounded session state |
| Message Expiry Interval | Supported; stored as a deadline and expired messages are removed |
| Will Delay Interval | Supported with bounded pending Will state |
| Receive Maximum and Maximum Packet Size | Supported and bounded in both directions |
| No Local, Retain As Published, Retain Handling | Supported subscription options |
| Payload Format Indicator and Content Type | Supported as bounded transport metadata |
| Response Topic, Correlation Data, User Properties | Supported as bounded transport metadata |
| Topic Alias | Not supported |
| Subscription Identifier | Not supported |
| Shared subscription filters | Not supported |
| Enhanced Authentication | Not supported |
| WebSocket transport | Not supported |

Unsupported capabilities are not silently accepted as active behavior. The broker
uses MQTT 5 reason codes where the protocol allows it. Exact packet/property
behavior and recovery compatibility are documented in [mqtt.md](mqtt.md).

## Authentication and transport protection

MQTT/TCP authentication happens at connection setup; the resulting immutable
authenticated device identity is bound to that connection. Normal MQTT packets and
TCP frames do not call a remote authentication service. UDP verifies each datagram
with HMAC and bounded timestamp/replay policy. Public TCP streams require TLS;
HMAC does not encrypt UDP payloads.

Management HTTP is a separate listener with separate authorization. It is not a
device protocol, and device credentials do not authorize management operations.

## Business egress

The server composes bounded business sinks. Confirmed HTTP webhook delivery is
acknowledged by configured 2xx status. Confirmed framed TCP/RPC delivery requires an
application `ACK event_id`. A socket write alone is not a business acknowledgement.
Best-effort sinks follow their configured bounded overflow policy. See
[delivery semantics](delivery-semantics.md) and
[business integration](business-integration-guide.md).

## Current business and recovery formats

| Surface | Supported format | Rejected formats |
| --- | --- | --- |
| Business RPC | V3 only | V1, V2 and unknown versions |
| MQTT restart recovery | NBMQ v6 only | NBMQ v1–v5 and unknown versions |
| EventBus restart spool | NBSP v3 only | NBSP v1/v2 and unknown versions |
| Device MQTT | 3.1.1 and 5.0 | See the implemented profile above |
| Management HTTP | `/api/v1/...` | Independent of RPC wire version |

Current wire/file bytes remain unchanged. Old clients must upgrade and old files must be completed or converted by a suitable previous version before upgrade. No runtime converter or fallback is provided. See [breaking change and upgrade](migration/current-protocol-only.md).
