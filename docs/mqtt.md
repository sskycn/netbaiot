# Embedded MQTT 3.1.1 and MQTT 5.0 broker

## Single Device Ingress

`device_ingress` binds one TCP listener and one UDP socket at the same address and
numeric port (development: `127.0.0.1:8080`; production example: `0.0.0.0:443`).
TCP serves standard MQTT 3.1.1 or MQTT 5.0 over TLS, and generic framed TCP over TLS using
one certificate. TLS finishes before application classification; no ALPN, custom
preface, or client wire change is required. UDP on the same port remains NBI1/HMAC,
authenticated but unencrypted; this does not add DTLS or QUIC.

Management HTTP (`management_http`, normally `127.0.0.1:9090`) and optional
`business_tcp` retain separate listeners and authorization. Management HTTP is a
control-plane protocol and never participates in device classification. Non-loopback TCP ingress requires TLS. Development mode
requires loopback and permits plaintext for local testing.

`device_ingress` is the only device address. Legacy separate-listener fields are
rejected. Port 443 is only a deployment choice, not an HTTPS endpoint. HTTP bytes
on device ingress close without an HTTP response; see [migration](remove-device-http.md).

NetbaIoT implements MQTT 3.1.1 and MQTT 5.0 directly. It does not require an external broker or
database. The subsystem is layered as incremental packet codec, connection state
machine, authenticated session attachment, bounded session store, topic trie,
retained store, QoS engine, and finally the IoT binding/EventBus.

Supported control packets are CONNECT/CONNACK, PUBLISH, PUBACK/PUBREC/PUBREL/PUBCOMP,
SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP, and DISCONNECT. QoS0, QoS1,
and explicit inbound/outbound QoS2 state machines are implemented. MQTT-SN,
WebSockets, shared subscriptions, bridge mode, and `$SYS` services are outside this
phase. MQTT wildcard rules around `$` topics are still enforced.

## Compatibility and MQTT 5 profile

| Capability | MQTT 3.1.1 | MQTT 5.0 |
| --- | --- | --- |
| QoS0/1/2, retained messages, Will, persistent sessions | Supported | Supported |
| Session expiry, message expiry, Will Delay | No wire property | Supported |
| Receive Maximum and Maximum Packet Size | Broker bounds | CONNECT and CONNACK bounds |
| No Local, Retain As Published, Retain Handling | No wire option | Supported |
| Publish payload format, content type, response topic, correlation data, user properties | No wire property | Bounded transport metadata |
| Topic Alias, Subscription Identifier, shared subscriptions, Enhanced Authentication, WebSocket | Not in profile | Not supported |

MQTT 5 CONNECT binds the connection to protocol level 5. Sessions from protocol
levels 4 and 5 never resume across a version switch: the old session is removed and
Session Present is false. Session Expiry Interval determines when disconnected MQTT 5
state is removed; MQTT 3.1.1 keeps its configured idle policy. Message expiry is
stored as an absolute deadline and forwarded with the remaining seconds. Expired
offline, retained and outbound PUBLISH payload state is released by the broker's
bounded maintenance pass. QoS2 packet state continues until its ACK handshake ends.

MQTT 5 CONNECT advertises the server Receive Maximum, Maximum Packet Size, zero
Topic Alias Maximum, and availability of wildcards and retained messages. Unsupported
shared subscriptions and subscription identifiers are declared unavailable. Property
bytes and counts have explicit `Limits` bounds; rejected features receive MQTT 5
reason codes. MQTT metadata remains inside the MQTT transport and broker, outside
the public `DeviceEvent` model. QoS1 PUBACK retains the existing `EventAccepted`
meaning: the required EventBus responsibility has been accepted, while business
processing can still be pending.

The optional device SDK defaults to MQTT 3.1.1. Select
`MqttProtocolVersion::V5` in its builder for MQTT 5 and configure session/message
expiry if needed. Standard MQTT clients need no SDK.

CONNECT authenticates once through the bounded AuthCache. The resulting
`Arc<AuthenticatedDevice>` is bound to the connection; normal MQTT packets never
perform remote authentication. MQTT ClientId is not trusted identity. Persistent
sessions are keyed by `(Authenticated DeviceKey, ClientId)`, so another device or
tenant cannot inherit or delete a session by copying ClientId. An empty ClientId is
accepted only with CleanSession=1 and receives a connection-local generated value.

CleanSession=1 deletes that authenticated identity's old session and always returns
Session Present=0. CleanSession=0 preserves subscriptions, offline QoS1/2 delivery,
inbound QoS2, outbound QoS1/2, and packet-ID allocation after socket destruction.
The default MQTT 3.1.1 disconnected-session retention policy is 24 hours and is a broker
resource policy. Expiry is evaluated during new attach and periodic maintenance;
all collections remain hard bounded meanwhile.

Subscriptions support exact topic filters, `+`, and final whole-level `#` using a
topic trie. A root wildcard does not match a topic beginning with `$`. Re-subscribe
updates the existing entry. The requested subscription QoS and publish QoS combine
using `min(publish_qos, subscription_qos)`. Authorization permits valid filters only
inside the bound device namespace. Publishing remains restricted to canonical `up`
and `down_ack` topics:

```text
v1/t/{tenant}/p/{product}/d/{device}/up
v1/t/{tenant}/p/{product}/d/{device}/up_ack
v1/t/{tenant}/p/{product}/d/{device}/down
v1/t/{tenant}/p/{product}/d/{device}/down_ack
```

Retained publish, replacement, wildcard replay, and zero-payload deletion are
implemented with count/byte/message/per-tenant bounds. The retained store is bounded
but wildcard retained replay currently scans that bounded store; this deliberate
simplicity is measured and listed as a scaling limitation.

Will Topic, binary payload, QoS, retain flag, size, syntax, and authorization are
validated during CONNECT. EOF, network/protocol error, keepalive timeout, and
connection replacement publish the Will once. DISCONNECT and planned server
shutdown are deliberately different: MQTT DISCONNECT deletes the Will, while a
planned server shutdown publishes the Will before the recovery snapshot. This
follows MQTT-3.1.2-8; orderly process shutdown is not an MQTT DISCONNECT from the
client. An accepted Will also reserves bounded broker responsibility at CONNECT.
If an abnormal disconnect cannot atomically route it because a durable subscriber
is full, the Will remains in a bounded pending queue, survives a planned restart,
and is retried when broker capacity changes; it is never partially routed.

For regular QoS0/QoS1 canonical uplinks, retained mutation and bounded broker routing
run before the IoT binding crosses `EventAccepted`; no later broker-side failure can
turn that accepted event into a producer-visible failure. A pre-accept IoT failure
can leave an MQTT delivery that is replayable under normal at-least-once semantics.
Inbound QoS2 stores a separate `EventAccepted`
pending-route stage, including across planned restart, so retained/subscriber
responsibility can finish without re-emitting the business event. Retained capacity
is reserved before PUBREC for a retained QoS2 flow and released atomically during
routing. `EventAccepted` means every required EventBus sink
reserved count/bytes and was enqueued; it is not a database commit. MQTT QoS2
prevents duplicate IoT binding for one stored MQTT flow, but it does not promise
business exactly-once: EventBus recovery is at-least-once and consumers remain
idempotent.

SUBSCRIBE validates and preflights the complete retained replay against session,
tenant, global, offline, and active-channel bounds before inserting either the
subscription map entry or trie node. A failed SUBACK therefore cannot leave a hidden
subscription that receives future live publications.

Persistent MQTT offline subscription delivery is separate from the command API.
Explicit management commands still require a live device and return
`DEVICE_OFFLINE`; they are never silently converted into stored MQTT commands.

Slow active consumers have a bounded sender. QoS0 may be shed when that bound is
full. For QoS1/2, the broker preflights every matching session, tenant/global
inflight limit, offline queue, session bytes, and an optional retained mutation as
one route plan. If any persistent target cannot own its responsibility, no target or
retained value is changed and the source is not acknowledged. Commit happens only
after the complete plan succeeds, so multi-subscriber routing cannot partially
deliver and silently shed a peer. Planning keeps only compact per-target decisions:
it performs one bounded global accounting pass and does not clone stored payloads
or rescan all sessions once per match.

Persistent sessions store authorization provenance (credential version, auth
generation, permissions, codec identifier, and codec version, never secrets) and a
monotonic session incarnation.
CleanSession=0 takeover keeps the incarnation; CleanSession=1 creates a new one.
Inbound QoS2 completion and routing require the same incarnation, packet identifier,
and operation token. Reconnect under changed authorization resets the old session
and returns Session Present=0. Management invalidation removes matching persistent
state in the same bounded control operation.

Planned restart writes compact NBMQ v6 records incrementally, with a checksummed
header, a length/checksum on every bounded record, and an authenticated whole-image
trailer containing the authoritative record count, byte count, and SHA-256 digest.
v6 records include protocol version, MQTT 5 session expiry, subscription options,
message properties/expiry, outbound transfer start state, retained origin, and
delayed Will deadline/cancellation state. Pending Wills also retain the original
publisher SessionKey for No Local filtering, independently of cancellation state.
Payload bytes remain binary; there is no complete snapshot clone or whole-image
serialization buffer. NBMQ v1 and v2 images remain readable under version-specific
ceilings; NBMQ v3, v4, and v5 are also readable, while all new writes use v6. Legacy sessions that lack complete
authorization/codec provenance are never exposed through the subscription index and
reset safely on attach. The file uses restrictive permissions, file fsync, atomic
rename, and directory fsync.
It contains no password or socket/TLS/task state. Reconnect must authenticate before
the `(DeviceKey, ClientId)` state can resume. Abrupt crash may lose mutations since
the last successful planned snapshot; this is intentionally not a crash-durable
broker.

See [mqtt-3.1.1-conformance.md](mqtt-3.1.1-conformance.md) and
[mqtt-session-recovery.md](mqtt-session-recovery.md) for evidence and recovery
details.
