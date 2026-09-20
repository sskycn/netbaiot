# Embedded MQTT 3.1.1 broker

NetbaIoT implements MQTT 3.1.1 directly. It does not require an external broker or
database. The subsystem is layered as incremental packet codec, connection state
machine, authenticated session attachment, bounded session store, topic trie,
retained store, QoS engine, and finally the IoT binding/EventBus.

Supported control packets are CONNECT/CONNACK, PUBLISH, PUBACK/PUBREC/PUBREL/PUBCOMP,
SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK, PINGREQ/PINGRESP, and DISCONNECT. QoS0, QoS1,
and explicit inbound/outbound QoS2 state machines are implemented. MQTT 5, MQTT-SN,
WebSockets, shared subscriptions, bridge mode, and `$SYS` services are outside this
phase. MQTT wildcard rules around `$` topics are still enforced.

CONNECT authenticates once through the bounded AuthCache. The resulting
`Arc<AuthenticatedDevice>` is bound to the connection; normal MQTT packets never
perform remote authentication. MQTT ClientId is not trusted identity. Persistent
sessions are keyed by `(Authenticated DeviceKey, ClientId)`, so another device or
tenant cannot inherit or delete a session by copying ClientId. An empty ClientId is
accepted only with CleanSession=1 and receives a connection-local generated value.

CleanSession=1 deletes that authenticated identity's old session and always returns
Session Present=0. CleanSession=0 preserves subscriptions, offline QoS1/2 delivery,
inbound QoS2, outbound QoS1/2, and packet-ID allocation after socket destruction.
The default disconnected-session retention policy is 24 hours and is a broker
resource policy, not MQTT 5 Session Expiry. Expiry is evaluated during new attach;
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
shutdown suppress it. Intentional restart closes sockets without manufacturing a
client failure.

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

Slow active consumers have a bounded sender. QoS0 is shed when that bound is full;
QoS1/2 moves to the bounded persistent offline queue where possible. Exhausting the
offline/session/tenant/global bound sheds that subscriber delivery without
unbounded waiting tasks. A single subscriber cannot make already-enqueued peers be
replayed.

Planned restart writes one internally consistent, versioned MQTT snapshot with
SHA-256, restrictive permissions, file fsync, atomic rename, and directory fsync.
It contains no password or socket/TLS/task state. Reconnect must authenticate before
the `(DeviceKey, ClientId)` state can resume. Abrupt crash may lose mutations since
the last successful planned snapshot; this is intentionally not a crash-durable
broker.

See [mqtt-3.1.1-conformance.md](mqtt-3.1.1-conformance.md) and
[mqtt-session-recovery.md](mqtt-session-recovery.md) for evidence and recovery
details.
