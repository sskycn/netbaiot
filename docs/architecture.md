# Architecture

## Single Device Ingress

`device_ingress` binds one TCP listener and one UDP socket at the same address and
numeric port (development: `127.0.0.1:8080`; production example: `0.0.0.0:443`).
TCP serves standard MQTT 3.1.1 over TLS, and generic framed TCP over TLS using
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

NetbaIoT is a database-free, event-driven IoT gateway. Its runtime path is:

```text
MQTT / TCP / UDP devices
          |
          v
transport framing and lifecycle
          |
          v
authentication / ACL / bound AuthContext
          |
          v
versioned DeviceCodec
          |
          v
DeviceEvent(event_id)
          |
          v
bounded EventBus and atomic required-sink admission
          |
          +------ confirmed HTTP webhook
          |
          +------ confirmed framed TCP/RPC stream
```

The shared TCP listener reserves the existing global count/byte and per-IP
connection lease before spawning its owned connection task, and applies the same
bounded source-IP rate limiter. Classification converts that lease in place to
MQTT/TCP accounting; cancellation, EOF, TLS error and detection failure release
it exactly once. MQTT limits and per-device/tenant admission remain unchanged. Management HTTP
retains bounded request slots and global/IP/byte leases but is excluded from device
`active_connections` counts. There are no new per-protocol connection reservations:
the original global/IP pool remains shared, so this is not starvation-proof QoS.

Detection uses a fixed 12-byte buffer: one packet byte, up to four Remaining Length
bytes, two protocol-name length bytes, four name bytes and one level byte. MQTT reuses the bounded fixed-header/Remaining-Length decoder and requires the
`00 04 MQTT` name plus a protocol level byte. Level 4 is supported; other levels go
to the existing parser only to return standard CONNACK=1 and close. Generic TCP
requires a valid 1..max_tcp_frame_size length and JSON object/whitespace start.
Validated frames never exceed 1 MiB, so their first length byte is zero, disjoint
from MQTT's 0x10. No failed parser falls back to another protocol.
The prefix is replayed unchanged before underlying reads, including through TLS.

TLS, detection and first packet share one `connect_timeout_ms`
deadline starting at admission; detection cannot refresh it. The existing bounded
authentication phase remains separate. Management HTTP keeps its own bounded
header/request/write phases. Detection failures
and timeouts use closed metric names and debug logging. Quiesce closes admission,
stops shared TCP/UDP and waits for connection owners, then commits MQTT recovery
and drains/spools required work; management stops only after durable completion.

Normal MQTT/TCP telemetry uses only socket parser state, its bound trusted auth
context, shared codecs and routing snapshots, and bounded memory routing. It performs no
database, filesystem, remote auth, or control-plane operation. UDP verifies each signed datagram and keeps a
bounded local replay window.

Inside the MQTT transport, packet parsing, protocol state, session storage,
subscription routing, retained state, and QoS are isolated from the IoT binding.
Persistent MQTT sessions are keyed by authenticated DeviceKey plus ClientId and hold
only bounded protocol state; they never hold a dead socket or arbitrary DeviceEvent
history. MQTT QoS and EventBus delivery semantics are separate contracts.

Commands travel in the opposite direction from management HTTP to `CommandRouter`,
then directly to a live local MQTT/TCP session's count-and-byte-bounded queue. An
offline device returns unavailable; no offline command is retained.

Management HTTP has an independent listener and admin authorization. Runtime
configuration is a revisioned immutable control snapshot. The auth cache and gateway control state
are separately bounded and reconstructed after restart. Neither owns device desired state.

Planned shutdown is `RUNNING -> QUIESCING -> DRAINING -> SPOOLING -> DRAINED`.
The admission gate closes before listeners. Accepted required deliveries either ACK
or are committed to the bounded local restart spool with file fsync, atomic rename,
and directory fsync. The same recovery directory holds a separate atomic MQTT
protocol snapshot for retained and persistent-session state. Abrupt crashes can lose
the bounded non-spooled memory window and recent MQTT mutations.

Workspace responsibilities:

- `netbaiot-protocol`: public wire/domain types, stable errors, paths, and versioning.
- `netbaiot-client`: business HTTP APIs and confirmed event-stream ownership.
- `netbaiot-device-sdk`: optional standard MQTT convenience client.
- `netbaiot-core`: strong domain/event/command types and synchronous codec trait.
- `netbaiot-codecs`: bounded vendor/device protocol codecs.
- `netbaiot-runtime`: caches, resource admission, event bus, sessions, lifecycle,
  commands, metrics, and restart spool.
- `netbaiot-transports`: management HTTP, embedded MQTT, TCP framing, UDP, ACLs, and connection
  owners.
- `netbaiot-server`: validated composition, webhook/TCP business sinks, listeners,
  recovery, and graceful shutdown.
- `netbaiot-cli`: operator interface implemented through `netbaiot-client` only.

The public client crates point inward only to `netbaiot-protocol` and network
dependencies; they never depend on runtime, transport, broker, session, or server
implementation crates.

There is deliberately no storage crate, SQL migration, database pool, durable
outbox, persistent command state, or runtime message history.

## UDP acceptance receipt

```text
Device -- NBI1 --> HMAC + version + clock + bounded replay
  New:               ingest -> EventAccepted -> replay commit -> sign NBA1
  AcceptedDuplicate: skip codec/presence/EventBus -------------> sign NBA1
Device <-- NBA1 -- nonblocking send (failure never rolls back acceptance)
```

A single receive-loop owner keeps replay and receipt work bounded. No ACK queue,
per-packet task, UDP session, command endpoint, or ACK spool is created. Quiesce
waits for active datagram admission guards; only required business work drains or
spools. Auth invalidation fences receipt signing without a second provider lookup.
See [wire format, retry and security boundaries](device-protocol.md#udp-acknowledgement-nba1).

```text
Device -- MQTT/TCP/UDP --> NetbaIoT -- DeviceEvent --> Business System
Business System -- DeviceCommand --> NetbaIoT -- MQTT/TCP --> Device
Device -- CommandAck --> NetbaIoT -- DeviceEvent --> Business System
```

NetbaIoT does not own or persist device desired configuration. Applications own
persistent desired/reported state, revisions/history, retries, rollout, rollback,
and offline reconciliation. Configuration changes can travel to online MQTT/TCP
devices as ordinary `DeviceCommand` values. Devices return `CommandAck`; the
application decides whether its desired state has converged. Commands remain
online-only; UDP remains sessionless with no downlink. See the
[ownership migration](remove-device-config.md).

Connection presence is control-plane/runtime state, not a `DeviceEvent`.
`Sessions`, presence timestamps, session generations, and management connection
queries retain current MQTT/TCP online state and UDP `last_seen` observations.
The business stream contains telemetry, device events, device-originated heartbeat,
and command acknowledgements only. NetbaIoT does not emit durable online/offline
events. Applications needing presence history own it through application heartbeat,
management polling, external monitoring, or business-specific presence logic.
