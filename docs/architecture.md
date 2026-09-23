# Architecture

## Single Device Ingress

`device_ingress` binds one TCP listener and one UDP socket at the same address and
numeric port (development: `127.0.0.1:8080`; production example: `0.0.0.0:443`).
TCP serves HTTPS, standard MQTT 3.1.1 over TLS, and generic framed TCP over TLS using
one certificate. TLS finishes before application classification; no ALPN, custom
preface, or client wire change is required. UDP on the same port remains NBI1/HMAC,
authenticated but unencrypted; this does not add DTLS or QUIC.

Management HTTP (`management_http`, normally `127.0.0.1:9090`) and optional
`business_tcp` retain separate listeners and authorization. Device HTTP cannot
serve management APIs. Non-loopback TCP ingress requires TLS. Development mode
requires loopback and permits plaintext for local testing.

Configuration replaces `device_http`, `mqtt`, `tcp`, and `udp` with
`device_ingress`; legacy fields are rejected as configuration errors. Choose the
new address explicitly and update every device destination/firewall rule. There is
no silent conversion of differing old ports.

NetbaIoT is a database-free, event-driven IoT gateway. Its runtime path is:

```text
HTTP / MQTT / TCP / UDP devices
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
HTTP/MQTT/TCP accounting; cancellation, EOF, TLS error and detection failure release
it exactly once. Existing HTTP request slots, MQTT limits and per-device/tenant
admission remain unchanged. There are no new per-protocol connection reservations:
the original global/IP pool remains shared, so this is not starvation-proof QoS.

Detection uses a fixed 12-byte buffer. HTTP requires a recognized method plus SP;
MQTT reuses the bounded fixed-header/Remaining-Length decoder and requires the
`00 04 MQTT` name plus a protocol level byte. Level 4 is supported; other levels go
to the existing parser only to return standard CONNACK=1 and close. Generic TCP
requires a valid 1..max_tcp_frame_size length and JSON object/whitespace start.
Validated frames never exceed 1 MiB, so their first length byte is zero, disjoint
from HTTP methods and MQTT's 0x10. No failed parser falls back to another protocol.
The prefix is replayed unchanged before underlying reads, including through TLS.

TLS, detection and first packet/HTTP headers share one `connect_timeout_ms`
deadline starting at admission; detection cannot refresh it. The existing bounded
authentication and HTTP request/write phases remain separate. Detection failures
and timeouts use closed metric names and debug logging. Quiesce closes admission,
stops shared TCP/UDP and waits for connection owners, then commits MQTT recovery
and drains/spools required work; management stops only after durable completion.

Normal MQTT/TCP telemetry uses only socket parser state, its bound trusted auth
context, shared codec/config snapshots, and bounded memory routing. It performs no
database, filesystem, remote auth, or control-plane operation. HTTP may call the
auth provider only on a cache miss. UDP verifies each signed datagram and keeps a
bounded local replay window.

Inside the MQTT transport, packet parsing, protocol state, session storage,
subscription routing, retained state, and QoS are isolated from the IoT binding.
Persistent MQTT sessions are keyed by authenticated DeviceKey plus ClientId and hold
only bounded protocol state; they never hold a dead socket or arbitrary DeviceEvent
history. MQTT QoS and EventBus delivery semantics are separate contracts.

Commands travel in the opposite direction from management HTTP to `CommandRouter`,
then directly to a live local MQTT/TCP session's count-and-byte-bounded queue. An
offline device returns unavailable; no offline command is retained.

Device and management HTTP have separate listeners and authorization. Runtime
configuration is a revisioned immutable control snapshot. Auth and configuration
caches are separately bounded and reconstructed after restart.

Planned shutdown is `RUNNING -> QUIESCING -> DRAINING -> SPOOLING -> DRAINED`.
The admission gate closes before listeners. Accepted required deliveries either ACK
or are committed to the bounded local restart spool with file fsync, atomic rename,
and directory fsync. The same recovery directory holds a separate atomic MQTT
protocol snapshot for retained and persistent-session state. Abrupt crashes can lose
the bounded non-spooled memory window and recent MQTT mutations.

Workspace responsibilities:

- `netbaiot-protocol`: public wire/domain types, stable errors, paths, and versioning.
- `netbaiot-client`: business HTTP APIs and confirmed event-stream ownership.
- `netbaiot-device-sdk`: optional standard MQTT/device-HTTP convenience client.
- `netbaiot-core`: strong domain/event/command types and synchronous codec trait.
- `netbaiot-codecs`: bounded vendor/device protocol codecs.
- `netbaiot-runtime`: caches, resource admission, event bus, sessions, lifecycle,
  commands, metrics, and restart spool.
- `netbaiot-transports`: HTTP, embedded MQTT, TCP framing, UDP, ACLs, and connection
  owners.
- `netbaiot-server`: validated composition, webhook/TCP business sinks, listeners,
  recovery, and graceful shutdown.
- `netbaiot-cli`: operator interface implemented through `netbaiot-client` only.

The public client crates point inward only to `netbaiot-protocol` and network
dependencies; they never depend on runtime, transport, broker, session, or server
implementation crates.

There is deliberately no storage crate, SQL migration, database pool, durable
outbox, persistent command state, or runtime message history.
