# Architecture

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

- `netbaiot-core`: strong domain/event/command types and synchronous codec trait.
- `netbaiot-codecs`: bounded vendor/device protocol codecs.
- `netbaiot-runtime`: caches, resource admission, event bus, sessions, lifecycle,
  commands, metrics, and restart spool.
- `netbaiot-transports`: HTTP, embedded MQTT, TCP framing, UDP, ACLs, and connection
  owners.
- `netbaiot-server`: validated composition, webhook/TCP business sinks, listeners,
  recovery, and graceful shutdown.

There is deliberately no storage crate, SQL migration, database pool, durable
outbox, persistent command state, or runtime message history.
