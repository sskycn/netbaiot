# Public protocol v1

`netbaiot-protocol` is the authoritative, runtime-independent Rust model for the
public NetbaIoT wire contract. It depends only on serialization, JSON, UUID, and
error-model crates. It does not depend on Tokio, HTTP, MQTT, the server, or runtime
internals. Crate SemVer and wire `PROTOCOL_VERSION` are separate compatibility
dimensions; the current wire version is `1`.

Strong public identifiers include `TenantId`, `ProductId`, `DeviceId`, `DeviceKey`,
`EventId`, `DeliveryId`, `CommandId`, `SinkId`, and
`SubscriptionId`. IDs are validated before use. UTC timestamps are Unix
milliseconds.

`DeviceEvent` contains a stable `event_id`, source message ID, authoritative device
identity, receive/occurrence times, and one typed event kind: telemetry, heartbeat,
device event, or command ACK. Restart replay retains
the event ID. `DeliveryId` instead identifies one stream delivery attempt and may
change after reconnect.

Connection presence belongs to Sessions and management connection queries, not the
business event stream. The 0.x connection-event cleanup removes the unused lifecycle
variants and filter strings from the public Rust/JSON model. Rebuild clients and
remove obsolete filters; supported device uplinks and confirmed stream framing keep
wire version 1. See the [compatibility review](connection-events-spool-upgrade-cleanup.md)
and [legacy spool upgrade procedure](restart-spool.md#legacy-configack-restart-spool-compatibility).

The confirmed stream uses four-byte big-endian length framing around bounded JSON.
The client sends `hello` with version/authentication, then `subscribe` with a bounded
`EventFilter`. The server responds `ready`, then sends `event` frames containing an
`EventDelivery`. The client confirms processing with `ack` containing all of
`delivery_id`, `subscription_id`, and `event_id`. A write or decode is not an ACK.

Management errors use `ApiError { code, message, request_id, required_scope }`.
Stable codes include authentication, authorization, invalid request/version,
device offline, overload, draining, timeout, connection loss, not found, conflict,
server unavailable, and internal error. Clients never need to parse error strings.
Server responses and event models intentionally tolerate harmless additional JSON
fields within wire v1; request parsing may remain strict where rejecting ambiguity
protects the server. The official management client permits plaintext HTTP only for
`localhost` or loopback IP endpoints and requires HTTPS off-loopback.

Control-plane snapshots and mutations share one serialization lock. Each accepted
mutation therefore observes the latest committed state and advances revision
semantics deterministically instead of racing an invalidate or replacement.

Device JSON v1 is represented by `DeviceUplink`. Its stable fields are
`schema_version`, `source_message_id`, optional `occurred_at`, `kind`, and `data`.
MQTT QoS1 PUBACK, TCP acceptance and signed UDP NBA1 mean only `EventAccepted`.

The 0.x device-HTTP removal is an intentional source/control-API breaking change:
`TransportKind::Http` and `ConnectionCounts.http` are removed. Transport strings
are now `mqtt`, `tcp`, `udp`; the status summary serializes exactly those three
counts and excludes management connections. UDP remains sessionless (active count
zero). Device JSON v1, MQTT 3.1.1, TCP framing, NBI1/NBA1 and confirmed business
stream v1 are unchanged; their wire versions do not change. Rebuild public clients
together with the server. See [migration](remove-device-http.md).
