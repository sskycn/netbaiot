# Architecture

```
HTTP / embedded MQTT / framed TCP / authenticated UDP
                  -> DeviceAuthenticator
                  -> synchronous versioned DeviceCodec
                  -> DeviceMessage
                  -> shared Ingress admission
                  -> Store transaction: message + outbox job
                  -> leased delivery worker -> business HTTP endpoint
```

## Workspace and ownership

- `netbaiot-core`: validated identifiers, scalar telemetry/event/heartbeat/ACK
  payloads, messages, commands, receipts, presence, and synchronous codec trait.
  No Tokio, transport, HTTP, MQTT, database, or storage dependencies.
- `netbaiot-codecs`: strict JSON v1 parsing, bounds checked before deserialization,
  unique telemetry fields, bounded typed scalar data and command encoding.
- `netbaiot-runtime`: object-safe authentication/storage ports, immutable codec
  registry, ingress quotas, command router, sessions, metrics, and workers.
- `netbaiot-storage`: PostgreSQL transactions/migrations and a bounded volatile
  adapter for tests/development. Storage depends on runtime ports, never transports.
- `netbaiot-transports`: shared stream supervision/TLS and independent HTTP,
  MQTT, TCP and UDP adapters. MQTT routing types never cross into core/runtime.
- `netbaiot-server`: configuration, credential provisioning, PostgreSQL/TLS,
  delivery sink, task supervision, and shutdown.

One server JoinSet owns four listeners and two workers. Each stream listener owns
its bounded connection JoinSet. The connection semaphore and memory reservation
are shared across HTTP, MQTT and TCP. One task owns each stream and its read/write
state; no packet spawns a task. `Reader` retains partial bytes across cancelled
select branches. Cancellation drops only the read future, not its buffer.

MQTT/TCP local sessions own bounded command channels. `SessionLease` cleanup only
removes the matching generation. Reconnect cancels the old owner, and delayed old
cleanup cannot delete new sessions or subscriptions. Both active and superseded
connections still hold their connection quotas until their actual task exits.
Only one current stream route exists per device, across MQTT and TCP.

HTTP/UDP update last-seen presence; they create no connected session. HTTP pull
leases are stored command state, not connection state. Presence is bounded by
provisioned-device capacity and expires for disconnected devices after retention.

## Extension points

`DeviceCodec` is synchronous; registry keys are `(CodecId, version)`. Authentication
selects the codec, so payloads cannot select credentials or arbitrary code. The
initial codec is `netbaiot-json` version 1, named `netbaiot-json-v1` in wire docs.
`TcpFramer` is independent of `DeviceCodec`; another framer can be composed later.
The MQTT parser/version boundary is isolated from sessions, exact-topic indexes,
commands and ingress. `MqttProtocolVersion::V5` reserves a version identity; it is
not evidence of MQTT 5 implementation.

## Failure and shutdown

Malformed input, failed auth, ACL violations and overload reject the request or
close the owning stream. UDP silently drops failures. Durable pending work stays
in PostgreSQL and resumes through lease expiry. Connection failures do not kill
other connections. Authentication on MQTT/TCP observes EOF/stop while preserving bounded pipelined input.
Command callbacks carry storage attempt/lease ownership independently of session generation.
A worker/listener failure causes coordinated server shutdown;
a supervisor must restart the process. There are no hidden retry-forever loops.

Shutdown sets ingress draining before cancelling admission/listeners and workers.
No new stream task or ingress operation is accepted. Existing ingress/store calls
can complete within their timeouts and send permitted receipts. Owners then drop
channels, QoS state, subscriptions and session leases. HTTP uses Hyper graceful
shutdown. Each listener drains its JoinSet; the server has an overall hard deadline
and aborts/joins remaining tasks. Dropping a listener JoinSet requests abort of its children. At the top-level hard
deadline, child destructors may complete on a subsequent runtime scheduling step;
`run()` is not an instantaneous all-grandchild-destructors completion barrier.
Uncompleted PostgreSQL transactions roll back; uncompleted delivery leases expire.
The process never persists sockets or task handles.

This is a single-node routing foundation, not MQTT clustering or distributed
presence. PostgreSQL admission locks enforce storage capacities across processes
sharing that database, but live socket ownership remains local.
