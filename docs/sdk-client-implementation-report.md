# SDK and client implementation report

## Baseline and scope

The implementation started from commit
`4446d64c098f3982fe83ab31d995653dae20aac1`. It adds the official Rust public
protocol, business client, operator CLI, and optional device SDK without adding a
database, external broker, hidden runtime, or unbounded offline queue. The existing
database-free event-bus runtime and embedded MQTT broker remain authoritative.

The final public dependency direction is:

```text
                         netbaiot-protocol
                         /       |       \
                        /        |        \
                       v         v         v
              netbaiot-client  server  netbaiot-device-sdk
                      |
                      v
                 netbaiot-cli
```

`netbaiot-protocol` is runtime-independent. `netbaiot-client` and
`netbaiot-device-sdk` do not depend on server or runtime crates. `netbaiot-cli`
delegates its business and management operations to `netbaiot-client`.

## Public protocol and wire contracts

The public contract is explicitly versioned as protocol v1. HTTP paths remain below
`/api/v1/...` for management and `/v1/device/...` for devices. The business stream
uses bounded JSON frames prefixed by a four-byte network-order length and versioned
`hello`, `subscribe`, `ready`, `event`, and `ack` messages. The client sends the
authenticated `hello` followed by `subscribe`; the server responds with `ready`.

The protocol crate owns the stable public shapes, including:

- strong tenant, product, device, event, delivery, command, source-message, codec,
  sink, subscription, and configuration-revision identifiers;
- `DeviceKey`, `DeviceEvent`, `DeviceEventKind`, `DeviceUplink`, and event types;
- `EventFilter`, `EventDelivery`, `EventAck`, and client/server stream frames;
- command payload, dispatch, delivery, and execution states;
- device configuration, application status, and configuration ACK;
- connection information, transport kind, runtime status, and connection counts;
- routes/control snapshots, authentication invalidation, `EventAccepted`, and the
  stable `ErrorCode`/`ApiError` wire error.

Serialization tests cover representative event and stream/error contracts. The
crate depends only on Serde, `serde_json`, `thiserror`, and UUID support; it owns no
Tokio runtime, transport, session, broker, or server state.

## Business client

`netbaiot-client` owns a pooled rustls HTTP client and optional confirmed event
subscriptions on the caller's Tokio runtime. Its builder validates endpoints,
timeouts, buffer limits, and reconnect bounds; token wrappers redact secrets from
`Debug`. Management and event-stream tokens may be configured independently.

The scoped public APIs are:

- `events()`: manual-ACK event subscriptions by default, with explicit immediate
  ACK mode as an opt-in;
- `commands()`: sends caller-owned stable command IDs without automatic replay;
- `devices()`: queries a device's connection status and transport;
- `configs()`: gets and sets revisioned device configuration;
- `runtime()`: status and explicit administrative drain;
- `auth_cache()`: typed device, product, tenant, or credential invalidation;
- `routes()`: applies the server's existing typed route snapshot format.

HTTP errors decode to `ClientError` variants for authentication, authorization,
invalid input, version mismatch, offline device, overload, draining, timeout,
connection loss, not found, conflict, server availability, protocol failure, and
transport failure. Request IDs are retained where the server provides them.

### Event delivery, ACK, and recovery

The SDK exposes the stable `event_id` separately from attempt-specific
`delivery_id`. The default sequence is receive, application process, then explicit
`Delivery::ack`. An unacknowledged delivery is never acknowledged on drop. Instead,
the connection is re-established so the server can replay it. One confirmed event
is outstanding per subscription, so the ACK path is fixed at one rather than an
unbounded queue.

Each subscription owns one receive/reconnect task. It has a bounded application
channel (32 items by default) and a separate byte semaphore (1 MiB by default).
These values are builder-configurable and maximum bytes are not preallocated. No
per-event task is created. Stopping reads applies backpressure, and dropping either
the stream or final client cancels its owned task and socket work.

Connection loss uses cancellation-aware exponential full-jitter backoff, bounded by
100 ms initially and 5 s maximum by default. Reconnect uses the same
`SubscriptionId`, authenticates again, and resubscribes. No durable client offset or
volatile deduplication layer is invented. An unacknowledged event can therefore
return with the same stable `event_id` and a new `delivery_id`; durable consumers
must make their own processing idempotent by `event_id`.

## Focused server API adjustments

The following server changes were made because the official client exposed missing
or ambiguous public contracts:

- management HTTP now returns the shared stable `ApiError` JSON shape with public
  error codes and request IDs;
- offline command dispatch maps to `DeviceOffline` rather than a generic internal
  failure;
- `POST /api/v1/devices/connection` exposes bounded typed connection metadata;
- `POST`/`PUT /api/v1/devices/config` expose typed device configuration get/set;
- status uses shared typed runtime/connection-count models;
- auth invalidation, route/control, event acceptance, command, and configuration
  shapes are shared from the protocol crate rather than duplicated JSON models;
- the confirmed stream implements the public versioned handshake, filter
  validation, stable event identity, attempt-specific delivery identity, and exact
  ACK matching;
- handshake/ACK reads have hard deadlines; malformed clients are isolated and
  authentication, version, and validation failures return structured stream errors
  without terminating the listener;
- confirmed-stream waits are shutdown-aware so an unacknowledged event cannot block
  graceful restart indefinitely;
- MQTT management commands use direct QoS 1 delivery to a live authenticated
  device session even when that device did not subscribe to a broker topic;
- an embedded/test server entry point accepts explicit credentials without mutating
  process-global environment variables.

No list API, persistence layer, alternate route format, or private session/socket
model was added to the public client.

## CLI

The `netbaiot` executable uses only `netbaiot-client` for its business/management
wire operations. It implements server status and confirmed drain; device status;
event subscription; command send from JSON or a file; config get/set; and device
auth-cache invalidation. Flags and `NETBAIOT_*` environment variables configure its
endpoint and credentials. Human and JSON/JSONL output are supported; event output is
flushed before the manual ACK. Exit codes distinguish usage, authentication,
authorization, offline device, and unavailable/runtime failures.

## Device SDK

`netbaiot-device-sdk` is optional and maps directly to the existing standard MQTT
3.1.1 topic and HTTP contracts. It accepts MQTT, HTTP, or both, owns no runtime or
database, and keeps credentials in redacted in-memory configuration.

MQTT uses `rumqttc` 0.25.1 with rustls. The dependency was selected after reviewing
its current documentation, repository, MQTT 3.1.1 support, bounded request-channel
model, TLS support, Apache-2.0 license, and dependency cost. The SDK runs one event
loop task, uses bounded MQTT and command channels, publishes telemetry/events at
explicit QoS, receives commands, and sends command execution results. Commands are
broker-ACKed only after admission to the bounded application queue. Malformed or
overflowing commands cause disconnect without broker ACK so a persistent session
can redeliver them.

An MQTT-configured `connect().await` waits for initial broker readiness rather than
returning a client that immediately reports `Offline`. Later connection loss uses
bounded exponential full-jitter retry and every successful connection explicitly
resubscribes the command topic. Authentication, authorization, client-ID, and
protocol-version CONNACK failures are terminal instead of entering a retry storm.

The only current offline publish policy is the explicit default `Reject`; there is
no hidden RAM backlog. MQTT publish success means bounded client admission, not
business persistence.

HTTP supports bounded-response data/heartbeat upload, conditional config retrieval
with ETag/304, and separate application config ACK. Upload success returns
`EventAccepted`; config download never implies that the application applied it. The
caller owns revision persistence across device restarts.

Standard MQTT 3.1.1 clients remain supported and covered separately by broker
interoperability tests; the SDK introduces no proprietary tunnel or authentication
scheme.

## Verification

Real-process integration tests use the official clients, not mocked handlers. They
cover:

- device HTTP/MQTT publication through the server to the business event stream and
  explicit ACK, retaining the same `event_id`;
- online command delivery to the device and a stable `command_id` through its
  execution ACK event;
- explicit `DeviceOffline` behavior with no offline server storage;
- config revision set/get, device conditional pull, apply ACK, and business
  `ConfigAck` event;
- device connection query, runtime status, routes/auth contracts, and shutdown;
- graceful restart with an outstanding unacknowledged event, automatic reconnect
  and resubscription, replay with the same `event_id`, a new `delivery_id`, and zero
  missing accepted IDs; the same test also verifies device-SDK MQTT reconnect,
  telemetry publication, and command reception after the new server starts;
- a real `netbaiot` CLI smoke run covering status, device status, event subscribe,
  command, config get/set, auth invalidation, and drain;
- existing standard MQTT interoperability and the realistic authentication-cache
  test: one CONNECT authentication plus 10,000 publishes results in one provider
  call, not 10,001.

Unit and integration coverage also verifies builder rejection of invalid bounds,
secret redaction, bounded reconnect delay, protocol serialization, server event-bus
backpressure/accounting, transport limits, and task shutdown paths. All workspace
tests pass with all features. The MQTT/business-stream/JSON parser fuzz targets each
completed 1,000 runs under nightly libFuzzer.

## Performance and memory measurement

The measured official-client loopback test processed 200 sequential confirmed
events in a debug test build:

| Measurement | Result |
| --- | ---: |
| Throughput | 2,696.79 events/s |
| HTTP `EventAccepted` return to stream delivery P50/P95/P99 | 59 / 88 / 100 µs |
| `Delivery::ack` call P50/P95/P99 | 28 / 44 / 58 µs |
| Test process RSS before clients | 12,464 KiB |
| Test process RSS with business client | 13,600 KiB |
| Approximate incremental business-client RSS | 1,136 KiB |
| Test process RSS with business and device clients | 13,840 KiB |
| Approximate incremental device-SDK RSS | 240 KiB |
| Server process RSS | 9,168 KiB |

These are reproducible engineering measurements, not production capacity claims.
They use a small sequential loopback sample, debug binaries, and process RSS. The
client/device figures are incremental within the test harness rather than isolated
standalone-process measurements. CPU attribution, command/config latency, and CLI
startup time were not separately benchmarked in this revision.

## Known limitations

- The server confirmed stream currently permits one active subscriber and one
  serial outstanding delivery; logical subscription multiplexing is not available.
- The business stream is a loopback-only plaintext listener today. HTTP and device
  transports use their existing TLS support, but stream TLS is not yet exposed.
- Management authentication remains the server's static all-or-nothing token.
  Scope names and forbidden errors are modeled for compatibility, but granular
  scope enforcement is not yet implemented.
- There are no durable client offsets or protocol-level NACKs. Disconnect before
  ACK intentionally relies on server replay.
- The device SDK currently implements only `OfflinePublishPolicy::Reject`; a bounded
  offline buffer mode is not provided.
- Product-level direct config get/set is not exposed because the current server API
  supports device configuration, while product runtime configuration remains part
  of the control snapshot.
- The business client does not automatically retry HTTP reads. This avoids hidden
  retry behavior; callers can retry idempotent operations explicitly.
- The performance sample is debug/loopback and too small for a production capacity
  claim. Memory numbers are incremental harness RSS, not isolated binaries.
