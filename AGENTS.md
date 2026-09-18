# AGENTS.md

## Project

This project is a high-performance IoT device access platform written in Rust.

Supported transports:

* HTTP
* MQTT
* TCP
* UDP

The architecture must remain extensible to additional transports and device protocols.

Priority order:

> Correctness > Resource Safety > Reliability > Maintainability > Performance

---

## Architecture

Keep transport, device protocol, and business logic strictly separated.

```text
Device
  -> Transport Adapter
  -> Authentication / Session
  -> Codec
  -> Unified DeviceMessage
  -> Message Bus
  -> Business Services
```

### Transport

Transport adapters handle only:

* connections
* framing
* network I/O
* transport lifecycle
* transport metadata

Business logic must not depend on whether a message came from HTTP, MQTT, TCP, or UDP.

### Device Protocol

Vendor/device protocol parsing belongs in codecs.

Do not hardcode product-specific parsing inside gateways.

Codecs must be replaceable and versioned.

```rust
pub trait DeviceCodec: Send + Sync {
    fn decode(
        &self,
        ctx: &DecodeContext,
        payload: &[u8],
    ) -> Result<Vec<DeviceMessage>, CodecError>;

    fn encode(
        &self,
        ctx: &EncodeContext,
        command: &DeviceCommand,
    ) -> Result<EncodedMessage, CodecError>;
}
```

Do not make codec methods async unless they actually perform asynchronous work.

---

## Unified Message Model

All uplink messages must eventually become a common `DeviceMessage`.

Prefer strong domain types over raw strings.

```rust
pub struct TenantId(Arc<str>);
pub struct ProductId(Arc<str>);
pub struct DeviceId(Arc<str>);
pub struct MessageId(Uuid);
pub struct CommandId(Uuid);
```

Avoid using `HashMap<String, Value>` as the primary domain model.

Vendor-specific message types must not leak into core business logic.

---

## Rust

Use stable, idiomatic Rust unless the project explicitly requires otherwise.

Prefer:

* Tokio for async I/O
* `bytes` for network buffers
* `thiserror` for library errors
* `tracing` for structured logging

Production request paths must not rely on:

```rust
unwrap()
expect()
panic!()
todo!()
unimplemented!()
```

Exceptions are acceptable only for clearly unrecoverable process initialization errors.

Avoid `unsafe`.

If `unsafe` is necessary:

* keep the scope minimal
* document invariants with `// SAFETY:`
* add targeted tests
* justify it with measured need

---

## Async and Tasks

Do not use unbounded concurrency.

Every spawned task must have a clear:

* owner
* lifetime
* shutdown path
* concurrency bound
* failure policy

Never write patterns equivalent to:

```rust
loop {
    let item = recv().await;
    tokio::spawn(handle(item));
}
```

without a hard concurrency limit.

Prefer:

* `Semaphore`
* `JoinSet`
* bounded worker pools
* `buffer_unordered(n)`

Do not perform blocking I/O on Tokio worker threads.

Use async APIs or `spawn_blocking` where appropriate.

Avoid holding locks across `.await`.

---

## Bounded Resources

All runtime resources must be bounded.

This includes:

* connections
* tasks
* queues
* buffers
* frames
* packets
* HTTP bodies
* pending writes
* pending commands
* caches
* deduplication state
* retries

Prefer bounded channels:

```rust
tokio::sync::mpsc::channel(capacity)
```

Avoid `unbounded_channel()` unless bounded growth is formally guaranteed.

Every queue must define:

* capacity
* producer
* consumer
* overflow behavior
* shutdown behavior

Never solve overload by allowing memory to grow indefinitely.

---

## Untrusted Input

Treat all device/network input as hostile.

Never allocate directly from an untrusted length.

Bad:

```rust
let len = parse_len(input);
let data = vec![0; len];
```

Required:

```rust
let len = parse_len(input)?;

if len > MAX_FRAME_SIZE {
    return Err(Error::FrameTooLarge);
}
```

Use checked conversions and arithmetic:

```rust
usize::try_from(value)?
checked_add(...)
checked_mul(...)
```

All protocols must enforce hard size limits.

---

## HTTP

HTTP endpoints must define hard limits for:

* body size
* headers
* request timeout
* rate limits

If processing is asynchronous, do not claim downstream processing completed when only ingestion succeeded.

A successful HTTP response may mean only that the message was accepted for processing.

---

## MQTT

Do not implement an MQTT broker unless there is a strong project-specific reason.

Prefer integrating with a mature broker.

MQTT topics and ACLs must prevent one device from accessing another device's data.

MQTT-specific details must not leak into downstream business services.

---

## TCP

TCP is a byte stream.

Never assume:

```text
one read == one message
```

Frame decoders must correctly handle:

* partial headers
* partial payloads
* multiple frames per read
* oversized frames
* invalid frames
* disconnects during frames

Per-connection memory must remain bounded.

Slow consumers must not create unlimited pending writes.

Use bounded outbound queues and write timeouts.

---

## UDP

UDP is connectionless.

Do not model UDP using TCP session semantics.

Account for:

* packet loss
* duplicates
* reordering
* replay
* spoofing
* MTU limits

Prefer small datagrams and avoid IP fragmentation.

Authentication should support replay protection using fields such as:

```text
device_id
timestamp
sequence
nonce
signature
```

---

## Sessions

Real MQTT/TCP connections belong to the local gateway node.

Distributed session storage should contain routing metadata, not socket state.

Example:

```text
device_id -> gateway_node_id
```

Handle:

* reconnects
* duplicate logins
* stale sessions
* node crashes
* delayed disconnect events

Use a session generation/version when needed so an old disconnect cannot invalidate a newer session.

HTTP and UDP must not be given long-lived connection semantics unless explicitly implemented by the protocol design.

---

## Commands

Downlink commands must use a common command model.

Every command must have a unique `command_id`.

Keep transport send state separate from device acknowledgement.

```text
SENT != ACKED
```

A successful transport write does not mean the device executed the command.

Pending commands must have:

* TTL
* per-device limit
* per-tenant limit

Offline commands must never accumulate without bounds.

---

## Delivery Semantics

Assume end-to-end delivery is:

```text
At-Least-Once
```

Do not rely on exactly-once delivery.

Consumers must be idempotent.

Use identifiers such as:

* `message_id`
* `device_id + sequence`
* `command_id`

for deduplication where appropriate.

Deduplication state itself must be bounded by capacity and TTL.

---

## Backpressure

Every stage must define behavior when downstream processing is slower than upstream input.

Never use unlimited in-memory buffering.

Possible policies include:

* wait
* reject
* drop permitted data
* disconnect
* shed load
* persist to bounded storage

Critical messages and telemetry may use different overload policies.

---

## Retries and Timeouts

Every external operation must have a timeout.

This includes:

* database calls
* Redis
* Kafka
* HTTP
* authentication services
* command routing

Retries must be:

* bounded
* limited to retryable errors
* exponential backoff
* jittered

Never implement infinite retry loops.

---

## Storage and Caches

Database queries that grow with dataset size must be bounded or paginated.

Avoid:

* unbounded list APIs
* loading entire tables
* N+1 queries
* unnecessary hot-path database calls

Every cache must define:

* maximum capacity
* TTL
* eviction policy
* metrics

Do not use permanently growing `HashMap`s as caches.

---

## Hot Path

Typical hot-path operations include:

```text
network read
frame decode
authentication/session lookup
codec decode
normalization
queue send
message publish
```

Avoid in the hot path:

* blocking calls
* database round trips
* large allocations
* large clones
* repeated config parsing
* excessive logging

Optimize only after measurement.

---

## Observability

Use structured logging with `tracing`.

Do not use `println!` or `dbg!` in production paths.

Important operations should expose appropriate metrics for:

* connections
* requests
* messages
* bytes
* authentication failures
* decode failures
* queue depth
* queue drops
* command latency
* timeouts
* dependency latency

Do not use high-cardinality identifiers such as `device_id` or `message_id` as Prometheus labels.

Put those in logs/traces instead.

---

## Graceful Shutdown

Long-running services must support graceful shutdown.

Shutdown should:

1. stop accepting new work
2. enter draining state
3. stop creating new tasks
4. finish bounded in-flight work
5. flush required data
6. close connections
7. terminate remaining tasks within a deadline

Do not leak tasks, timers, queues, or sessions during shutdown.

---

## Security

Always consider:

* malformed packets
* oversized payloads
* replay attacks
* forged device identity
* connection floods
* slow clients
* invalid UTF-8
* integer overflow
* memory amplification
* log/credential leakage

Never log:

* passwords
* secrets
* access tokens
* full authorization headers
* raw credentials

Public network transports should support TLS.

---

## Testing

Changes must include appropriate tests.

Protocol parsers should test at least:

* valid input
* truncated input
* invalid headers
* invalid lengths
* oversized input
* boundary sizes
* malformed input

TCP framing must test:

* split frames
* multiple frames per read
* partial frames
* oversized frames

Prefer fuzzing/property testing for binary protocol decoders.

A parser receiving arbitrary bytes must not panic or allocate unbounded memory.

---

## Performance

Do not optimize based on intuition alone.

Use measurement:

* benchmarks
* profiling
* allocation data
* P50/P95/P99 latency
* throughput
* CPU
* RSS
* queue depth

Any significant performance change should provide before/after evidence when practical.

Do not introduce complexity or `unsafe` for theoretical micro-optimizations.

---

## Dependencies

Before adding a crate, check:

* whether existing dependencies already solve the problem
* maintenance status
* transitive dependency cost
* unsafe usage
* hot-path impact
* license compatibility

Do not add dependencies casually.

---

## Code Quality

Before completing a change, run relevant checks:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Run relevant benchmarks or fuzz/property tests when changing hot paths or protocol parsers.

Do not claim tests were run unless they were actually executed.

---

## Agent Rules

Before changing code:

1. Read the relevant implementation.
2. Read related tests.
3. Understand ownership and lifecycle.
4. Identify resource bounds.
5. Understand failure and shutdown paths.
6. Reuse existing abstractions when appropriate.

Do not hide root causes by:

* increasing queue sizes
* increasing timeouts
* swallowing errors
* adding infinite retries
* removing failing tests

Keep changes focused.

Do not perform unrelated refactors unless required to fix the underlying problem.

---

## Non-Negotiable Invariants

1. Transport and device protocol are separate.
2. HTTP, MQTT, TCP, and UDP converge on a unified message model.
3. Device codecs are pluggable and versioned.
4. Gateways contain minimal business logic.
5. Long-lived connection state is node-local.
6. All queues are bounded.
7. All externally controlled sizes have hard limits.
8. All background tasks have defined lifetimes.
9. All external calls have timeouts.
10. All retries are bounded.
11. All caches and pending state are bounded.
12. Slow devices cannot exhaust node resources.
13. A single device cannot exhaust tenant resources.
14. A single tenant cannot exhaust cluster resources.
15. `SENT` never means `ACKED`.
16. At-least-once delivery is assumed.
17. Consumers must tolerate duplicates.
18. No untrusted input may cause unbounded allocation.
19. Resource safety takes priority over micro-optimizations.
20. Performance claims require evidence.
