# Rust business client

`netbaiot-client` is the official async Rust client for business and management
integrations. It uses a pooled HTTP client for control operations and the current Business RPC V3 stream for events. It creates no Tokio runtime and no database.

```rust
let client = NetbaIoTClient::builder()
    .endpoint("https://gateway.example")
    .token(management_token)
    .event_address("127.0.0.1:9100".parse()?)
    .event_token(stream_token)
    .connect()
    .await?;
```

The scoped APIs are `events()`, `commands()`, `devices()`, `runtime()`,
`auth_cache()`, and `routes()`. Secrets have redacted `Debug`. Connect, request,
stream handshake, and ACK-write timeouts are finite and builder-validated.

Event delivery defaults to `AckMode::Manual`:

```rust
let mut events = client.events().subscribe(EventFilter::default()).await?;
while let Some(delivery) = events.next().await {
    let delivery = delivery?;
    process(delivery.event()).await?;
    delivery.ack().await?;
}
```

There is one server-confirmed delivery outstanding per subscription. The user-facing
channel is bounded by 32 items and 1 MiB by default, and the RPC command/ACK path is bounded. One server-confirmed event remains outstanding. Count capacity is not preallocated with maximum payload bytes. Dropping an unacknowledged delivery never sends ACK; the gateway timeout/retry policy may redeliver.

Connection loss uses cancellation-aware bounded exponential backoff from 100 ms
to 5 s. A reconnect authenticates and recreates a subscription with the same filter
and a new epoch/subscription identity. No client offset is invented. Replay can return the same `event_id` under
a new `delivery_id`; applications requiring durable idempotency must persist event
IDs themselves. Dropping the stream aborts its owned task and closes the socket.

Commands are never retried automatically. `DeviceOffline` is distinct from generic
server failure, and caller-supplied `command_id` remains unchanged. Applications
persist desired/reported configuration and history externally. Ordinary commands
can carry application-defined configuration operations; `CommandAck` reports device
execution, while application code decides convergence, retries and rollback. `runtime().drain()` is explicitly administrative.

## Current Business RPC client

`netbaiot-client::business_rpc` exposes `BusinessRpcV3Client`, current `BusinessAuthHandler`, reset sync, invalidation, event ACK and online commands. See [Business RPC V3](business-rpc-v3.md) and the compilable [current example](../crates/netbaiot-client/examples/business_rpc_current.rs). Low-level `wait_ready()` is persistent; wrap it in a timeout or cancellation. The root event façade uses this same current driver. Configure a separate development `event_token`, or `event_tls(BusinessRpcTls)` for mTLS; no management-token fallback exists. Old wire/client APIs have been removed; see [upgrade requirements](migration/current-protocol-only.md).
