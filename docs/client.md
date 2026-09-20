# Rust business client

`netbaiot-client` is the official async Rust client for business and management
integrations. It uses a pooled HTTP client for control operations and the confirmed
framed TCP stream for events. It creates no Tokio runtime and no database.

```rust
let client = NetbaIoTClient::builder()
    .endpoint("https://gateway.example")
    .token(management_token)
    .event_address("127.0.0.1:9100".parse()?)
    .event_token(stream_token)
    .connect()
    .await?;
```

The scoped APIs are `events()`, `commands()`, `devices()`, `configs()`, `runtime()`,
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
channel is bounded by 32 items and 1 MiB by default, and the outstanding ACK path is
fixed at one. Count capacity is not preallocated with maximum payload bytes. Dropping
an unacknowledged delivery closes/reconnects the stream so the server may redeliver.

Connection loss uses cancellation-aware exponential full-jitter backoff from 100 ms
to 5 s. A reconnect authenticates, resubscribes with the same `SubscriptionId`, and
continues. No client offset is invented. Replay can return the same `event_id` under
a new `delivery_id`; applications requiring durable idempotency must persist event
IDs themselves. Dropping the stream aborts its owned task and closes the socket.

Commands are never retried automatically. `DeviceOffline` is distinct from generic
server failure, and caller-supplied `command_id` remains unchanged. Configuration
revisions are typed. Download/set success is separate from a device's application
ACK. `runtime().drain()` is explicitly administrative.
