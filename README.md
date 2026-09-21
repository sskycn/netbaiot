# NetbaIoT

[简体中文](README.zh-CN.md)

NetbaIoT is a database-free, memory-first IoT protocol gateway and real-time event
router. It accepts device traffic over HTTP, embedded MQTT 3.1.1, generic framed
TCP, and authenticated UDP; normalizes it into `DeviceEvent`; and sends it to
confirmed or best-effort business sinks.

The runtime never requires PostgreSQL or another database. Business systems own
durable business data and offline commands. NetbaIoT's only persistent mechanism is
a bounded local restart spool used when a planned graceful shutdown cannot finish
all already accepted required deliveries.

## Run locally

```bash
cargo run -p netbaiot-server -- configs/development.json
```

Development listeners are:

- device HTTP: `127.0.0.1:8080`
- management HTTP: `127.0.0.1:9090`
- embedded MQTT: `127.0.0.1:1883`
- generic TCP: `127.0.0.1:9000`
- UDP: `127.0.0.1:9001`

Upload a device event:

```bash
curl --noproxy '*' -i http://127.0.0.1:8080/v1/device/data \
  -H 'Authorization: Bearer demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f' \
  --data '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'
```

HTTP `202` and MQTT QoS1 PUBACK mean the event crossed the bounded
`EventAccepted` boundary. They do not mean that a business database stored it.

Set a 64-character `NETBAIOT_ADMIN_SECRET` to enable management calls. Production
configurations must specify a confirmed webhook or framed TCP/RPC business sink.
The business sink must deduplicate by stable `event_id` because retry and restart
replay can duplicate delivery.

See [architecture](docs/architecture.md), [delivery semantics](docs/delivery-semantics.md),
[HTTP API](docs/http-api.md), [MQTT profile](docs/mqtt.md), and the
[refactor report](docs/pure-event-bus-refactor.md).

## Official Rust clients

Business systems use `netbaiot-client`; event ACK is explicit and occurs after
application processing:

```rust
let client = NetbaIoTClient::builder()
    .endpoint(endpoint)
    .token(token)
    .event_address(event_address)
    .connect()
    .await?;
let mut events = client.events().subscribe(EventFilter::default()).await?;
while let Some(delivery) = events.next().await {
    let delivery = delivery?;
    handle(delivery.event()).await?;
    delivery.ack().await?;
}
```

Commands use `client.commands().send(&command)`, configuration uses
`client.configs()`, and operations use `client.runtime()`. An offline device returns
typed `ClientError::DeviceOffline`; commands are never stored by NetbaIoT.

The optional `netbaiot-device-sdk` supports standard MQTT telemetry/commands and
device HTTP upload/config without lock-in. Standard MQTT 3.1.1 clients remain
first-class. The `netbaiot` CLI exposes status, event subscribe, command, config,
cache invalidation, and explicit drain operations. See [SDK overview](docs/sdk.md),
[business client](docs/client.md), [device SDK](docs/device-sdk.md), and
[CLI](docs/cli.md).
