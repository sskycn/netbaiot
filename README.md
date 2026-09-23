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

## Quick start

Requirements: Rust 1.88 or newer, Python 3, `curl`, and optional Mosquitto client
tools. Mosquitto is only a client here; NetbaIoT includes its own MQTT 3.1.1
broker.

Build the locked workspace, start the tutorial business consumer, then start the
gateway:

```bash
cargo +1.88.0 build --locked
python3 examples/business_http_sink.py
```

In another terminal:

```bash
export NETBAIOT_ADMIN_SECRET=abababababababababababababababababababababababababababababababab
cargo run -p netbaiot-server -- configs/tutorial.json
```

Development listeners are:

- single device ingress: `127.0.0.1:8080` (TCP: HTTP/MQTT/framed TCP; UDP: NBI1)
- separate management HTTP: `127.0.0.1:9090`
- optional `business_tcp` remains separate.

Production can use `device_ingress=0.0.0.0:443`: HTTPS, MQTTS and TLS TCP share
one certificate; UDP uses the same numeric port and remains HMAC authenticated,
not encrypted. No ALPN or custom preface is required. The four old device address
fields are replaced by `device_ingress`; see [migration details](docs/architecture.md).

Publish the first device event (HTTP requires no additional client package):

```bash
curl --noproxy '*' -i http://127.0.0.1:8080/v1/device/data \
  -H 'Authorization: Bearer demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f' \
  --data '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'
```

HTTP `202` and MQTT QoS1 PUBACK mean the event crossed the bounded
`EventAccepted` boundary. They do not mean that a business database stored it.
The Python terminal prints the normalized event and acknowledges it with HTTP
204. Continue with the Chinese [10-minute end-to-end tutorial](docs/getting-started.md)
for MQTT publish and subscribe, a live command, CLI usage, and graceful shutdown.

Set a 64-character `NETBAIOT_ADMIN_SECRET` to enable management calls. Production
configurations must specify a confirmed webhook or framed TCP/RPC business sink.
The business sink must deduplicate by stable `event_id` because retry and restart
replay can duplicate delivery.

See the [complete user guide](docs/user-guide.md), [architecture](docs/architecture.md), [delivery semantics](docs/delivery-semantics.md),
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

UDP v1.1 returns a signed 64-byte NBA1 receipt after EventAccepted. Lost ACKs can be retried with the exact original NBI1 datagram without duplicate ingestion within the live replay window. See [UDP protocol and retry limits](docs/device-protocol.md#udp-acknowledgement-nba1).
