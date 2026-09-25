# NetbaIoT

[简体中文](README.zh-CN.md)

NetbaIoT is a database-free, memory-first IoT protocol gateway and real-time event
router. It accepts device traffic over embedded MQTT 3.1.1 and MQTT 5.0, generic framed
TCP, and authenticated UDP; normalizes it into `DeviceEvent`; and sends it to
confirmed or best-effort business sinks.

The runtime never requires PostgreSQL or another database. Business systems own
durable business data and offline commands. NetbaIoT's only persistent mechanism is
a bounded local restart spool used when a planned graceful shutdown cannot finish
all already accepted required deliveries.

## Quick start

Requirements: Rust 1.88 or newer, Python 3, `curl`, and Mosquitto client
tools. Mosquitto is only a client here; NetbaIoT includes its own MQTT 3.1.1/MQTT 5.0
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

- single device ingress: `127.0.0.1:8080` (TCP: MQTT/framed TCP; UDP: NBI1/NBA1)
- separate management HTTP: `127.0.0.1:9090`
- optional `business_tcp` remains separate.

Port 443 is a deployment choice for firewall compatibility, not an HTTPS promise.
Production can use `device_ingress=0.0.0.0:443`: MQTTS and TLS TCP share
one certificate; UDP uses the same numeric port and remains HMAC authenticated,
not encrypted. No ALPN or custom preface is required. The four old device address
fields are replaced by `device_ingress`; see [migration details](docs/architecture.md).

Publish the first device event with a standard MQTT 3.1.1 client:

```bash
mosquitto_pub -h 127.0.0.1 -p 8080 -V mqttv311 \
  -u demo-device -P 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f \
  -i quickstart -t v1/t/demo/p/sensor/d/device-1/up -q 1 \
  -m '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'
```

MQTT 5.0 clients use the same listener and canonical topics; change the example to
`-V mqttv5`. MQTT 3.1.1 remains the default SDK mode. See the
[MQTT compatibility profile](docs/mqtt.md) for supported MQTT 5 properties and
features that are outside this release.

MQTT QoS1 PUBACK means the event crossed the bounded
`EventAccepted` boundary. It does not mean that a business database stored it.
The Python terminal prints the normalized event and acknowledges it with HTTP
204. Continue with the Chinese [10-minute end-to-end tutorial](docs/getting-started.md)
for MQTT publish and subscribe, a live command, CLI usage, and graceful shutdown.

Set a 64-character `NETBAIOT_ADMIN_SECRET` to enable legacy bootstrap management calls. This token has full scope and Global resource access; disable it with `"legacy_static_token_enabled": false` after moving to scoped API Keys, RS256 JWT, or management mTLS. A request uses one management credential, and JWKS outages return 503. See [management authentication](docs/management-auth.md). Production
configurations must specify a confirmed webhook or framed TCP/RPC business sink.
The business sink must deduplicate by stable `event_id` because retry and restart
replay can duplicate delivery.

Dependency audits run in CI. [The audit exceptions](.cargo/audit.toml) track four
`rustls-webpki 0.102.x` advisories pinned by `rumqttc` in the optional device SDK;
they are not part of the gateway server runtime. The exceptions need removal when
an upstream compatible release is available.

See the [complete user guide](docs/user-guide.md), [architecture](docs/architecture.md), [delivery semantics](docs/delivery-semantics.md),
[HTTP API](docs/http-api.md), [MQTT profile](docs/mqtt.md), and the
[refactor report](docs/pure-event-bus-refactor.md).


Device HTTP has been removed. Existing clients must migrate to MQTT, framed TCP or
UDP; automatic device config pull has no replacement. See the
[breaking changes and migration](docs/remove-device-http.md).

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

Commands use `client.commands().send(&command)` and operations use `client.runtime()`. An offline device returns
typed `ClientError::DeviceOffline`; commands are never stored by NetbaIoT.

The optional `netbaiot-device-sdk` supports standard MQTT telemetry/commands
without lock-in. Standard MQTT 3.1.1 clients remain
first-class. The `netbaiot` CLI exposes status, event subscribe, command,
auth cache invalidation, and explicit drain operations. See [SDK overview](docs/sdk.md),
[business client](docs/client.md), [device SDK](docs/device-sdk.md), and
[CLI](docs/cli.md).

UDP v1.1 returns a signed 64-byte NBA1 receipt after EventAccepted. Lost ACKs can be retried with the exact original NBI1 datagram without duplicate ingestion within the live replay window. See [UDP protocol and retry limits](docs/device-protocol.md#udp-acknowledgement-nba1).

NetbaIoT does not own or persist device desired configuration. Applications own
persistent desired/reported state, revisions/history, retries, rollout, rollback,
and offline reconciliation. Configuration changes can travel to online MQTT/TCP
devices as ordinary `DeviceCommand` values. Devices return `CommandAck`; the
application decides whether its desired state has converged. Commands remain
online-only; UDP remains sessionless with no downlink. See the
[ownership migration](docs/remove-device-config.md).
