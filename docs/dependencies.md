# Runtime dependencies

The production workspace uses Tokio, bytes, serde/serde_json, thiserror, tracing,
uuid, Axum-compatible Hyper primitives, rustls, reqwest, HMAC/SHA-256, and subtle
constant-time comparison. MQTT is implemented directly.

There is no SQLx or database/storage crate and no PostgreSQL, SQLite, Redis,
RocksDB, Kafka, NATS, RabbitMQ, LMDB, or sled runtime dependency. The local restart
spool uses only bounded versioned files and is not a database.

The optional device SDK uses `netbaiot-mqtt-wire`, Tokio, rustls and `url` for MQTT;
`rumqttc` is no longer in the workspace dependency graph. The SDK has no `reqwest`,
Hyper or HTTP-body dependency. The server's management HTTP, authentication provider
and business webhook, plus the management client, retain their HTTP dependencies.

Native device payloads additionally use pinned ciborium 0.2.2, rmp-serde 1.3.1 and
prost 0.14.4. Protobuf generation uses prost-build 0.14.4 and bundled protoc-bin-vendored
3.2.0 at build time only; no protoc or service is required at runtime.
