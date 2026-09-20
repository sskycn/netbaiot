# Runtime dependencies

The production workspace uses Tokio, bytes, serde/serde_json, thiserror, tracing,
uuid, Axum-compatible Hyper primitives, rustls, reqwest, HMAC/SHA-256, and subtle
constant-time comparison. MQTT is implemented directly.

There is no SQLx or database/storage crate and no PostgreSQL, SQLite, Redis,
RocksDB, Kafka, NATS, RabbitMQ, LMDB, or sled runtime dependency. The local restart
spool uses only bounded versioned files and is not a database.
