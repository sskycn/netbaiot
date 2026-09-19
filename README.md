# NetbaIoT

A Rust foundation for bounded IoT device access over HTTP, an embedded MQTT
server, length-prefixed TCP, and authenticated UDP. All transports normalize into
one `DeviceMessage` and use the same ingress and command services.

This milestone is not a production certification. See the [implementation
report](docs/implementation-report.md), [validation](docs/validation.md), and
[limitations](docs/mqtt.md).

## Run locally

```sh
cargo run -p netbaiot-server -- configs/development.json
```

The supplied configuration is explicitly **development only**: loopback listeners,
a published test credential, bounded in-memory storage, `volatile` receipts, and
an audit delivery sink. No external MQTT broker is used.

```sh
curl -i http://127.0.0.1:8080/v1/device/messages \
  -H 'Authorization: Bearer demo-device:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f' \
  -H 'Content-Type: application/json' \
  --data '{"schema_version":1,"source_message_id":"boot-7:42","kind":"telemetry","data":{"temperature":25.3,"humidity":61.2}}'
```

Expected: HTTP 202 and a receipt with `boundary: "volatile"`. Retry the same
application ID/content to receive the same message ID. Change its content to get
409. Acceptance does not imply downstream business processing has completed.

## Durable configuration

Copy the development configuration to ignored `configs/local.json`. Set
`development: false`; provision unique random 32-byte device keys (64 hexadecimal
characters), configure `delivery_url`, and set `DATABASE_URL`. SQL migrations and
bounded configuration provisioning run at startup. A database role needs migration
permissions for this milestone. Use a verified PostgreSQL TLS connection for
remote databases, for example `sslmode=verify-full` and its trusted root certificate.

Public HTTP/MQTT/TCP binds require `tls` with `certificate` and `private_key` PEM
paths; the configured stream ports then accept TLS exclusively. Do not use test
fixture keys for deployment. UDP is authenticated but unencrypted; restrict its
network exposure according to payload confidentiality needs.

`delivery_url` must use HTTPS, or HTTP on loopback for development. The worker
POSTs `DeviceMessage` JSON with `Idempotency-Key: <message_id>` and optionally
`NETBAIOT_DELIVERY_TOKEN`. Redirects are disabled. Consumers must deduplicate.

Set a distinct random `NETBAIOT_ADMIN_SECRET` (64 hexadecimal characters) to enable
`POST /v1/admin/commands` with `Authorization: Bearer <admin-secret>`. The body is a
`DeviceCommand`; the target must be provisioned. Without that variable the admin
route is disabled. Device credentials cannot create business commands. MQTT/TCP
receive commands on active sessions; HTTP uses `GET /v1/device/commands` and
`POST /v1/device/commands/ack`. UDP downlink is intentionally unsupported.

Authenticated `GET /metrics` exports a fixed metric vocabulary and queue gauges.
SIGINT/SIGTERM drains work and closes listeners within the shutdown deadline.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo bench -p netbaiot-transports --bench foundation
```

The PostgreSQL integration test is explicitly ignored by the default test run.
Run it against a **fresh disposable database**:

```sh
NETBAIOT_TEST_DATABASE_URL=postgres://user@localhost/netbaiot_test \
  cargo test -p netbaiot-storage --test semantics \
  postgres_transaction_and_command_contract -- --ignored
```

See [fuzz/README.md](fuzz/README.md) for fuzz commands. See
[architecture](docs/architecture.md), [MQTT](docs/mqtt.md),
[device protocol](docs/device-protocol.md), [delivery semantics](docs/delivery-semantics.md),
and [resource budgets](docs/resource-budgets.md) for contracts and boundaries.

Inspect all default bounds with:

```sh
cargo run -p netbaiot-server -- --print-default-limits
```
