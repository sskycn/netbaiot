# Dependency choices

The starting repository had no dependency manifest or implementation to reuse.
The locked versions compiled and were tested on stable rustc 1.97.1. Direct
libraries are established Rust infrastructure with upstream documentation and
active versioned releases; this is not a third-party code/security audit.

| Dependency family | Purpose and tradeoff |
|---|---|
| Tokio / tokio-util | Async sockets, bounded channels, semaphores, owned JoinSets and cancellation; centralized task ownership |
| bytes | Incremental network buffers; no MQTT session behavior |
| serde / serde_json | Typed wire models; bounded preflight before parsing; no arbitrary JSON domain maps |
| thiserror | Explicit library errors; credentials never embedded in errors |
| tracing / tracing-subscriber | Structured logs with configured filtering |
| uuid | Application and command IDs independent of MQTT packet IDs |
| async-trait | Object-safe async runtime ports; small dispatch/allocation cost accepted for replaceable auth/storage |
| SQLx 0.8 | PostgreSQL transactions, bounded pool, migrations; runtime queries do not require build-time database |
| Hyper / hyper-util / http-body-util | HTTP/1 parser and explicit connection/header/body ownership; avoids another router/layer stack for four routes |
| Rustls / tokio-rustls / rustls-pemfile | TLS with ring provider; cryptographic implementation is not handwritten |
| HMAC / SHA-256 / subtle | UDP MAC and constant-time comparison for provisioned random device keys; not a password KDF for human passwords |
| reqwest | Timed business HTTP delivery, HTTPS verification, no redirects and bounded idle pool; not on device parsing path |
| libfuzzer-sys (separate fuzz workspace) | AddressSanitizer/coverage-guided native fuzzing; not linked into the server |

Primary references: [Tokio](https://docs.rs/tokio/),
[Hyper connection builder](https://docs.rs/hyper/latest/hyper/server/conn/http1/struct.Builder.html),
[SQLx](https://docs.rs/sqlx/0.8.6/sqlx/),
[tokio-rustls](https://docs.rs/tokio-rustls/),
[OASIS MQTT 3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).

No packet/broker MQTT dependency was needed: the limited packet grammar is internal
and fuzzed. Broker state, sessions, subscriptions and routing remain NetbaIoT code.
No Redis/Kafka/framework/plugin dependency was introduced. PostgreSQL-only SQLx
features and ring-only TLS features avoid unused runtime backends. Cargo.lock may
include optional/target-specific packages that are not linked into this host binary.

The selected direct Rust libraries use MIT and/or Apache-2.0 licenses (Hyper and
bytes use MIT). Their costs are recorded by Cargo.lock; transitive crypto/platform
code includes unsafe and native components. Workspace code forbids unsafe, which
does not imply dependencies contain none. A full transitive license/vulnerability
and unsafe-code audit remains future release work. No claim of completing that
audit or verifying the declared MSRV on a second toolchain is made.
