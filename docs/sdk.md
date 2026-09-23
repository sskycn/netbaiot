# SDK ecosystem

NetbaIoT exposes one public wire-model crate and two purpose-specific clients:

- `netbaiot-protocol`: portable public IDs, messages, versioning, paths, and errors;
- `netbaiot-client`: business event consumption and management APIs;
- `netbaiot-device-sdk`: optional standard MQTT 3.1.1 and device HTTP convenience;
- `netbaiot-cli`: operator/debug client built exclusively on `netbaiot-client`.

Neither client depends on `netbaiot-runtime`, `netbaiot-transports`, or
`netbaiot-server`. The protocol crate does not depend on either client. This keeps
the public contract usable by future Go, Java, Python, TypeScript, and C/C++ clients.

The server remains a database-free connectivity and real-time routing process.
Client crates do not add persistence, offline command storage, background runtime
threads, or unbounded queues. Standard MQTT clients remain fully supported; the
device SDK is convenience rather than a proprietary requirement.

Compiling examples:

```bash
cargo check --workspace --all-targets
```

Business examples are under `crates/netbaiot-client/examples`; device examples are
under `crates/netbaiot-device-sdk/examples`.

UDP is specified separately as [NBI1/NBA1 reliable uplink](device-protocol.md#udp-acknowledgement-nba1). The Rust device SDK remains MQTT/device HTTP; the bounded Python UDP example demonstrates exact-datagram retries and authenticated acceptance receipts.
