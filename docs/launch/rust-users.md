# Draft for the Rust Users Forum

**Suggested title:** NetbaIoT: Rust device ingress, bounded event routing, and embedded MQTT 3.1.1/5.0

```text
Hello,

I’m working on NetbaIoT, a Rust IoT ingress gateway for applications that
already own their business backend. The runtime accepts MQTT 3.1.1, a bounded
MQTT 5.0 profile, length-prefixed TCP, and HMAC-authenticated UDP. A synchronous,
versioned codec normalizes uplinks into the public DeviceEvent model.

Some implementation details I’d like Rust-specific feedback on:

- ownership of connection-bound authentication state and invalidation fences;
- count-and-byte admission for queues, caches, sessions, and replay state;
- atomic admission across required event sinks and bounded retry ownership;
- incremental MQTT framing and separate QoS state machines;
- planned-restart snapshots, checksums, and bounded recovery decoding.

The project is single-node. Its restart spool is not crash durability, delivery
is at-least-once, UDP is not encrypted, and device commands are online-only. The
repository has raw MQTT state-machine tests, Mosquitto-client interoperability,
and fuzz targets; the README links to the exact support matrix and current
measurement caveats.

I’d appreciate review of the delivery contract, the ownership/resource model,
and the boundary between protocol state and business state.
```
