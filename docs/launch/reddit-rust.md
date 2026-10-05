# Draft for r/rust

**Title:** I built a database-free IoT gateway in Rust — MQTT 3.1.1/5, TCP and authenticated UDP

```text
I built NetbaIoT as a small ingress gateway for systems that already have a
business backend. It accepts MQTT 3.1.1, a bounded MQTT 5.0 profile, framed TCP,
and HMAC-authenticated UDP, then normalizes device uplinks into one DeviceEvent
model.

The gateway does not require a runtime database or external MQTT broker. Required
sink admission is explicit, queues and caches have count and byte limits, and
commands are delivered only to a live local MQTT/TCP session. Business systems
keep durable telemetry, offline command intent, and workflow state.

The limits are deliberate: it is single-node; UDP is authenticated but not
encrypted; recovery is for planned graceful restarts and is not crash durability;
and it is not a full IoT platform. MQTT over WebSocket, shared subscriptions,
MQTT-SN, and bridge mode are not in the current profile.

The README has a loopback demo with a Mosquitto client and a dependency-free
webhook receiver. I’d especially appreciate feedback on the delivery semantics,
MQTT state handling, and bounded-resource design.
```
