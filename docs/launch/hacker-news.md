# Draft for Hacker News

**Title:** Show HN: NetbaIoT – a database-free IoT ingress gateway written in Rust

```text
Hi HN,

I'm the author of NetbaIoT. It is a single-node device ingress gateway that sits
in front of an existing application backend. It accepts MQTT 3.1.1, a bounded
MQTT 5.0 profile, framed TCP, and authenticated UDP, then routes normalized
DeviceEvent values to business sinks.

The design boundary is intentional: the gateway handles connections,
authentication, decoding, routing, and live commands. Applications own durable
business data, offline command intent, workflows, and idempotency. Required sink
admission has an explicit boundary, and runtime queues and caches have count and
byte limits.

It is not a clustered broker or a complete IoT platform. UDP is not encrypted,
and the local restart spool covers successful planned shutdowns rather than
arbitrary crashes. The README includes a loopback MQTT demo; Mosquitto is only
used as a client.

I'd be interested in feedback on the protocol boundary, delivery contract, and
whether this role is useful beside an existing backend.
```
