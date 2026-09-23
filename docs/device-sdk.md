# Rust device SDK

`netbaiot-device-sdk` is optional. It maps directly onto NetbaIoT's standard MQTT
3.1.1 topics and does not introduce a tunnel or
second authentication scheme.

The builder requires `mqtt_endpoint`; the SDK is a MQTT convenience client. It operates on the caller's Tokio runtime,
keeps credentials only in memory, redacts them from `Debug`, validates all bounds,
and enables TLS verification for `mqtts` endpoints. MQTT uses the maintained
`rumqttc` 0.25 client (Apache-2.0), a bounded request channel, MQTT 3.1.1, and manual
broker acknowledgement for commands admitted to the bounded application channel.
At connection time, `connect().await` does not return until the initial
CONNACK and a successful command-topic SUBACK have been received, or the configured
connect timeout expires. SUBACK `0x80` is terminal authorization failure and is
never exposed as Connected. This makes immediate publication after a successful
connect safe.

Supported workflows are QoS0/QoS1 event publish, QoS1 telemetry, command receive,
command execution ACK, and heartbeat/config application ACK via `publish(DeviceUplink, PublishQos)`. Canonical topics are the existing `v1/t/.../up`, `down`,
and `down_ack` namespace.

`OfflinePublishPolicy::Reject` is the only current policy and the default. An MQTT
publish while disconnected returns `Offline`; the SDK never accumulates an offline
RAM queue. Successful `publish` means admission to the bounded MQTT client, not a server
acceptance receipt or business storage.

The SDK has no device configuration pull API. Management clients can read/write
revisioned configuration; no automatic MQTT/TCP config download currently replaces
the removed HTTP GET. Applications may use their existing command contract to
carry configuration, then publish `ConfigAck` after applying it. See the
[breaking change and migration](remove-device-http.md).

Dropping the final client cancels the one MQTT event-loop task. Command buffering is
16 items by default. Malformed commands or command overflow force disconnect without
acknowledging the MQTT delivery, permitting broker redelivery for persistent
sessions.

Connection loss clears the observable connected state, then retries with bounded
exponential full-jitter backoff (100 ms to 5 s by default). Every successful
reconnect explicitly resubscribes the command topic, including when the broker no
longer has the previous persistent session. `mqtt_connected()` and
`wait_until_connected()` let applications coordinate work after later outages.

UDP is specified separately as [NBI1/NBA1 reliable uplink](device-protocol.md#udp-acknowledgement-nba1). The Rust device SDK uses MQTT only; the bounded Python UDP example demonstrates exact-datagram retries and authenticated acceptance receipts.
