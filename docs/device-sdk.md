# Rust device SDK

`netbaiot-device-sdk` is optional. It maps directly onto NetbaIoT's standard MQTT
3.1.1 topics and `/v1/device/...` HTTP endpoints and does not introduce a tunnel or
second authentication scheme.

The builder accepts MQTT, HTTP, or both. It operates on the caller's Tokio runtime,
keeps credentials only in memory, redacts them from `Debug`, validates all bounds,
and enables TLS verification for `mqtts`/`https` endpoints. MQTT uses the maintained
`rumqttc` 0.25 client (Apache-2.0), a bounded request channel, MQTT 3.1.1, and manual
broker acknowledgement for commands admitted to the bounded application channel.
When MQTT is configured, `connect().await` does not return until the initial
CONNACK and a successful command-topic SUBACK have been received, or the configured
connect timeout expires. SUBACK `0x80` is terminal authorization failure and is
never exposed as Connected. This makes immediate publication after a successful
connect safe.

Supported workflows are QoS0/QoS1 event publish, QoS1 telemetry, command receive,
command execution ACK, HTTP data/heartbeat upload, conditional config GET, and
config application ACK. Canonical topics are the existing `v1/t/.../up`, `down`,
and `down_ack` namespace.

`OfflinePublishPolicy::Reject` is the only current policy and the default. An MQTT
publish while disconnected returns `Offline`; the SDK never accumulates an offline
RAM queue. Successful `publish` means admission to the bounded MQTT client, while a
successful HTTP upload means only `EventAccepted`. Neither means business storage.

`config().check(Some(revision))` uses ETag/304 and returns `Unchanged` or a typed
`Updated(DeviceConfig)`. The application applies and persists the revision itself,
then calls `config().ack(...)`. Download success is never treated as apply success.

Dropping the final client cancels the one MQTT event-loop task. Command buffering is
16 items by default. Malformed commands or command overflow force disconnect without
acknowledging the MQTT delivery, permitting broker redelivery for persistent
sessions.

Connection loss clears the observable connected state, then retries with bounded
exponential full-jitter backoff (100 ms to 5 s by default). Every successful
reconnect explicitly resubscribes the command topic, including when the broker no
longer has the previous persistent session. `mqtt_connected()` and
`wait_until_connected()` let applications coordinate work after later outages.
