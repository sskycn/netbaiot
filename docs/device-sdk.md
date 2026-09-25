# Rust device SDK

`netbaiot-device-sdk` is optional. It maps directly onto NetbaIoT's standard MQTT
3.1.1/MQTT 5.0 topics and does not introduce a tunnel or
second authentication scheme.

The builder requires `mqtt_endpoint`; the SDK is a MQTT convenience client. It operates on the caller's Tokio runtime,
keeps credentials only in memory, redacts them from `Debug`, validates all bounds,
and enables TLS verification for `mqtts` endpoints. MQTT uses the repository-owned `netbaiot-mqtt-wire` primitives and an independent
Device Profile state machine. MQTT 3.1.1 remains the default; MQTT 5.0 uses
`protocol_version(MqttProtocolVersion::V5)`. Commands use manual
broker acknowledgement for commands admitted to the bounded application channel.
At connection time, `connect().await` does not return until the initial
CONNACK and a successful command-topic SUBACK have been received, or the configured
connect timeout expires. SUBACK `0x80` is terminal authorization failure and is
never exposed as Connected. This makes immediate publication after a successful
connect safe.

MQTT 5 mode also accepts `session_expiry_interval(seconds)` and
`message_expiry_interval(Some(seconds))`. The latter applies to telemetry and
command ACK publishes. The default remains MQTT 3.1.1 and offline publishes
remain rejected.

Supported workflows are QoS0/QoS1 event publish, QoS1 telemetry, command receive,
command execution ACK, and heartbeat via `publish(DeviceUplink, PublishQos)`. Canonical topics are the existing `v1/t/.../up`, `down`,
and `down_ack` namespace.

`OfflinePublishPolicy::Reject` is the only current policy and the default. An MQTT
publish while disconnected returns `Offline`; the SDK admits no new offline
publishes. Work accepted before a disconnect can remain bounded in memory for
session recovery. Successful `publish` means admission to the bounded MQTT client, not a server
acceptance receipt or business storage.

NetbaIoT does not own or persist device desired configuration. Applications own
persistent desired/reported state, revisions/history, retries, rollout, rollback,
and offline reconciliation. Configuration changes can travel to online MQTT/TCP
devices as ordinary `DeviceCommand` values. Devices return `CommandAck`; the
application decides whether its desired state has converged. Commands remain
online-only; UDP remains sessionless with no downlink. See the
[ownership migration](remove-device-config.md).

Dropping the final client cancels the one MQTT event-loop task. Command buffering is
16 items by default. Malformed commands or command overflow force disconnect without
acknowledging the MQTT delivery. A persistent broker session can redeliver an
unacknowledged QoS1 command; QoS0 has no such guarantee.

Connection loss clears the observable connected state, then retries with bounded
exponential full-jitter backoff (100 ms to 5 s by default). Every successful
reconnect explicitly resubscribes the command topic, including when the broker no
longer has the previous persistent session. `mqtt_connected()` and
`wait_until_connected()` let applications coordinate work after later outages.

UDP is specified separately as [NBI1/NBA1 reliable uplink](device-protocol.md#udp-acknowledgement-nba1). The Rust device SDK uses MQTT only; the bounded Python UDP example demonstrates exact-datagram retries and authenticated acceptance receipts.

## Profile, migration and limits

| Capability | MQTT 3.1.1 | MQTT 5.0 |
| --- | --- | --- |
| TCP, verified TLS, existing username/password | Yes | Yes |
| Fixed up/down/down_ack topics, QoS0/1 uplink, QoS1 command subscription | Yes | Yes |
| Persistent reconnect | CleanSession=0 | Clean Start=0, default Session Expiry 3600 s |
| Flow/expiry negotiation | Not applicable | Receive Maximum, Maximum Packet Size, Server Keep Alive, Message Expiry |

The SDK does not expose QoS2, arbitrary topics, retained publishing, LWT configuration,
Topic Alias, WebSocket, enhanced authentication, or an offline disk queue. These SDK
limits do not change the gateway broker's standard MQTT features. Existing public
`DeviceClient`/builder, credentials, errors, `PublishQos`, version, offline and reconnect
policy, `publish`, `publish_telemetry`, `commands`, `ack_command`, `mqtt_connected`,
`wait_until_connected`, `metrics`, and `shutdown` calls remain available. The source
compatibility additions are `publish_receipts()`, `shutdown_with_timeout()`,
`mqtt_ca_pem()`, and explicit count/byte limit builders.
`DeviceSdkError::SessionStateMismatch` is a new public variant; downstream
exhaustive matches on `DeviceSdkError` must add a branch. A mismatched old
broker session is normally repaired internally, so applications usually see
this variant only when the broker contradicts the clean-session reset.

`publish` still means local bounded admission. `publish_receipts()` is a bounded
broadcast of `Written`, `Puback`, `Rejected(reason)`, `SessionLost`, `Expired`, and
`Uncertain`; subscribe before publishing if results matter. A lagging receiver gets a
broadcast lag error rather than an unlimited backlog. QoS0 `Written` is a socket
write. QoS1 `Puback` is an MQTT broker confirmation. On the NetbaIoT broker, PUBACK
means EventAccepted; it never proves business processing or command execution.
`ack_command` is a separate application execution report via `down_ack`. A command
is MQTT-PUBACKed only after fixed-topic/full-DeviceKey validation and successful
admission to the bounded application queue. A device process crash can still lose an
unexecuted in-memory command after PUBACK.

The default payload limit is 65,536 bytes; the complete MQTT packet limit is 131,072
bytes. The send queue, QoS1 inflight table, and command queue each default to 16
items; command bytes default to 1 MiB. The handshake deadline is 10 s, KeepAlive
15 s, and reconnect full-jitter bounds are 100 ms to 5 s. Builders expose
`max_payload_bytes`, `max_packet_bytes`, `mqtt_queue_items`,
`qos1_inflight_items`, `command_buffer_items`, `command_buffer_bytes`,
`mqtt_connect_timeout`, and `reconnect_policy`. Queue and inflight bytes are bounded;
there is no new offline publish queue. `shutdown_with_timeout(timeout)` stops new
admission, waits for accepted work up to the timeout, sends DISCONNECT, and joins
the owned task. The synchronous `shutdown()` requests immediate closure.

The same client process retains unacknowledged QoS1 packet IDs and payloads for
permitted session recovery. MQTT 5 Session Present=0 reports old inflight work as
`SessionLost`; Session Present=1 replays it with the original ID, DUP and reduced
Message Expiry. If a new process sees an old MQTT 5 broker session without local
state, it reconnects with Clean Start to establish consistent state. MQTT 3.1.1
clears such an unknown broker session with a temporary CleanSession=1 connection,
then establishes a fresh persistent CleanSession=0 session; normal same-process
reconnect retains its QoS1 replay behavior. Client state is never persisted
to disk, so process restart is not full client-session recovery.

Use `mqtts://host:port` for production credentials. The SDK verifies the server
certificate and host/IP name against public roots; `mqtt_ca_pem(path)` adds a private
CA. Endpoints with embedded credentials, paths, queries, or fragments are rejected.
The [runnable example](../crates/netbaiot-device-sdk/examples/device_mqtt.rs) selects
MQTT 5 with `NETBAIOT_MQTT_VERSION=5` and a private CA with
`NETBAIOT_MQTT_CA_PEM`. Set `NETBAIOT_WAIT_COMMAND=1` to wait for one command;
the example executes `example_noop` and reports its result with `ack_command`.
`NETBAIOT_RECONNECT_CHECK=1` runs a broker restart exercise. The
[Mosquitto test](../tests/run_device_profile_mosquitto.py) exercises both
versions over TCP/TLS, persistent reconnect, and untrusted, wrong-host, and
expired certificate rejection.
The [validation record](device-profile-validation.md) includes local RSS,
thread, QoS1 latency, and reconnect measurements with reproduction commands.
