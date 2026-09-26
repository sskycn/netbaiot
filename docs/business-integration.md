# Business integration

## HTTP webhook

The built-in confirmed webhook sends the complete transport-independent event and
an `Idempotency-Key` equal to `event_id`. A configured 2xx response is the sink ACK.
Connection pooling, redirect refusal, request timeout, response content-length and
streamed-body limits, bounded concurrency, and bounded retry are enforced. An
optional bearer token comes from `NETBAIOT_DELIVERY_TOKEN` and is never logged.

## Framed TCP/RPC stream

The optional business listener is a long-lived application-ACK stream. Frames use a
four-byte network-order length followed by bounded JSON. The public v1 contract uses
versioned `hello`, `subscribe`, `ready`, `event`, and `ack` frames from
`netbaiot-protocol`. The client authenticates with `hello`, sends its filter in
`subscribe`, and waits for the server's `ready` before receiving events. Every
delivery has a `delivery_id` distinct from stable `event_id`, plus a
`subscription_id` and attempt. Use `netbaiot-client` for automatic reconnect,
resubscription, bounded buffering, and explicit application ACK.

The client must return the matching `ack` only after application processing. A
successful socket write is not an ACK. Malformed or mismatched ACKs fail the
delivery. The current implementation permits one active subscriber and serial
confirmed delivery, which keeps flow control and uncertainty bounded.

This is a global required-stream model. The subscriber filter is connection
eligibility, not a post-accept routing decision: every accepted event remains the
single TCP sink's required responsibility. A reconnect with a different filter
cannot ACK an older nonmatching event; mismatch returns retryable delivery failure
until an eligible subscriber explicitly ACKs it. No event is silently discarded
after `EventAccepted` because a current filter changed.

Hello, subscribe, event ACK reads, and writes all have hard deadlines and frame
size limits. Malformed handshakes are isolated to that connection and do not stop
the listener. Authentication, version, and validation failures use structured v1
stream errors so official clients can distinguish terminal failures from outages.
The server installs the bounded subscription before sending `ready`, so successful
handshake completion is a real admission boundary with no post-ready routing gap.

This framed RPC option is the provided high-rate streaming path. Native gRPC and
WebSocket adapters are not implemented in this revision. WebSocket remains an
optional future dashboard integration and must be best-effort unless it adds an
application-level `ACK event_id`.

Consumers must be idempotent by `event_id`: a sink can process an event and lose its
ACK immediately before a planned restart, causing the same ID to replay.

## Business RPC V2

The bidirectional V2 protocol, independent authentication provider, mTLS mapping, configuration, and Rust SDK are documented in [Business RPC Stream V2](business-rpc-v2.md). V1 remains the default when `business_rpc` is absent.

## Sending online device commands over Business RPC V2

Use an mTLS `commands` principal for command-only services, or `application` to receive confirmed events and send commands on one connection. Both must have `call_methods: ["device.command.send"]`; `application` also requires `sink_id: "tcp-rpc"`. `BusinessRpcClient::send_command(&command)` returns a `Queued` dispatch when the gateway accepts the command for the current live session. It does not wait for device execution. Match later `CommandAck` events by `command_id`, commit application work, then ACK the event. Preserve the same `command_id` when explicitly retrying an unknown RPC outcome; each attempt gets its own `request_id`. The process-local dedup window is `command_dedup_ttl_ms` and is lost on process restart. Offline commands are rejected and remain the business system's responsibility. Management HTTP `/api/v1/devices/commands` remains available for operations and older clients, sharing the same command dedup behavior. See [Business RPC Stream V2](business-rpc-v2.md) for limits and failure semantics.

## Sending online device commands over Business RPC V3

Configure an mTLS principal with `call_methods: ["device.command.send"]` and the intended tenant scope; add `sink_id: "tcp-rpc"` if it also consumes events. Set `business_rpc.v3` and use `BusinessRpcV3Client::send_command(&command)` after `wait_ready()`. V3 uses an independent RPC stream and the same request/response DTO as V2. `Queued`, `CommandAck`, `OutcomeUnknown`, conflict, offline, and process-local dedup semantics are described in [Business RPC V3](business-rpc-v3.md). A command-only client can set `provider = false` and `events = false`. See the [V3 command example](../crates/netbaiot-client/examples/business_v3_command.rs).
