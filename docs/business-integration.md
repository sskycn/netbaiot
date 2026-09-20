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

This framed RPC option is the provided high-rate streaming path. Native gRPC and
WebSocket adapters are not implemented in this revision. WebSocket remains an
optional future dashboard integration and must be best-effort unless it adds an
application-level `ACK event_id`.

Consumers must be idempotent by `event_id`: a sink can process an event and lose its
ACK immediately before a planned restart, causing the same ID to replay.
