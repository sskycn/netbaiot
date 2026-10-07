# Business integration

## HTTP webhook

The built-in confirmed webhook sends the complete transport-independent event and
an `Idempotency-Key` equal to `event_id`. A configured 2xx response is the sink ACK.
Connection pooling, redirect refusal, request timeout, response content-length and
streamed-body limits, bounded concurrency, and bounded retry are enforced. An
optional bearer token comes from `NETBAIOT_DELIVERY_TOKEN` and is never logged.

## Current Business RPC

[Business RPC V3](business-rpc-v3.md) is the only business stream protocol. The explicit `business_rpc` configuration supplies current limits and an mTLS principal map, or a development-only loopback token. The bootstrap is bounded length-prefixed JSON; application traffic uses the binary stream framing. No listener version dispatch or downgrade exists.

An authenticated EventSubscription installs the one active owner of required sink `tcp-rpc`. Its filter controls eligibility: an already accepted required event cannot be discarded because a reconnect uses a different filter. Each EventDelivery has a stable `event_id`, a separate `delivery_id`, `subscription_id` and attempt. Commit application work before ACK; a socket write or WINDOW_UPDATE is not acknowledgement. Missing, malformed or mismatched ACKs retain retryable responsibility. Reconnect/restart may replay the same `event_id`, so business processing must be idempotent. Use the [official Rust client](client.md) for bounded buffering, cancellation and manual ACK.

## Online commands and authentication

The current client can independently own Provider and EventSubscription streams. Providers implement `BusinessAuthHandler`, reset sync, `device.authenticate`, `device.resolve_verifier` and revisioned invalidation. Session authentication is bound once; normal MQTT/TCP uplinks never call the provider. A revision gap revokes affected authorization and requires reset sync. Management HTTP and RPC share the complete admission/invalidation fence.

For commands, configure an mTLS principal with `call_methods: ["device.command.send"]` and the intended tenant scope. Add `sink_id: "tcp-rpc"` when it consumes confirmed events. Use `BusinessRpcV3Client::send_command(&command)` after readiness; command-only clients set `provider = false` and `events = false`. `Queued` means the live local MQTT/TCP session accepted dispatch. Later device `CommandAck` is an ordinary event; it is distinct from transport SENT. An unknown result is `OutcomeUnknown`; applications choose whether to retry the same contents and `command_id`. Conflict, TTL, count/byte limits and offline rejection remain enforced. HTTP `/api/v1/devices/commands` shares the same process-local dedup service. No offline command or durable command history is stored. See the [current command example](../crates/netbaiot-client/examples/business_v3_command.rs).
