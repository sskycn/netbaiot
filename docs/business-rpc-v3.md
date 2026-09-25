# Business RPC V3 streams

Business RPC V3 is an explicit opt-in on the existing `business_tcp` listener. Set `business_rpc.version` to `2` as before and add `business_rpc.v3` with the V3 limits. A V2 Hello still selects the unchanged four-byte-length-prefixed JSON V2 protocol; a V3 Hello selects the binary stream engine. The client never silently downgrades. V3 Hello has no role: the authenticated business principal, each OPEN kind, method and tenant scope determine permission. Production uses TLS with client certificates mapped to configured principal fingerprints; the loopback development token remains development-only. The V2 identity configuration and `BusinessRole` remain for V2 compatibility.

## Wire contract

The bootstrap Hello and Ready are bounded length-prefixed JSON. Ready returns a fresh `connection_epoch` and negotiated limits. Subsequent frames use a 12-byte header: `payload_length: u32 BE`, `stream_id: u32 BE` (high bit clear), `frame_type: u8`, `flags: u8`, `reserved: u16 BE` (zero), followed by payload. The stable type numbers are OPEN 1, ACCEPT 2, RESPONSE 3, DATA 4, WINDOW_UPDATE 5, RESET_STREAM 6, CLOSE_STREAM 7, PING 8, PONG 9, GOAWAY 10. `END_STREAM = 1` is valid only on DATA and RESPONSE; all other flag bits are invalid. Header type, flags, ID and negotiated payload limit are checked before payload allocation. Metadata remains bounded JSON (4 KiB); DATA contains raw application body bytes (currently JSON DTOs). A declared `content_length` is checked against the exact reassembled byte count.

Stream 0 is limited to PING, PONG, connection WINDOW_UPDATE and GOAWAY. Client IDs are odd, gateway IDs even, strictly increasing and never reused within one connection. The stream identity is `(connection_epoch, stream_id)`; application `request_id`, stable `event_id`, `delivery_id` and `subscription_id` retain separate meanings. GOAWAY's `last_stream_id` reports the highest peer stream considered. A reconnect receives a new epoch.

Provider and EventSubscription are long-lived parent streams. A provider parent admits gateway-initiated child RPCs for `device.authenticate` and `device.resolve_verifier`, and client-initiated child RPCs for `auth.sync` and `auth.invalidate`. The provider becomes Serving only after reset sync completes and the client confirms receipt through the PING/PONG barrier. An EventSubscription parent admits gateway-initiated EventDelivery children. One delivery per subscription remains in flight; the application calls `BusinessRpcV3Delivery::ack()` only after its commit, or `nack()` on failure. A socket write, WINDOW_UPDATE or receipt by the SDK is not the business ACK. Retries may change `delivery_id` while `event_id` stays stable. V3 deliberately adds no command RPC.

Each RPC or delivery body is sliced lazily into DATA frames of at most the negotiated size; a single writer schedules frames across streams. The scheduler gives RPC work four turns for each Event turn, caps consecutive control frames at four, and skips streams without send credit. Per-stream OPEN/RESPONSE metadata must be written before that stream's DATA. Both connection and stream windows must have credit; receive credit is returned after WINDOW_UPDATE is written and is independent of application Event ACK. RESET_STREAM ends a stream and its children if it is a parent. A connection protocol violation or expired principal causes GOAWAY and close. A stream-local error should be handled with RESET_STREAM.

## Bounds and operation

Defaults: 8 KiB DATA payload, 256 concurrent streams, 256 KiB stream window, 4 MiB connection window and 5 s heartbeat setting. The hard frame payload ceiling is 16 KiB, metadata 4 KiB, message 8 MiB, server and SDK outbound body budgets 16 MiB per connection, server inbound reassembly 16 MiB per connection and 128 MiB across V3 connections. Control queues are limited to 64 frames and 256 KiB; per-connection writer and credit channels hold 256 items. These are configured limits, not measured capacity or optimal performance claims. Monitor the fixed-name `business_rpc_v3_*` metrics alongside existing auth and event metrics. Identifiers, tokens and certificate material must never become metric labels.

For a loopback setup, add the following to a valid V2 development configuration:

```json
"business_rpc": {
  "version": 2,
  "v3": {
    "max_frame_payload_bytes": 8192,
    "max_concurrent_streams": 256,
    "initial_stream_window_bytes": 262144,
    "initial_connection_window_bytes": 4194304,
    "heartbeat_ms": 5000
  },
  "tls": null,
  "development_token_env": "NETBAIOT_BUSINESS_RPC_TOKEN"
}
```

Use `BusinessRpcV3ClientConfig` and `BusinessRpcV3Client::connect` to opt in, then `wait_ready()` before relying on provider or subscription service. The client reconnects with bounded backoff and rebuilds parent streams. An old delivery handle is fenced by its connection epoch and cannot ACK on a replacement connection. TCP still has transport-level head-of-line blocking and packet loss can stall all streams; V3 only interleaves application DATA frames. Keep V2 clients during migration, enable V3 explicitly on the listener, move one client, and compare authentication latency, ACK behavior and resource use under the same workload before broad rollout.

The measured 16 KiB Event workload has not yet shown a stable, material authentication tail-latency improvement with the default 256 KiB stream window. See the [V3 readiness measurements](business-rpc-v3-production-readiness.zh-CN.md) before making a deployment decision.
