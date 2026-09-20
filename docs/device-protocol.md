# Device protocol: netbaiot-json-v1

Authentication selects codec ID `netbaiot-json`, version `1`. The same synchronous
codec decodes HTTP/MQTT/TCP/UDP payloads. Devices cannot supply their own trusted
identity in the payload; unknown envelope fields are rejected.

```json
{"schema_version":1,"source_message_id":"boot-7:42","kind":"telemetry","data":{"temperature":25.3,"humidity":61.2}}
```

`source_message_id` is mandatory (1–64 namespace-safe ASCII characters). Reuse it
for retries with identical content. `occurred_at` is optional Unix milliseconds;
`received_at` and stable `event_id` are assigned by the server. The codec preserves typed numeric,
boolean and text scalar fields; arbitrary JSON objects are not domain payloads.

Other `kind` / `data` pairs:

```json
{"kind":"event","data":{"name":"boot","value":true}}
{"kind":"heartbeat","data":{"sequence":42}}
{"kind":"config_ack","data":{"revision":7,"status":"applied","error":null}}
{"kind":"command_ack","data":{"command_id":"00000000-0000-0000-0000-000000000001","execution":"succeeded"}}
```

These examples show the kind/data portion; also include schema_version and
source_message_id. Execution may be running/succeeded/failed. Business systems
correlate and persist command/application results by `command_id` when required.

Codec defaults: 64 KiB input/encoded bytes, one output message, 64 telemetry fields,
256-byte names/text fields, depth 8. A structural member-count preflight limits
allocation before serde; duplicate telemetry names are rejected. Invalid UTF-8,
unknown fields, malformed JSON and oversized structures fail. A future multi-message
codec needs a matching atomic batch receipt design; ingress currently requires one.

## HTTP

`POST /v1/device/data` with `Authorization: Bearer <credential-id>:<key>`.
Successful configured acceptance returns 202; errors map to 400/401/403/409/413/429/
503/504. Request and header bounds are enforced by Hyper and the adapter. Any
Content-Encoding header is rejected with 415; request decompression is unsupported.
Authenticated request-stage permits cover slow bodies at device, tenant and node levels. HTTP/1
uses one request per connection in this milestone; header/body/response deadlines
bound slow clients. HTTP devices do not have an offline command queue. POST typed
configuration and command results to `/v1/device/config/ack` and
`/v1/device/commands/ack`.

## Generic TCP

Every frame is `u32` big-endian payload length followed by payload. Length must be
1..max_tcp_frame_size. The first frame is an authentication handshake:

```json
{"credential_id":"demo-device","secret":"<64-hex-character-key>"}
```

The server returns a framed `{"authenticated":true}`. Subsequent frames are JSON
uplinks. Server frames contain receipts or common `DeviceCommand` JSON. A command
has command_id, device, expires_at, and payload `{name,arguments}`. Execution ACKs
use the shared codec. Partial frames survive fragmented reads; EOF closes and drops
connection-owned resources. Vendor framing can implement `TcpFramer` separately.

## UDP v1

No sessions, replies, command downlink or application fragmentation. Datagrams
are at most 1200 bytes. Network byte order:

| Field | Bytes |
|---|---:|
| magic `NBI1` | 4 |
| credential ID length | 1 |
| credential ID | length, 1–64 |
| credential version | 4 |
| boot ID | 16 |
| sequence | 8 |
| Unix timestamp in milliseconds (signed i64) | 8 |
| payload length | 2 |
| JSON v1 payload | length |
| HMAC-SHA256 | 32 |

HMAC covers all preceding bytes and uses the **decoded 32-byte key**, not the ASCII
hex text. Credential version must match provisioning. Timestamp skew is at most
30 seconds. Each device/boot has a 64-sequence bitmap, supporting bounded reorder
while rejecting duplicates and older packets. Replay records expire after 120
seconds, longer than twice timestamp skew; only expired records are evicted.
New boots are rejected when device/tenant/global capacity is full. Replay is
committed after ingestion succeeds, so failed ingestion does not consume sequence.

A client retry after a lost/uncertain UDP send should use a new sequence. No datagram gets
a response, including unauthenticated traffic; there is no amplification or forged
application-receipt channel. Use HTTP/MQTT/TCP if receipts are required. Payloads
are authenticated, not encrypted. Replay state is process-local and rebuilt after
restart, so a valid signed packet still inside the timestamp window can be accepted
again; business consumers must deduplicate stable application identifiers when that
risk matters.
