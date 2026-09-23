# Device protocol: netbaiot-json-v1

## Single Device Ingress

`device_ingress` binds one TCP listener and one UDP socket at the same address and
numeric port (development: `127.0.0.1:8080`; production example: `0.0.0.0:443`).
TCP serves standard MQTT 3.1.1 over TLS, and generic framed TCP over TLS using
one certificate. TLS finishes before application classification; no ALPN, custom
preface, or client wire change is required. UDP on the same port remains NBI1/HMAC,
authenticated but unencrypted; this does not add DTLS or QUIC.

Management HTTP (`management_http`, normally `127.0.0.1:9090`) and optional
`business_tcp` retain separate listeners and authorization. Management HTTP is a
control-plane protocol and never participates in device classification. Non-loopback TCP ingress requires TLS. Development mode
requires loopback and permits plaintext for local testing.

`device_ingress` is the only device address. Legacy separate-listener fields are
rejected. Port 443 is only a deployment choice, not an HTTPS endpoint. HTTP bytes
on device ingress close without an HTTP response; see [migration](remove-device-http.md).

Authentication selects codec ID `netbaiot-json`, version `1`. The same synchronous
codec decodes MQTT/TCP/UDP payloads. Devices cannot supply their own trusted
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

## UDP v1.1 signed reliable uplink

NBI1 (device → gateway) is unchanged. No sessions, command downlink, endpoint
registry, encryption, or application fragmentation. Datagrams are at most 1200
bytes. All integer fields use network byte order:

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

HMAC covers all preceding bytes using the **decoded 32-byte credential key**, not
ASCII hex. Credential version must match provisioning. Every datagram, including
retries, must pass HMAC, authorization, and timestamp skew (default ±30 seconds).

Each `(DeviceKey, credential_version, boot_id)` has a bounded 64-sequence bitmap.
A new sequence enters the codec/Ingress/EventBus, then commits replay **only after
EventAccepted**. An accepted duplicate skips codec, event ID generation, presence,
and EventBus entirely and receives another ACK. Sequences outside the 64-slot
window are silently dropped, even if previously accepted. Records expire after
120 seconds by default (strictly longer than twice clock skew); only expired
records are evicted. Duplicate ACKs do not extend expiry. Device/tenant/global
limits also count separate credential versions; full capacity rejects new records.
Failed ingestion never consumes a sequence.

## UDP acknowledgement: NBA1

NBA1 (gateway → device) is a fixed **64-byte signed acceptance receipt**:

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | magic `NBA1` |
| 4 | 4 | credential_version, u32 big-endian |
| 8 | 16 | boot_id |
| 24 | 8 | sequence, u64 big-endian |
| 32 | 32 | HMAC-SHA256 over bytes `[0..32)` |

The HMAC uses the same decoded 32-byte key as NBI1. NBA1 means **EventAccepted**,
at the same acceptance level as MQTT QoS1 PUBACK, and the generic TCP
acceptance receipt. It does **not** mean final required-sink ACK, database commit,
business processing, or device command execution. A codec `CommandAck` is a
separate application event; NBA1 may acknowledge acceptance of that event.

The device must check: length exactly 64; magic NBA1; expected credential version;
current boot ID; an outstanding sequence; and a valid constant-time HMAC. Source
IP or a matching sequence alone is insufficient. NBA1 has no status or event_id.

### Retry and message identity

Within an authenticated device, `(credential_version, boot_id, sequence)` identifies
one immutable message. If no valid NBA1 arrives, **resend the exact original NBI1
datagram**, preserving credential version, boot ID, sequence, timestamp, payload,
and HMAC. Do not refresh the timestamp or use a new sequence for an ACK retry.
Reusing an accepted identity for different content violates the protocol: the
first accepted message wins, without storing payload copies or per-sequence IDs.

Use bounded exponential backoff (for example 100, 200, 400, 800, 1600 ms) with
jitter. Finish retries before the original timestamp becomes invalid, allowing for
actual clock skew, and before newer traffic moves the sequence out of the 64-slot
window. Beyond either bound, delivery is **uncertain**, not known to have failed.

Replay state is memory-only. Reliable UDP ACK prevents duplicate ingestion during
the lifetime of the replay window in the same runtime instance. It does not provide
exactly-once semantics across unexpected process restart. Planned restart also
rebuilds replay state. Preserve a stable `source_message_id` for business idempotency;
EventBus replay preserves event_id, but renewed UDP ingestion may create a new one.

### Failure, resource, and security boundaries

Malformed, unauthenticated, unknown/stale credentials, bad clocks, too-old replay,
codec/authorization/admission/EventBus failures and draining receive **no response**.
There are no NACKs. After acceptance, replay is committed before a nonblocking
`try_send_to`. Send pressure/failure drops the ACK and increments a counter; it never
rolls back acceptance. A subsequent accepted duplicate can retry the receipt.
Credential invalidation fences in-progress signing. There is no server ACK queue,
retransmission task, ACK drain, or ACK spool, and UDP never registers a session.

NBA1 is 64 bytes versus at least 76 bytes for a structurally valid authenticated
NBI1 request: payload-byte amplification is at most **64/76 ≈ 0.842**. Unauthenticated
traffic never receives a reply. Captured valid signed datagrams with spoofed source
addresses can still cause authenticated reflection within the time window; smaller
fixed responses and the existing source-IP/process rate limits prevent byte
amplification. Duplicate ACKs pass those same limits. Payloads remain unencrypted.

Counters `udp_datagrams`, `udp_accepted`, `udp_accepted_duplicates`, `udp_acks_sent`, and
`udp_ack_send_failures` distinguish incoming traffic, new accepted datagrams, duplicate fast paths, local
socket emission, and dropped/suppressed receipts. Local emission does not prove
receipt at the device. No device IDs or other high-cardinality labels are added.

Breaking cleanup in the current 0.x release: `config_ack` is no longer a supported
DeviceEvent kind and is rejected. `schema_version=1` remains the unchanged envelope
version; there is no compatibility alias. Application configuration operations use
ordinary MQTT/TCP commands and `command_ack`. See [migration](remove-device-config.md).

## Connection presence

The JSON v1 device uplink kinds remain `telemetry`, `event`, `heartbeat`, and
`command_ack`. The normalized business event kinds are `telemetry`, `device_event`,
`heartbeat`, and `command_ack`. Connection lifecycle kinds are not accepted in
uplinks, business events, or subscription filters. Query current connection state
through `/api/v1/connections` and `/api/v1/devices/connection`; UDP updates
`last_seen` without creating a connected session. Presence history belongs to the
business application, not the EventBus.
