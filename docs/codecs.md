# Device payload codecs

[中文](codecs.zh-CN.md)

The server registers these synchronous codecs once at startup. All four normalize
one authenticated uplink to the existing `DeviceEvent`, and encode online commands
using that device's bound profile. Business consumers still deduplicate `event_id`.

| Codec ID | Version | Uplink kinds | Downlink |
| --- | --- | --- | --- |
| `netbaiot-json` | 1 | telemetry, event, heartbeat, command_ack | Existing DeviceCommand JSON |
| `netbaiot-cbor` | 1 | Same four kinds | Native CBOR DeviceCommand map |
| `netbaiot-msgpack` | 1 | Same four kinds | Native MessagePack DeviceCommand map |
| `netbaiot-protobuf` | 1 | Same four kinds | Protobuf DeviceCommand message |

## Selection and product consistency

Provision `AuthenticatedDevice.codec_id` and `codec_version` through static
credentials, the HTTP auth provider or Business RPC V3. All providers pass through
the same registered-profile check before session registration, and registration
rechecks under the existing auth-registration fence. Normal MQTT/TCP packets do
not call the provider. A payload cannot select or override its trusted codec or
identity; there is no decoder probing, fallback, or automatic format negotiation.
UDP retains HMAC, timestamp, credential-version and replay checks for each datagram.

A tenant/product has one codec ID/version. Conflicting static credentials fail
startup, `config check`, and `doctor`; unknown IDs, zero/unsupported versions and
duplicate credentials are errors. Existing product profiles must agree with the
authentication result. When a dynamic provider supplies a product absent from the
control snapshot, its supported profile is authoritative for the immutable session.
Adding a product profile that conflicts with a live session fails atomically.

Control snapshots validate every codec before publication. Revisions and atomic
replacement remain in effect. V1 rejects changing or removing an established
product codec profile in a running process; it preserves the previous snapshot and
sessions. Plan a drain/restart and reprovision authentication to switch codecs.
Invalidate the affected product authentication and sessions before reprovisioning;
on reconnect, the existing MQTT provenance check resets any recovered session
whose codec ID/version, credential version, generation or permissions differ.
Never mutate an established authentication object to switch its encoding.

[configs/multi-codec.json](../configs/multi-codec.json) provisions four separate
products on one gateway, using only the existing configuration fields. Its public
keys are development fixtures. Production deployments provision their own keys.
The configuration JSON Schema is unchanged because no configuration fields or
public protocol fields were added.

## JSON V1

The existing [JSON device wire format](device-protocol.md) and validation remain
unchanged, including numeric conversion, UUID text, optional timestamps, command
serialization, unknown-field handling, limits and the one-event rule. `DeviceUplink`
in the public protocol crate remains the JSON convenience type.

## CBOR and MessagePack V1

Both use native text-keyed maps with these exact uplink fields:

| Field | Type | Rule |
| --- | --- | --- |
| schema_version | integer | Required, exactly 1 |
| source_message_id | text | Required, 1–64 namespace-safe ASCII bytes |
| occurred_at | integer or null | Optional, nonnegative Unix milliseconds fitting i64 |
| kind | text | telemetry / event / heartbeat / command_ack |
| data | map | Kind-specific fields below |

CBOR uses the RFC 8949 definite-length profile with text keys, integers, finite
half/single/double floats, booleans and optional nulls. Indefinite lengths, tags,
byte strings, arrays, undefined and other simple values are rejected. MessagePack
uses named maps, UTF-8 strings, integers, finite float32/64, booleans and optional
nil. Arrays, bin, ext and reserved markers are rejected. Map order and integer
encoding width do not convey semantics. Duplicate keys at any supported level,
unknown envelope/data fields, malformed UTF-8 and trailing bytes are rejected.

| kind | data |
| --- | --- |
| telemetry | Nonempty map of named Scalar values |
| event | Required nonempty `name`; optional `value` Scalar (null means absent) |
| heartbeat | Required `sequence` unsigned integer, full u64 range |
| command_ack | Required UUID-text `command_id`; `execution` = running/succeeded/failed |

Scalar maps integer values in **[-2^53, 2^53]** exactly to Number(f64). Larger
integer representations are rejected even when a particular value is representable;
use a finite floating representation when floating semantics are intended. Finite
floats map to Number, booleans to Boolean, UTF-8 text to Text. Scalars cannot be
maps, arrays, bytes, null, nonfinite numbers, or control-character text. Names and
text are byte-bounded, and telemetry names cannot be empty. This stricter integer
policy applies to the new binary codecs; JSON's existing policy is preserved.

Downlinks are maps with `schema_version=1`, `command_id` (UUID text), `device`
(map containing tenant_id/product_id/device_id text), `expires_at` (Unix milliseconds
or null), and `payload` (map containing name and arguments). `arguments` is a
text-keyed Scalar map. CommandRouter owns TTL, permissions, live-session checks,
and count/byte queues. Encoders validate identity and field/encoded-byte bounds.
Socket SENT and device CommandAck execution remain distinct. UDP has no downlink.

## Protobuf V1

[device_v1.proto](../crates/netbaiot-codecs/proto/device_v1.proto) is authoritative.
It is compiled by pinned `prost-build` and `protoc-bin-vendored` during ordinary
Cargo builds. Users do not install protoc or manually generate Rust files. There
is no JSON blob field. Generated types are exposed in
`netbaiot_codecs::protobuf::v1::wire`; they do not add dependencies to the public
protocol crate. Other clients generate their own language bindings from the schema.

| Message | Stable field numbers |
| --- | --- |
| Uplink | schema_version 1; source_message_id 2; optional occurred_at 3; oneof telemetry 10 / event 11 / heartbeat 12 / command_ack 13 |
| Scalar | oneof double number 1 / boolean 2 / text 3 / sint64 signed_integer 4 / uint64 unsigned_integer 5 |
| Field | name 1; Scalar value 2 |
| Telemetry | repeated Field fields 1 |
| DeviceEvent | name 1; optional Scalar value 2 |
| Heartbeat | uint64 sequence 1 (absent means proto3 default zero) |
| CommandAck | UUID-text command_id 1; Execution execution 2 |
| DeviceKey | tenant_id 1; product_id 2; device_id 3 |
| DeviceCommand | schema_version 1; UUID-text command_id 2; DeviceKey device 3; optional expires_at 4; name 5; repeated Field arguments 6 |

Execution values are UNKNOWN=0 (rejected), RUNNING=1, SUCCEEDED=2, FAILED=3.
An Uplink must contain schema_version=1, a valid source_message_id and exactly one
known kind. Scalar must contain one known value. Telemetry entries require name and
value, and duplicate names fail. Semantic validation matches the other codecs;
integer Scalars use the same exact conversion interval. Optional occurred_at must
be nonnegative i64. Nonfinite double values and unknown enum values fail.

Unknown Protobuf fields of wire types 0/1/2/5 are bounded and skipped, without
allocating a buffer for unknown content. Unknown fields are not retained in the
business event. Unknown-only kinds are rejected. Duplicate **known** singular or
oneof fields fail, rather than applying last-one-wins or merge behavior; repeated
Field entries remain legal within limits. Groups and malformed lengths/varints are
outside this bounded proto3 profile. These rules are tested independently of the
stricter CBOR/MessagePack unknown-key policy.

Codec/wire versions are distinct from crate SemVer. Never reuse field numbers or
enum values; reserve retired numbers/names. Additive optional Protobuf fields may
be ignored by V1 within its budget. A semantic or required-field change needs a
new registered codec version. Register both versions for staged device migration;
never try another decoder after failure.

## Resource bounds and extension

New decoders check input_bytes, decoded_bytes and output_messages before parsing.
An allocation-free preflight checks all lengths against remaining input before
any library allocation; it bounds members/nesting, checks UTF-8, rejects unsupported
container types and charges a conservative structural budget (128 bytes per node,
3 times text bytes; Protobuf also charges unknown length-delimited bytes).
Container depth is at most min(nesting_depth,64); Protobuf counts nested schema
messages. Data field counts and field_bytes are validated as well. Limits are
cumulative: not every individual maximum can be reached at once. decoded_bytes is
a conservative variable-structure budget, not a process RSS or stack measurement.
The server retains its existing transport-derived byte ceilings; standalone codec
defaults are 64 KiB, 64 fields, 256-byte text, depth 8, one event. Transport limits
(in particular UDP's 1200-byte envelope) still apply before codec decoding.

Command output writers grow from small buffers and stop at decoded_bytes. Protobuf
checks owned-structure budget before cloning, and encoded_len before allocating
output. No parser creates async work, tasks, queues, filesystem or network requests.
QoS2 prevalidation and decode share the same parsing function and semantic rules.
EventAccepted, required-sink ACK, NBSP v3 and NBMQ v6 are unchanged. Abrupt failure
can still lose bounded, unspooled memory traffic.

Implement `DeviceCodec` independently under a named/versioned module (vendor
implementations can use `src/vendor`). Reuse common validators and add a bounded
preflight before calling a generic binary decoder. Override validate_payload with
the same parser as decode, so prevalidation creates no event_id. Emit exactly one
event with identity from DecodeContext, preserve source_message_id, and implement
encode for that profile. Add one explicit ID/version entry in builtins; the
registry allows up to 64 unique positive-version pairs, including several versions
of one ID. Add independent wire vectors, malformed/limit tests, fuzz and integration
coverage. Transport adapters own new industrial framing (e.g. Modbus), not codecs.

## Runnable examples and validation

```bash
cargo run -p netbaiot-codecs --example multi_codec
cargo test -p netbaiot-codecs
cargo test -p netbaiot-transports --test multi_codec
cargo build -p netbaiot-server
python3 tests/multi_codec_interop.py  # mosquitto_pub is a test dependency only
cargo bench -p netbaiot-codecs --bench codecs
cargo +nightly fuzz run cbor_codec -- -max_total_time=30 -max_len=65536
cargo +nightly fuzz run msgpack_codec -- -max_total_time=30 -max_len=65536
cargo +nightly fuzz run protobuf_codec -- -max_total_time=30 -max_len=65536
```

Fixed vectors under codecs/tests/fixtures are generated by an independent Python
specification encoder, not by these Rust codecs. `generate.py` reproduces them.
The example shows the same temperature/humidity data normalized across all four
formats. The Mosquitto tool sends native files through MQTT 3.1.1/5.0 QoS0/1/2 and
checks a real confirmed webhook. Generic MQTT clients remain first-class; the
optional SDK's JSON convenience API does not automatically encode binary formats.
See the [implementation and measurement report](multi-codec-report.zh-CN.md).
