# Vendor payload codecs

[中文](vendor-codecs.zh-CN.md)

This page describes `main`. Its integer Decimal contract and diagnostic updates
are **not included in public v0.3.3**. See the [migration steps](migration/integer-decimal.md)
before upgrading an existing vendor configuration.

Vendor codecs are synchronous payload transformations behind the existing
`DeviceCodec` / `CodecRegistry`. They do not implement transport framing,
authentication, acknowledgement, event admission, persistence, or command queuing.
An authenticated device/product selects an explicit `codec_id` and
`codec_version`. There is no probing, automatic selection, or fallback after error.
The four built-in profiles and their wire representations are unchanged.

## Native Rust registration

Implement `DeviceCodec` in an independent versioned module. Decode exactly one
`DeviceEvent`, use identity from `DecodeContext`, preserve the source message ID,
and encode the full caller-supplied `command_id`. Use one complete parser for
`decode` and `validate_payload`; the latter must not generate an event ID. Both
must reject the same malformed input. Bound input, output, fields and allocations
before accessing or allocating data. Native implementations remain responsible
for these contracts; registration does not sandbox arbitrary Rust code.

`vendor::demo::AcmeV1` is an independent, bounded native reference for the example
telemetry and `set_temperature` command. It is opt-in, not a built-in profile.
The declarative example additionally demonstrates heartbeat, event and command ACK.

```rust,ignore
use netbaiot_codecs::vendor::{demo::AcmeV1, registration};
use netbaiot_core::CodecLimits;
use std::sync::Arc;

config.native_codecs.push(registration(
    "vendor-acme", 1,
    Arc::new(AcmeV1::new(CodecLimits::default())?),
)?);
config.validate()?;
// netbaiot_server::run(config, cancellation).await?;
```

`Config.native_codecs` is a Rust composition hook, excluded from JSON and Schema;
JSON cannot request loading native code. Configure it before validation/startup.
A custom embedded host must register it again when loading/round-tripping Config.
A standalone `netbaiot config check` only knows built-ins and JSON declarations.
For custom runtime composition, the same registration tuple can be appended to
`netbaiot_codecs::builtins(limits)` and passed to `CodecRegistry::new`.

Vendor IDs use `vendor-<name>` (the existing namespace-safe ID limit is 64 bytes).
Versions must be positive. Different versions of one ID may coexist. Duplicate
ID/version pairs, native/declarative collisions and built-in replacement fail at
startup. The registry holds at most 64 entries: four built-ins and up to 60 custom
profiles. Select the new ID/version in trusted authentication and product control
profiles. Plan a drain/restart for a product profile change; do not mutate a codec
behind existing authenticated sessions.

Run the native composition example:

```bash
cargo run -p netbaiot-server --example native_vendor
```

## JSON declarations

[`configs/codecs/acme-v1.json`](../configs/codecs/acme-v1.json) is a complete standalone
plan, and [`configs/vendor-codec.json`](../configs/vendor-codec.json) embeds it in the
server's `payload_codecs` array with development credentials. This is a fictional
wire protocol, not compatibility with a commercial device.

The server checks and compiles declarations before opening transport listeners.
Checked byte ranges and sorted message/command tables are shared immutably. The
hot path reads payload slices directly; it never parses configuration, clones the
payload, performs I/O, creates tasks, or guesses a codec. Only normalized owned
fields/identifiers and the bounded command output are allocated. Existing built-in
codec implementations and registry lookup behavior are unchanged.

| Configuration | Meaning |
| --- | --- |
| `id`, `version` | Explicit authentication-bound vendor profile |
| `header` | 1–64 literal bytes at offset zero |
| `message_type` | Unsigned integer field selecting an uplink `tag` |
| `length` | Optional unsigned field; wire value + signed `adjustment` equals total payload size |
| `messages` | Unique tags, exact `size`, required `source_message_id`, optional `occurred_at`, payload mapping, optional checksum |
| `commands` | Unique command names/tags, exact size, 16-byte `command_id_offset`, optional `expires_at`, named argument fields, optional checksum |

All offsets and checksum ranges are relative to the **payload**, excluding MQTT,
TCP and UDP envelopes. Each message has a fixed exact size. Fixed-size variants
can have different sizes/tags. Unmapped bytes are reserved and must be zero.
Trailing bytes, split/truncated payloads, unknown tags, overlapping fields, and
invalid UTF-8 fail. Transport adapters still assemble MQTT/TCP payloads and enforce
their original packet/frame/deadline limits. This profile does not implement
variable-length arrays, bit fields, nested structures, industrial framing, WASM,
dynamic libraries, or scripting; implement a bounded native codec for other layouts.

`IntegerField` has `offset`, `type` (`u8/u16/u32/u64`) and optional `endian`.
Value fields additionally support `i8/i16/i32/i64`, `f32/f64`, `bool` and `utf8`.
Big endian is the default. UTF-8 requires an exact positive byte `length` without
padding/terminators; other types must omit `length`. Text cannot contain control
characters. Boolean bytes are strictly 0 or 1. IEEE floats must be finite.

Numeric normalization is `wire * scale + bias`, but integer and IEEE floating-point
wire types have separate, intentionally incompatible configuration contracts.

Every integer value field (telemetry, event value or command argument) uses the exact decimal engine. Omitting `conversion` means the
exact identity mapping (`scale = 1`, `bias = 0`); decoded values are still
`Scalar::Decimal`, including full-width i64/u64 values. A non-identity integer mapping
must use the explicit string form:

```json
{
  "offset": 9,
  "type": "i16",
  "conversion": {
    "mode": "decimal_exact",
    "scale": "0.001",
    "bias": "1.0"
  }
}
```

`conversion` is supported only by `i8/i16/i32/i64/u8/u16/u32/u64`. Its scale and
bias are strings, the scale must be nonzero, each parameter is at most 64 bytes and
38 significant digits, and the normalized configuration exponent must be between
-38 and 38. Startup compilation aligns scale and bias to one checked i128 affine
coefficient. It separately proves that multiplication and addition are representable
over the complete raw integer interval and that every output fits both the 64-byte public
Decimal limit and `CodecLimits.field_bytes`; it does not assume endpoint normalization proves interior values.
Configurations that cannot satisfy that proof are rejected before ingress starts.

The old numeric `scale` or `bias` members are rejected on every integer field, even
when explicitly `null` or set to `1` or `0`, with a diagnostic pointing to the obsolete member. Migrate
them to `conversion: {"mode":"decimal_exact","scale":"...","bias":"..."}`.
This is an intentional breaking configuration change. Those numeric members now
belong only to `f32/f64` fields. Explicit `null` on a float still means omission
(default scale 1, bias 0).

Integer uplinks always return `Scalar::Decimal`, serialized as
`{"decimal":"1.001"}`. Integer downlinks accept that form, and also accept an ordinary
`Scalar::Number` after converting its finite f64 value to the shortest decimal text
that round-trips to the same f64. This convenience cannot recover precision lost
before the value reached the codec. Ordinary text is never treated as a number.
The exact engine accepts at most 39 significant digits and a normalized scalar
exponent magnitude of 324; plan outputs are additionally limited to canonical,
non-exponent decimal text of at most 64 bytes. Unsupported precision, exponent,
intermediate arithmetic, nonintegral inverse results, and target range overflow are
hard errors—there is no rounding, epsilon, ULP, or f64 fallback.

JSON, CBOR and MessagePack preserve the decimal object rather than converting it to a
JSON number. Protobuf v1 uses Scalar oneof field 6 (`string decimal`) and retains all
existing field numbers.

`f32/f64` fields keep IEEE 754 semantics. NaN, infinity, inverse overflow and
nonzero-to-zero underflow are rejected. f64 stores the finite inverse result. f32
narrowing is accepted only when converting the candidate back to f64 exactly
reproduces that inverse value. The forward multiply/add may incur only the documented
four-f64-epsilon relative IEEE operation error and may not collapse a nonzero signal
to the bias. These floating rules are not used to quantize integer fields. Text and
bool cannot be scaled. Full u64 remains available for source IDs, heartbeat sequences
and integer metadata. Arguments must match configured names, count and types exactly;
there are no silent defaults or extra ignored arguments.

The source ID is an unscaled unsigned integer rendered as decimal, or an exact
namespace-safe UTF-8 identifier of at most 64 bytes. It is independent metadata:
unsigned IDs support the full `u64` range without the business Scalar Decimal
output-length proof or `CodecLimits.field_bytes` scalar-text ceiling. Source fields
cannot use a non-null `conversion` or legacy numeric conversion. UTF-8 IDs must
use the public identifier characters (ASCII letters, digits, `-`, `_`, `.`, `:`);
empty IDs and other characters are rejected. No payload field changes the
authenticated device identity. Vendors must provide source IDs unique within the
device's message namespace (including across message types). The gateway generates
a fresh event ID when decoding; EventBus retry/spool replay preserves the accepted
event ID as before. `occurred_at` and `expires_at` are unsigned Unix milliseconds,
bounded to the public timestamp range. A configured `expires_at` must use `u64`
to represent the full nonnegative i64 range (0 through 2^63−1); smaller types fail
compilation. Expiry omitted from a command is encoded
as zero when that field is configured. Gateway command TTL/admission still applies
whether or not expiry appears on the wire.

Payload mappings:

- `telemetry`: nonempty unique named scalar fields.
- `heartbeat`: an unscaled unsigned `sequence`.
- `event`: a fixed nonempty event `name` and optional scalar `value`.
- `command_ack`: full 16-byte RFC 4122 UUID plus three distinct configured integer
  values for `running`, `succeeded`, `failed`; all other values fail. This reports
  device execution, distinct from transport SENT and EventAccepted.

`sum8` (sum modulo 256), `xor8`, and `crc16_modbus` are supported. CRC-16/MODBUS
uses init FFFF, reflected polynomial A001 and xorout 0000; `123456789` gives 4B37.
Checksum byte order is explicit (default big endian; specify little for conventional
Modbus CRC storage). `start` is inclusive, `end` exclusive; the checksum bytes must
be outside this nonempty covered range. Coverage is exactly the configured range.
Checksums detect corruption; authentication/TLS/HMAC protections remain unchanged.

## Bounds and validation

Each profile has at most 64 total message/command layouts, each at most 64 KiB;
each telemetry/command layout has at most 64 fields and each name/text at most
256 bytes, further restricted by `CodecLimits`. Every offset/length addition is
checked. Plans charge a conservative budget of 512 bytes per layout plus 128 per
value field and three times its wire/name bytes against `decoded_bytes`; cumulative
retained plans also charge range metadata and are capped at 256 KiB per profile.
This is a structural admission budget, not a claim about process RSS.

`DeclarativeCodec::from_json` limits standalone configuration to 256 KiB. The server
uses its existing 16 MiB outer config-file limit, at most 60 custom profiles, and
the same per-profile compilation bounds. The generated configuration Schema
validates structure and simple limits; `netbaiot config check` additionally checks
layout overlaps, metadata ranges, uniqueness, budgets and cross-field rules.
Invalid plans produce configuration diagnostics before network service starts,
including schema field paths such as `payload_codecs[0].commands[0].expires_at.type`.
Diagnostic paths and messages omit configuration values and unknown user keys.

Legacy integer `scale/bias` is rejected during deserialization. Legal numeric or
null legacy values produce `NBI-CFG-016` with the full member path and advice to
remove the old members, use string `decimal_exact` parameters, or omit identity
conversion. Invalid types, unknown fields and malformed JSON retain their original
error categories. Numeric proofs, layouts and resource budgets are checked during
compilation (`NBI-CFG-015`); Schema alone cannot prove these cross-field rules.
Human and `--output json` reports share the same `code/path/message/help` fields.

Command layouts must fit `max_command_bytes` at startup. All codecs, including
native implementations, also undergo encoded-size admission before queue ownership.
The active session further bounds commands by TCP frame/write capacity or MQTT
packet/write capacity, including topic, framing and MQTT 5 expiry metadata and the
client's Maximum Packet Size. MQTT reserves a packet identifier even at QoS0 (two
bytes), so a later resubscription to QoS1/2 cannot make an already queued command
unsendable. Rejected commands do not reserve queue capacity or disconnect devices.

QoS2 prevalidation and decode call the exact same parser, including source IDs,
checksums, reserved bytes, numeric bounds and execution states. Successful
prevalidation is not EventAccepted. Required sinks still reserve atomically;
QoS1 PUBACK, TCP receipt and signed UDP NBA1 retain their existing meaning.

## Tests and measurement

```bash
cargo test -p netbaiot-codecs --all-features
cargo test -p netbaiot-transports --test multi_codec
cargo test -p netbaiot-server --lib vendor
cargo bench -p netbaiot-codecs --bench vendor
cargo +nightly fuzz run vendor_codec -- -max_total_time=30 -max_len=65537
cargo +nightly fuzz run vendor_config -- -max_total_time=30 -max_len=262145
```

The benchmark reports native/declarative decode, QoS2 prevalidation, encode,
rejection and startup compilation separately, including allocator calls/bytes.
Compilation is outside all hot-path samples. These are local microbenchmarks,
not gateway throughput, tail latency, capacity or a production performance promise.
