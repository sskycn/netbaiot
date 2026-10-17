# Vendor integer conversion migration (v0.3.4+)

[中文](integer-decimal.zh-CN.md)

This migration applies when upgrading from `v0.3.3` or earlier to `v0.3.4` or
later, including `v0.4.0`. It does not change device transport framing. See
[version status](../README.md#versions-and-upgrades); `v0.4.0` also requires the
[Business RPC V4 client upgrade](current-protocol-only.md).

Integer **value** fields in telemetry, event values and command arguments now
always use exact Decimal arithmetic. Remove the legacy numeric `scale` and `bias`
members, including explicit `null`, `1` and `0`. For example:

```json
{"offset":9,"type":"i16","conversion":{"mode":"decimal_exact","scale":"0.1","bias":"0"}}
```

Omit `conversion` for exact identity. Do not mechanically stringify an old float
without checking the intended decimal value. Startup proves the entire integer
range fits checked arithmetic, the public Decimal 64-byte ceiling and
`CodecLimits.field_bytes`. It rejects configurations that cannot satisfy all three.
`f32/f64` retain IEEE 754 behavior and numeric `scale/bias`; explicit null still
means omission. Do not add `decimal_exact` to floats, text or bool.

`source_message_id` is independent unscaled metadata. Remove legacy integer
members there too, but do not add a non-null `conversion`. Unsigned IDs preserve
full `u64`; fixed UTF-8 IDs obey the public 1–64-byte ASCII identifier character
rules. They do not use business Scalar Decimal output-length bounds.

Integer value decoding returns `Scalar::Decimal`, including identity mappings.
Command arguments should use `{"decimal":"1.001"}` to preserve precision.
Finite `Scalar::Number` convenience input uses its shortest round-tripping decimal;
it cannot recover precision already lost to f64. Text is not numeric input.
JSON, CBOR and MessagePack use the explicit Decimal object; Protobuf uses existing
Scalar oneof field 6 (`string decimal`). This diagnostic fix changes no field numbers
or wire encoding. When moving from public `v0.3.3`, Rust consumers must account for
the new `Scalar::Decimal` variant in exhaustive matches.

Check with the target version's CLI; from a matching source checkout:

```bash
cargo run --locked -p netbaiot-cli -- config check --config configs/vendor-codec.json
cargo run --locked -p netbaiot-cli -- --output json config check --config configs/vendor-codec.json
```

A recognized legacy integer setting returns `NBI-CFG-016`, the complete schema
member path and migration help in both formats. Malformed JSON, illegal field
types/float values and unknown fields keep their existing diagnostic categories.
Compilation errors use `NBI-CFG-015`. Config checks omit input values and unknown
keys. They do not start listeners, contact providers or mutate recovery state.
Plan a normal drain/restart for a changed codec profile; accepted EventBus replay
keeps its `event_id`, and business consumers remain responsible for idempotency.
