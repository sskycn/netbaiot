# Parser fuzzing

Requires nightly Rust and cargo-fuzz. Targets are independent of listeners/storage.
Every harness caps its own input, and incremental harnesses cap buffered bytes.

Business RPC uses the version-independent `business_rpc_bootstrap` and
`business_rpc_wire` target names. They exercise the current V4 Hello/Ready and
binary stream protocol; the wire version is 4. `seed_corpus.py` writes their
seeds under those target names.

```sh
cargo +nightly fuzz run business_rpc_bootstrap -- -runs=10000 -max_len=4097
cargo +nightly fuzz run business_rpc_wire -- -runs=10000 -max_len=65536
```

```sh
cargo +nightly fuzz run mqtt_fixed_header -- -max_total_time=30 -max_len=65540
cargo +nightly fuzz run mqtt_remaining_length -- -max_total_time=30 -max_len=8
cargo +nightly fuzz run mqtt_packet -- -max_total_time=30 -max_len=65540
cargo +nightly fuzz run tcp_frame -- -max_total_time=30 -max_len=65540
cargo +nightly fuzz run udp_envelope -- -max_total_time=30 -max_len=1201
cargo +nightly fuzz run json_codec -- -max_total_time=30 -max_len=65537
```

Use `CARGO_NET_OFFLINE=true` after dependencies have been cached. Keep crashing
inputs as regression cases. Smoke runs are not a security audit or a proof of
memory bounds. See docs/validation.md for the runs actually performed.

## Audit campaign

Run `python3 fuzz/seed_corpus.py` from the repository root before the six targets.
The MQTT target asserts forward progress/NeedMore behavior and output bounds;
TCP asserts consumption and frame bounds; JSON asserts a single trusted-device output.
The 2026-09-19 audit used 500,000 MQTT packet iterations and 100,000 for each other
target with ASan. See `docs/correctness-resource-reliability-audit.md` for results.

The `device_classifier` target exercises every partial prefix (at most 12 bytes),
malformed/ambiguous inputs and frame-limit boundaries without network or allocation.

## Native binary device codecs

Seed cbor_codec, msgpack_codec and protobuf_codec with the corresponding fixed
files under crates/netbaiot-codecs/tests/fixtures. Each checks arbitrary bytes with
a 64 KiB input cap, single trusted-device output, telemetry count bounds and
prevalidation/decode agreement.

Run `python3 fuzz/seed_corpus.py` before fuzzing. Independent vectors include all
legal kinds, 2/16/64-field boundaries, integer/nonfinite/enum errors, truncated and
trailing inputs, overflow lengths, duplicate keys and excessive nesting. The shared
harness checks complete prevalidation/decode semantics, trusted identity/timestamp,
one-event output, text/count limits, finite Scalars, legal execution states and
fresh event IDs across separate decodes, with default and tight resource budgets.
Structural preflight alone is not a semantic acceptance oracle. Binary integer
range boundaries are also covered by independent numeric fixtures and unit tests.

`.github/workflows/codec-audit.yml` executes all three binary targets in separate
ASan jobs with 30-second engine budgets and a 20-minute job deadline, retaining
reproduction artifacts for 14 days. Original JSON/MQTT/TCP/UDP targets are preserved.

```sh
cargo +nightly fuzz run cbor_codec -- -max_total_time=30 -max_len=65536
cargo +nightly fuzz run msgpack_codec -- -max_total_time=30 -max_len=65536
cargo +nightly fuzz run protobuf_codec -- -max_total_time=30 -max_len=65536
```
