# Parser fuzzing

Requires nightly Rust and cargo-fuzz. Targets are independent of listeners/storage.
Every harness caps its own input, and incremental harnesses cap buffered bytes.

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
