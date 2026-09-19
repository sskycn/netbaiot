# Historical foundation validation

The results below predate the [focused audit](correctness-resource-reliability-audit.md).
Current regression, PostgreSQL crash, load, fuzz and benchmark evidence is linked there.

# Validation evidence — 2026-09-18

Environment: macOS arm64; stable rustc 1.97.1 / cargo 1.97.1; local PostgreSQL
17.11; nightly Rust plus cargo-fuzz 0.13.2 for AddressSanitizer smoke runs.

## Required checks

| Command | Actual result |
|---|---|
| `cargo fmt --all -- --check` | Passed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Passed, no warnings |
| `cargo test --workspace --all-features` | 43 passed; one PostgreSQL test explicitly ignored by default and run separately |
| `cargo test -p netbaiot-storage --test semantics postgres_transaction_and_command_contract -- --ignored` | Passed on a fresh disposable PostgreSQL 17.11 database |
| `cargo build -p netbaiot-server --offline` | Passed |
| `DATABASE_URL=... python3 tests/smoke_postgres.py` | Passed with actual server executable, PostgreSQL, HTTP sink, and MQTT client |
| `cargo run -p netbaiot-server --offline -- --print-default-limits` | Passed; generated configs/resource-limits.json |
| `cargo bench -p netbaiot-transports --bench foundation --offline` | All seven benchmarks completed |
| `git diff --check` | Passed |

No existing test was removed. The workspace tests exercise 15 real-socket
transport scenarios plus four server/config/TLS tests. The remaining tests cover
codecs, identities, quotas, session races, bounded queues, replay, framing, packet
IDs, storage semantics, retention, leases and attempts. The PostgreSQL contract
also injects an outbox insertion failure and checks atomic rollback of the message
and execution ACK, then races two identical admissions.

The executable smoke test verifies durable HTTP/MQTT receipts, stable duplicate
receipts, three unique outbox deliveries with idempotency headers, separately
authorized command creation, active MQTT downlink, transport PUBACK, application
execution ACK and SIGTERM shutdown. It owns and stops its process, HTTP sink thread
and sockets. Its temporary configuration is removed.

Initial socket tests failed because this environment's sandbox prohibited local
listeners; rerunning with loopback permission passed. The local PostgreSQL binaries
needed ICU libraries under `/opt/local/lib/icu/lib`; a temporary wrapper supplied
the loader path to subprocesses without modifying the installed binaries. The
throwaway cluster used loopback port 55432. Initial dependency downloads needed the
host's unavailable proxy bypassed; the resolved lockfiles now support offline builds
when cached. These environment failures were not reported as passing tests.

## Fuzz smoke runs

All six targets built using `cargo +nightly fuzz build --sanitizer address` with
`CARGO_NET_OFFLINE=true`. Each command below completed 5,000 runs with no crash or
AddressSanitizer finding:

```sh
CARGO_NET_OFFLINE=true cargo +nightly fuzz run mqtt_fixed_header -- -runs=5000 -max_len=65540
CARGO_NET_OFFLINE=true cargo +nightly fuzz run mqtt_remaining_length -- -runs=5000 -max_len=8
CARGO_NET_OFFLINE=true cargo +nightly fuzz run mqtt_packet -- -runs=5000 -max_len=65540
CARGO_NET_OFFLINE=true cargo +nightly fuzz run tcp_frame -- -runs=5000 -max_len=65540
CARGO_NET_OFFLINE=true cargo +nightly fuzz run udp_envelope -- -runs=5000 -max_len=1201
CARGO_NET_OFFLINE=true cargo +nightly fuzz run json_codec -- -runs=5000 -max_len=65537
```

The final fuzz processes reported RSS between 42 and 49 MiB. This is fuzzer-process
RSS, not service RSS under load. These short runs are regression smoke tests;
mutation inputs did not exhaust every configured boundary or protocol state.
Boundary, fragmentation, malformed lengths and arbitrary-byte smoke tests also run
in the normal suite. Long fuzz campaigns and independent MQTT-client interoperability
matrices remain unperformed.

## Microbenchmarks

Final recorded release run, 20,000 measured iterations per operation after 1,000
warmups. Payload is a small two-field telemetry message. Results include the test
harness's clock/sample overhead. The source is benches/foundation.rs.

| Operation | operations/s | P50 ns | P95 ns | P99 ns |
|---|---:|---:|---:|---:|
| MQTT decode | 6,311,637 | 125 | 167 | 292 |
| MQTT encode | 9,222,255 | 83 | 125 | 125 |
| Exact topic ACL | 13,053,416 | 42 | 84 | 84 |
| Subscription lookup | 23,157,073 | 41 | 42 | 42 |
| JSON codec | 678,287 | 1292 | 2250 | 2458 |
| TCP frame decode | 13,074,032 | 42 | 84 | 84 |
| Ingress admission | 3,287,198 | 291 | 333 | 334 |

These are a local baseline, not a before/after optimization claim, network throughput,
database throughput, or supported device capacity. Clock granularity and host
contention materially affect nanosecond measurements. No service-load CPU/RSS,
allocation profile, sustained connection flood, or end-to-end P99 study was run.
The storage admission lock and single delivery worker are deliberate first-milestone
scaling constraints.

## Reproduction

The root CI workflow runs fmt, clippy, all workspace tests and the separate
PostgreSQL contract using a disposable PostgreSQL 17 service. That workflow was
written but was not executed on GitHub during this task. See README.md and
fuzz/README.md for local commands. The PostgreSQL contract and executable smoke
script each require their own fresh database to avoid retained-message interference.
