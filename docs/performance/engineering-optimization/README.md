# 2026-10-08 engineering measurements

See the [complete report](../../engineering-optimization.md) for scope, results,
tradeoffs and tests not run. JSON files are small curated summaries, not raw
logs, binaries, certificates or large benchmark artifacts.

- `eventbus.json`: identical before/after workload at six backlog depths, three
  repeats. `before` uses `0454626` plus the unchanged measurement harness;
  `after-counter` isolates the incremental required counter and `after-final`
  also includes tenant quota, operation fencing and diagnostics.
- `webhook.json`: old JSON Value tree versus borrowed envelope with the same
  reqwest JSON request construction and payloads, three repeats; no network.
- `mqtt.json`: all 20 offline/outbound combinations, five operations, three
  repeats; measured-action allocations, timing and broker lock hold sums.
  Fixture-only capacity and clock-resolution limitations are recorded in JSON.
- `soak.json`: real TLS MQTT QoS1 to confirmed HTTP at 10 events/second, an HTTP
  outage between the first and second thirds of 600 publishes, final accounting
  and task cleanup; also the existing 12-cycle restart soak. RSS is the fixed
  test/subprocess suite observation, excluding compilation.
- `validation.json`: verified source commit and PASS links for the three native
  package jobs and six complete release CI jobs.

Run release measurements serially and avoid concurrent builds or other loads:

```sh
cargo test -p netbaiot-runtime --release engineering_eventbus_accounting_benchmark -- --ignored --nocapture --test-threads=1
cargo test -p netbaiot-server --release engineering_webhook_serialization_benchmark -- --ignored --nocapture --test-threads=1
cargo test -p netbaiot-transports --release engineering_mqtt_session_matrix -- --ignored --nocapture --test-threads=1
cargo test -p netbaiot-server --test server sixty_second -- --ignored --nocapture --test-threads=1
```

Performance figures are subsystem evidence on a development macOS arm64 host
with Rust 1.99.0, not gateway production capacity or an SLA. The soak is a bounded
reliability test on a shared host. It does not replace multi-day tests, real
network failure injection, or dedicated per-connection memory profiling.
