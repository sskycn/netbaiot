# Device protocol removal evidence

Baseline: `945fe5e386d623c32e2c7d2d0568fe0c058107ec`.
Implementation and binary hashes: `after-build.json`.
[Analysis and migration](../../remove-device-http.md),
[aggregated results](../../remove-device-http-results.json).

`before-plan.json` and `after-plan.json` contain five modes: plaintext MQTT/TCP,
signed UDP NBI1/NBA1, MQTTS and TLS TCP. Each has three 30-second trials, preceded
by five seconds of warmup and a one-second client ramp. Trials run serially, all
before followed by all after, without concurrent compilation, tests or benchmarks.
They are local comparison windows, not current production capacity certification.

The compact aggregate result records per-run rates, receipt percentiles, error
counters, cleanup checks, and the hashes of each raw run. The full runs include
one-second CPU/RSS/task/queue samples, kernel counters, cooldown and child shutdown;
they were removed from Git and are indexed in the [archive manifest](../archive-manifest.json).
The pre-cleanup local copy retains the original records. `before-environment.json`
and `after-environment.json` record the host/toolchain. The same baseline generator
executable is used on both sides; the current source independently removes its
obsolete Device HTTP modes.

**Worker metadata correction:** the frozen generator explicitly uses four Tokio
workers. The before records inherited the old harness's incorrect value of two,
which only represented `TOKIO_WORKER_THREADS` requested in the environment.
It never overrode the macro. Original files were not modified before archiving; see
`metadata-corrections.json`. After records correctly report four. The old mixed
worker diagnostic is invalidated in the historical report. The runtime worker
count did not change between these measurements.

Reproduction (build the indicated revisions first; use a fresh output directory and
remove or rename only this task's completed `target/mixed-audit/{before,after}-remove-http-*`
run folders if intentionally repeating):

```sh
python3 scripts/perf/device_protocol_benchmark.py --server target/remove-device-http/before-server --loadgen target/remove-device-http/loadgen --plan docs/performance/remove-device-http/before-plan.json --label before --output target/performance/remove-device-http
python3 scripts/perf/device_protocol_benchmark.py --server target/remove-device-http/after-server --loadgen target/remove-device-http/loadgen --plan docs/performance/remove-device-http/after-plan.json --label after --output target/performance/remove-device-http
python3 scripts/perf/device_protocol_summary.py
```

`--resume` skips only completed matching plan/binary records. The summary checks
all 30 receipts/hashes/windows and cooldown counters, equal idle/cooldown task/FD
counts, zero device connections, and successful graceful child exits. The only
post-exit spool file permitted by these healthy trials is the MQTT recovery image.
Management connections are excluded from the after device status summary; the
before server reports the observing management connection in its legacy `http` key.

`validation/index.json` preserves the command, exit status, and log hashes for
required Rust checks, full tests, MQTT release gate, real tutorial, restart soak,
and fuzz runs. Full logs are indexed in the archive manifest and retained in the
pre-cleanup local copy. `config-preflight.json` has the five
TLS/config validation cases. `before-sdk-dependencies.txt`,
`after-sdk-dependencies.txt`, and build metadata cover native normal dependencies
and release artifact sizes; the dependency counts exclude the SDK root package.
