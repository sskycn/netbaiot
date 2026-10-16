# Event-gateway load tools

The Rust `netbaiot-loadgen` is the supported load generator. It targets the
database-free device listeners and reports accepted-event and transport latency.
Do not use the historical PostgreSQL-era JSON results in `docs/performance` as
evidence for the current architecture.

Published evidence uses `<repo>`, `<tmp>`, and `<home>` path placeholders. These
are archival inputs, not directly runnable configurations: substitute your own
paths before replaying a run. The V3 command-readiness and HOL harnesses scrub
their completed output (including failure logs) after child processes stop, using
dynamically discovered roots. Live configs retain real paths while a run is active.
For other drivers, sanitize evidence before copying it into documentation:

```bash
python3 scripts/scrub_paths.py target/perf-audit --write
python3 scripts/scrub_paths.py docs/performance
```

Release preflight rejects unsanitized performance paths. Scrubbing is not secret
redaction: only publish runs made with public test fixtures and inspect the
output for secrets separately. Do not publish a still-running harness directory.

Build release binaries:

```bash
cargo build --release -p netbaiot-server -p netbaiot-loadgen
```

Start `netbaiot-server` with a confirmed local webhook or TCP/RPC sink, then run:

```bash
target/release/netbaiot-loadgen --help
```

Measure at minimum event rate/bytes, device-to-EventAccepted and sink-ACK
P50/P95/P99, queue count/bytes, RSS, CPU, tasks, FDs, and rejection/drop counts.
A sustainable rate requires stable queues and RSS after warmup. Connection memory
experiments must distinguish plaintext, TLS, generic TCP, application logical
bytes, Tokio/TLS/allocator overhead, and kernel buffers.

Reproducible MQTT probes:

```bash
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000 --tls
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000 --persistent
python3 scripts/perf/connection_memory.py --transport mqtt --connections 1000 --persistent --subscribe
python3 scripts/perf/connection_memory.py --transport mqtt --connections 10000 --persistent --disconnected
python3 scripts/perf/connection_memory.py --transport tcp --connections 1000
python3 scripts/perf/connection_memory.py --transport tcp --connections 3000
python3 scripts/perf/event_load.py --rate 10000 --duration 20 --warmup 5 --connections 64 --sink-mode none --qos 1
python3 scripts/perf/event_load.py --rate 1000 --duration 15 --connections 32 --sink-mode webhook --sink-delay-ms 10
python3 scripts/perf/event_load.py --rate 10000 --duration 15 --warmup 5 --connections 64 --sink-mode none --qos 1 --tls
python3 scripts/perf/mixed_load.py --scenario route-fairness --duration 15 --warmup 5 --cooldown 2 --sample-every 1
python3 scripts/perf/mixed_load.py --duration 1800 --warmup 30 --cooldown 10 --sample-every 5
cargo bench --bench foundation
cargo test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
```

`--disconnected` measures RSS after every socket/task/FD has gone while persistent
MQTT and auth-cache state remains. Broker logical session bytes are printed by the
foundation benchmark; RSS additionally includes hash tables, allocator capacity,
credentials/auth cache, metrics, runtime, and process overhead.
The memory fixture raises the device protocol ceiling together with the global
connection ceiling. It requires the entire requested population to establish and
the load generator to finish without errors; partial admission emits diagnostic
JSON with `measurement_valid=false`, null per-population ratios, and a failed exit.
Active ratios use the observed active device count. Disconnected ratios are named
`rss_delta_bytes_per_established_persistent_session` and use the successfully
established unique-client population; there is no public broker gauge for the
retained-session collection length.

`mixed_load.py` uses 1,000 MQTT devices (70% idle, 20% at 1 msg/s, 9% at
10 msg/s, and 1% bursty), an approximately 60/30/10 QoS0/QoS1/QoS2 event mix,
persistent reconnects, low-frequency command/downlink traffic, and the confirmed
HTTP webhook. It writes one JSON result to stdout; redirect it under
`target/perf-audit/`. Run the server and load generator on separate hosts before
treating throughput as a production hardware ceiling.

The `route-fairness` scenario instead runs one saturated QoS1 publisher beside
100 publishers at 10 msg/s each, without an external business sink. Compare the
`low-rate-q1` PUBACK P99 and completion count between builds. Both event and mixed
drivers accept `--server-bin` and `--loadgen-bin`, which permits an exact-SHA
comparison binary from an isolated worktree without moving the current checkout.

`event_load.py --subscribe-down` makes every MQTT publisher subscribe only to
its own `/down` topic. It exercises the broker's no-matching-uplink route while
QoS1/2 completion is counted by PUBACK/PUBCOMP. It is mutually exclusive with
`--subscribe-uplink`, which additionally receives the published message. The new
loadgen `subscribe_down_only` option defaults to false; unlike the legacy
`subscribe` mode, it does not wait for an `up_ack` application echo. For QoS0,
use gateway metrics to establish EventAccepted; a successful client write alone
is not acceptance. Preserve all full-lifecycle and measurement-window counts.

Both drivers accept `--server-workers N` (1..64). It sets
`TOKIO_WORKER_THREADS` only for the server; sink and load-generator runtimes are
unchanged. Results record the requested count and the environment passed to the
server, not an independently observed worker count. Keep this value identical
between builds when measuring a code change. Compare worker configurations
separately and include CPU, tail latency and fairness.

The event and mixed drivers set `NETBAIOT_PERF_LOCK_METRICS=1` for the child
server. Broker/EventBus lock timing is disabled by default outside these drivers.

For UDP A/B capacity runs, `device_protocol_benchmark.py` accepts interleaved plan
entries with `variant: "baseline"` or `"candidate"` and a `--candidate` executable.
Freeze both Release executables and the load generator first. Increase `groups`
UDP `rate` progressively, then repeat the rates around the loss/latency boundary
and run a longer soak; keep worker counts, payloads, limits and client windows
identical. This driver leaves lock timing disabled unless `lock_metrics: true` is
set in the plan. Set `host_cpu_sample: false` to omit the intrusive midpoint `top`
sample; process CPU and RSS samples remain enabled. Record these choices.
On a macOS desktop, wrap the driver in `caffeinate -i` to prevent idle sleep for
that command's lifetime. Keep incomplete runs; do not count a sleep-interrupted
warmup as a capacity measurement.

On macOS, a separate diagnostic plan with `cpu_profile: "time"` records up to ten
seconds using Instruments Time Profiler; `cpu_profile: true` uses `sample`.
Do not mix profiled results into capacity statistics. `sample` includes parked
threads and is not a CPU percentage profile. Raw traces stay under ignored
`target/mixed-audit`; inspect exported metadata before publishing anything.
After exporting only the `time-profile` table with `xctrace export`, summarize it
with `python3 scripts/perf/macos_cpu_summary.py input.xml output.json`. The output
contains sampled running-stack weights and symbol names, not the trace's process
environment or local binary paths.

Summarize matching UDP records without mixing counter windows:

```bash
python3 scripts/perf/udp_capacity_summary.py target/performance/udp/raw target/performance/udp/summary.json --match '*capacity*.json'
```

CPU/event divides process CPU by gateway EventAccepted deltas from the same
sample interval. Receipt rate/P99 and acceptance percentage use the load generator
window. Preserve `udp_no_ack`, client window exhaustion, gateway rejects and ACK
send failures separately: their accounting intervals differ. Kernel socket drops
are host-wide deltas over warmup, measurement and cooldown, not per-process loss.
Summaries retain hashes, per-run plans, queue/RSS peaks and graceful-stop results.

The V3 HOL and command-readiness harnesses wait for the configured gateway
shutdown deadline plus five seconds. They retain the authoritative recovery
directory and copy committed files with permissions, byte counts and SHA256
metadata. A forced exit, missing MQTT snapshot or failed evidence copy fails the
durability check. The snapshot check supplements the gateway's successful exit;
it does not independently decode or verify the recovery format.
These harnesses scrub only their top-level text artifacts. Never recursively run
`scrub_paths.py --write` on a directory containing recovery files: they are opaque
checksummed data, including when their bytes happen to be valid UTF-8.

The ignored restart soak runs 12 healthy process generations with a five-second
dwell per generation after the initial forced-spool/recovery pair. Each generation
waits for the accepted `event_id` at the confirmed webhook, drains, exits, and
verifies that no committed EventBus spool segment remains.

## EventBus lifecycle experiment

The opt-in lock metrics additionally expose closed-vocabulary
`event_bus_probe_*` counters and `event_bus_state_{wait,hold}_us` histograms.
The older `event_bus_lock_*` names still cover successful publish only. Probe
wakes count select branches returning, not executor polls. `empty_wake` includes
completion-to-idle iterations and is not synonymous with a useless OS wake.

```bash
cargo build --release --locked
cargo run --release --locked -p netbaiot-runtime --example eventbus_probe
cargo run --release --locked -p netbaiot-runtime --example eventbus_probe -- --overload
cargo test --release --locked -p netbaiot-runtime eventbus_queue_depth_probe -- --ignored --nocapture
python3 scripts/perf/eventbus_campaign.py --server-bin /absolute/path/to/server --loadgen-bin /absolute/path/to/loadgen --output target/eventbus-experiment/before
python3 scripts/perf/eventbus_summary.py target/eventbus-experiment/before
```

Run comparison binaries sequentially on an otherwise idle host with identical
limits. The campaign performs three primary QoS1 20k repetitions, 25k/30k sweep,
QoS0/QoS2, publisher scaling, bounded offered overload, macOS `sample`, and fairness.
It requires local socket/process-sampling permissions and uses the existing
loopback-only drivers. The microbench uses fixed sample storage and a bounded
producer window; `--overload` blocks one sink, verifies independent fast completion,
forces explicit rejection, opens the sink, and verifies zero residual accounting.
Its process RSS is distinct from the real server's RSS. Results are diagnostics,
not production capacity certification.

## Four-codec network matrix

`codec_campaign.py` runs actual MQTT 3.1.1/5.0 QoS0/1/2, framed TCP and signed
NBI1/NBA1 UDP against one frozen gateway binary and a required confirmed webhook.
Every codec covers two-field telemetry, large telemetry (64 x 128-byte text on
streams, four fields on UDP to retain its 1200-byte limit), heartbeat, and online
MQTT/TCP commands. Independent wire encoders check exact command bytes. It uses
bounded Python clients and at most eight webhook handler threads, fixed inflight
work per client, hard receive limits and owned process shutdown.

```bash
python3 scripts/perf/codec_campaign.py --server /absolute/path/to/before-server --output target/performance/codec-before --revision BEFORE_SHA --seconds 1 --workers 4 --repeats 3
python3 scripts/perf/codec_campaign.py --server /absolute/path/to/after-server --output target/performance/codec-after --revision AFTER_SHA --seconds 1 --workers 4 --repeats 3
```

Freeze both release binaries before running, use the same harness and configuration,
and serialize runs without compilation or other tests. `summary.json` records each
case, protocol-receipt and confirmed-webhook P50/P95/P99, device-received command
latency, CPU, sampled RSS/growth, errors, management counters and post-case queues.
The rate denominator includes connection setup and bounded result polling; latency
starts after payload encoding immediately before socket send. QoS0 has no receipt;
QoS2 reports complete PUBCOMP exchange. Webhook timing ends when the test handler
flushes its 2xx response, rather than when the gateway observes ACK or business
finally completes. Commands report reception, never execution. CPU uses Linux
procfs clock ticks or macOS `ps` fractional process time; Linux `ps TIME` has only
whole-second precision and is unsuitable for short measurements. RSS is sampled
at 100ms and does not measure allocation peak or isolate
per-connection memory. Source/configuration and executable SHA-256 are retained.

This is a short regression matrix, not a sustainable production capacity or soak
claim. Use `--sink-delay-ms 10` for closed-loop slow-sink measurements; this controls
offered work and does not itself force queue overload. The mixed-codec bounded
backpressure regression explicitly fills required capacity and checks rollback and
complete byte/count cleanup. `--codecs`, `--protocols` and `--workloads` select
focused cases for longer repeat measurements. Synthetic unique commands use an
explicit 1000ms dedup retention window while preserving the 4096-entry hard limit;
this keeps long sweeps from filling retained command history. Client errors fail
the campaign and retain the partial result, including HTTP status and counters.
The workflow's opt-in `linux-comparison` job builds
both revisions on the same Linux x86_64 runner; dispatch with `compare=true` and an
explicit baseline revision. An explicitly tagged main commit containing
`[codec-comparison]` also requests this bounded comparison, using `6e16b31` as the
baseline. Ordinary PR and push smoke do not run the longer comparison. Merely
configuring this job is not execution evidence.
`[codec-followup]` requests alternating before/after microbench repeats and a
0.2-second one-repeat full matrix, keeping CPU sampling correction separate from
the longer matrix. Compare only matching versions within each campaign.

`codec_audit_summary.py --root target/performance/codec-audit --output
docs/performance/codec-audit` validates matching harness/configuration, complete
repeat groups and error-free results, then emits compact per-case medians,
environment and raw-file SHA-256 manifests. Percentiles are medians of each run's
percentiles, not pooled percentiles. The original matrix used the older harness's
default 300000ms dedup window; compare versions within a campaign, not across
campaigns with different options. Raw outputs remain in ignored local directories
or GitHub Actions artifacts.

## Capacity and stability workloads

`production_readiness.py` runs the four fixed Codec formats over real MQTT
3.1.1/5.0, TCP and signed UDP sockets, with bounded observation and confirmed
business ACKs. Use sensor, 16/64-field telemetry and abnormal-device scenarios.

```bash
python3 scripts/perf/production_readiness.py --devices 100 --scenario sensor --duration 60 --tls --output target/performance/production-readiness/sensor
```

See [capacity and stability testing](../../docs/production-readiness.zh-CN.md) for
100/1000/5000/10000-device matrices, owned 6/24-hour runs, report definitions and
unverified production coverage. Use a fresh output directory and preserve failed
results. Existing test fixtures are for loopback testing only.
