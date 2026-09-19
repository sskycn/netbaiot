# Local capacity audit tooling

These tools target an **owned loopback test service and disposable local PostgreSQL**.
They never tune kernel limits, push code, or contact a production database. The
provided fixture credentials are public test values. Do not use them in deployment.

```sh
CARGO_PROFILE_RELEASE_DEBUG=1 cargo build --release -p netbaiot-server -p netbaiot-loadgen
python3 scripts/perf/run_case.py scripts/perf/cases/calibration.json /tmp/calibration.json --name my_calibration
python3 scripts/perf/matrix.py idle
python3 scripts/perf/matrix.py uplink
python3 scripts/perf/matrix.py tls_cold
python3 scripts/perf/dataset.py
```

PostgreSQL must already be running with `track_io_timing=on`. Defaults are host
127.0.0.1, port 55432, user sam; override PGHOST/PGPORT/PGUSER. Native programs
and libraries use `/opt/local/lib/pgsql` and `/opt/local/lib/icu`. Each experiment
creates a new `cap_<name>` database and leaves it available for inspection.
An existing database produces an error rather than being deleted or reused.
The runner always stops its owned server, generators, sink and injection processes.

A result JSON contains its complete `spec`. Reproduce it with:

```sh
python3 scripts/perf/run_case.py docs/performance/CASE.json /tmp/repeated.json --name unique_repeat_name
```

The matrix skips completed result files; it does not silently replace unfavorable
runs. Review the result's `error`, exit statuses and final generator counters;
process completion alone does not mean the requested offered rate was achieved.

## Separate Rust generator

`target/release/netbaiot-loadgen CONFIG.json` is a separate process. MQTT/TCP use
real sockets, HTTP uses reqwest, UDP signs actual wire envelopes. No transport
load result comes from direct server function calls. Its config supports:

- `transport`: mqtt/tcp/http/udp; `address`, `http_url`, optional `tls_ca`.
- `connections`, `offset`, `tenant_width`, `ramp_per_sec`.
- `duration_secs`, `warmup_secs`, `cooldown_secs`, `report_every_secs`.
- `publish_rate`: total offered rate across devices; per-device rate = total/N.
- `qos`: 0/1; `payload_bytes`; `heartbeat_every`; `subscribe`.
- `command_rate`, bounded `command_concurrency`, `command_padding`.
- `slow_fraction`, `reconnect_every_secs`, `reconnect_fraction`,
  `retry_connections`, `clean_disconnect`, `bad_auth`, `partial_frame`, `udp_replay_every`.
- `phases`: successive `{seconds, rate}` entries for normal→overload→normal.
- `tls_resumption`: false by default, so independent virtual devices do not
  accidentally share cached TLS tickets. True intentionally measures shared-cache
  resumption and must be labeled separately from full handshakes.

All clients have one owner task. Total owners ≤ configured devices plus ≤32 command
producers. Each stream has a ≤65541-byte receive buffer and ≤32 pending receipts/IDs.
HTTP receipts are consumed as chunks with a 64 KiB cumulative limit, without
collecting an arbitrary response body before checking its size.
No per-message tasks or unbounded sender queues exist. Absolute run deadlines abort
and join stragglers; retries are bounded by time and 10,000 reconnects/device.
Histograms use fixed buckets shared by the process. SIGTERM from the runner is the
fallback process-level cleanup for an interrupted run.

Virtual devices use credentials aN, tenant t(N/tenant_width), product p, device dN.
A runner spec can include up to seven `extra_loads`, each with its own offset and
transport; this enables independent healthy/noisy groups and intentional mixed load.
Only one case runs at a time. The server, generator(s), database and sink are
colocated, and their resource usage is sampled separately.

## Measurement definitions and limits

- CONNECT latency includes socket/TLS establishment and MQTT CONNACK or TCP auth.
- PUBACK latency starts before client write and ends at client receive.
- Application receipt latency is tracked for QoS0 **and** QoS1; sent counts alone
  are not accepted counts. UDP has no receipt; use server metric/DB deltas.
- Command queue-to-receive starts immediately before admin submission (wall-clock
  milliseconds); it includes enqueue, polling, dispatch and network delivery.
- Client downlink PUBACK write time is a turnaround measure, not a device execution
  guarantee. Optional server debug tracing measures send-start→PUBACK handling,
  including local write and handler scheduling, before attempt fencing.
- SQLx acquisition tracing includes queue wait, opening connections and health
  checks; it is not a pure semaphore-wait measurement. Enable only in diagnostic
  runs with `rust_log: "warn,sqlx::pool::acquire=debug,netbaiot_transports::mqtt=debug"`.
- Latency bucket upper bounds: 10us below 100ms, 1ms through 60s; max/mean are exact
  at microsecond resolution. The final bucket is overflow, not a 60s latency cap.
- `client_window_full` and generator schedule lag expose pacing limitations. The
  generator drops missed schedule slots instead of creating infinite catch-up work.
- Payloads contain bounded scalar fields up to the codec's 64-field/256-byte limits.
  Near 64KiB requires trailing JSON whitespace after those fields; it stresses wire
  size/scan cost, not an unsupported 64KiB scalar or binary IoT payload.
- HTTP currently closes after one request. The generator does not claim keep-alive
  reuse where the service explicitly disables it.
- RSS is `ps` RSS, not allocator live bytes, physical footprint or a reservation sum.
  `vmmap` classifies live allocated bytes/dirty allocator regions when requested.
- Sampling and diagnostic profiles have overhead. Profile/logging runs are marked
  separately from uninstrumented latency comparisons. No CPU affinity or frequency
  tuning is applied; record variation across repetitions.

`sql_trace` enables statement duration logging **only for the disposable database**,
with parameter values suppressed. The result stores byte offsets into the owned
cluster's `/tmp/netbaiot-capacity-postgres.log`; summarized query counts/timings are
retained, not the large raw log. Dataset scripts bypass application quotas solely
to measure query plans at 10K/100K/1M rows and do not demonstrate supported capacity.

DB unavailability intentionally tests the existing fail-stop worker policy. A
`restart` event is an explicit external restart after database recovery, not a
claim that the service contains an automatic supervisor.

## Complete audit sequence

Run groups serially (the matrix result files preserve their own exact settings):

```sh
python3 scripts/perf/sustained.py
python3 scripts/perf/extended.py commands
python3 scripts/perf/extended.py protocols
python3 scripts/perf/extended.py recovery
python3 scripts/perf/extended.py churn
python3 scripts/perf/extended.py fairness
python3 scripts/perf/extended.py profiles
python3 scripts/perf/extended.py database_growth
python3 scripts/perf/dataset.py
python3 scripts/perf/run_case.py scripts/perf/cases/soak.json /tmp/soak.json --name my_soak
python3 scripts/perf/summarize.py
python3 scripts/perf/evidence.py
python3 scripts/perf/profiles.py
python3 scripts/perf/slow_sql.py
python3 scripts/perf/sql_summary.py generator_calibration downlink_sql_profile
```

The runner now reserves an unused final credential in a separate `audit-observer`
tenant for its metrics observer. Intermediate `uplink_isolated_*` results reserved
only an unused device, which could still share a tenant. `reproduction_overrides`
records earlier observer/checkpoint/TLS conditions and is merged by the CLI when
replaying a result.
Earlier observer-shared results remain marked and are excluded from capacity
claims. Its default precondition completes `CHECKPOINT` before measurement;
`checkpoint_before_seconds` records this. Earlier results without that field ran
without this precondition and include cross-case background checkpoint noise.
Fsync, synchronous commit, autovacuum and the production schema are unchanged.
Background I/O from the host/cluster still cannot be attributed perfectly; global
WAL and pg_stat_io are not per-database or per-request counters.

Later instrumented server builds expose current Tokio tasks, session/subscription
registry sizes, and ingress/protocol in-flight permits. Missing keys in older raw
files mean unobserved, not zero. HTTP metrics itself holds a protocol permit.
Presence has the configured dedup TTL; it is separate from a live socket session.
The count/byte gauges are separate instantaneous samples, not an atomic snapshot.

Diagnostic logging is limited to 120-second cases. Observer history has a 10,000
sample cap, multi-generator cases have at most eight generators, injected blocker
processes are bounded, and each process has an owner and termination deadline.
The two-hour soak must run without another benchmark, compiler or dataset loader.
Finite repeated windows are labeled as such; they do not prove 24-hour retained
state capacity or a production SLA.

The `resource-evidence.json` derivative records sampled resource extrema, separate
process CPU, phase windows, final command states and shutdown outcomes. Soak
five-minute windows and cumulative-count-derived interval means show trends;
interval P95/P99 cannot be reconstructed by subtracting cumulative percentiles.
`audit-overview.json` identifies the largest server RSS across all case snapshots
and preserves runner failures/cancellations. It is not a count of passed cases.
Observed PostgreSQL backend CPU excludes checkpointer/autovacuum and backends
that appear/disappear between samples. All sampled peaks can miss shorter spikes.

The one-off `run_rest.sh` / `finish_run.sh` orchestration files document this audit's
serial continuation and bounded completion gates. For a new audit use the explicit
sequence above with fresh case names/databases; old output files must not be used
as synchronization signals for a different run. Final contracts/fuzz can be run
with `python3 scripts/perf/validate.py` after the workspace checks.

CPU profile duration is bounded by `sample_seconds` (1–15 seconds, default 3).
`vmmap` may suspend the target, so new profile runs collect peak vmmap only after
`sample` has exited. `diagnostic_events` records their ordering. The older
`downlink_sql_profile` overlapped both and had verbose logging; its latency and
hot stacks are diagnostic artifacts, not representative capacity measurements.
Its reproduction overrides retain `profile_vmmap_overlap: true`. Later clean
profiles use 10 seconds and no verbose SQL/PUBACK logging. The separate
`downlink_timing` run enables only low-rate command PUBACK timing.

Rate phases take effect when each virtual device next reaches its due time. They
do not reset all per-device phases to a new uniform distribution; reconnects can
leave devices aligned. Report actual sends, and do not attribute post-overload
latency entirely to database growth without checking arrival shape. UDP has one
serial receiver/store path and no receipts: successful send_to calls can exceed
userspace receive counts under kernel-buffer pressure. Metrics before ingress
(decode/replay drops) and kernel drops are not all represented by ingress_rejected.

Sink control accepts only `delay` (seconds, 0–10) and `status` (HTTP status).
The old `sink_slow_recovery` used an ignored `delay_ms` field; it is explicitly
invalid evidence and is replaced by `sink_delay_200ms_verified`. Reproduction of
that old invalid spec now fails validation instead of silently skipping the fault.

Database unavailability is injected from the `postgres` management database;
PostgreSQL rejects disabling the current database. `db_unavailable` is retained
as an invalid injection/runner failure; `db_unavailable_verified` is its correction.
`db_exhausted` includes an explicit restart of a still-live process. Use
`db_lock_8s_no_restart` to assess natural recovery from long advisory-lock stalls.

The slow-consumer boundary probes use separate configurations: the default count
window, 1 KiB/connection + 4 KiB/tenant + 32 KiB/node, and a four-tenant
1 KiB/connection + 4 KiB/tenant + 8 KiB/node case. Do not describe the latter
two as production defaults or equate a sampled aggregate gauge with a per-device
measurement. A keepalive or pending-limit close is not proof of a socket write
timeout; the existing bounded duplex writer regression tests that deadline.

Summary byte rates distinguish generated telemetry/heartbeat payload bytes per
measurement second from counted stream/datagram writes per full generator lifetime.
The latter includes connect/control writes, excludes received bytes and reqwest
HTTP traffic, and is not a packet-captured bidirectional network bandwidth metric.

Fuzz dependencies must be cached before an offline smoke run. If absent, use
`cargo +nightly fetch --manifest-path fuzz/Cargo.toml --locked` with network access,
then rerun `python3 scripts/perf/validate.py --fuzz-only`. This preserves successful
PostgreSQL contract results instead of repeating unrelated tests. Initial build
failures are retained separately and are not counted as executed fuzz iterations.

During a case, `status.py RESULT.json --generator-log /path/to/generator.log`
reads existing output only. It adds no network/database probes. Server and generator
snapshots have separate timestamps; a transient count difference is not itself loss.
After completion the result embeds the generator log, so `--generator-log` is optional.

After all live workloads finish, `python3 scripts/perf/index_probe.py` can compare
the exact frozen expired-command query on the existing `cap_dataset_10000`,
`cap_dataset_100000`, and `cap_dataset_1000000` fixtures. Each comparison warms
once and retains three EXPLAIN plans before, with, and after removing a temporary
partial expiry index. The unique probe index is removed in `finally`, with absence
recorded in the result. This A/B/A probe measures an empty expired-command poll;
it does not measure index write amplification or prove an end-to-end throughput
improvement. It creates no production migration.

Optional `python3 scripts/perf/plots.py` requires matplotlib and generates standalone
PNG/SVG charts after all workloads finish. Plotting dependencies are separate from
the Rust service; the versions used for this report are recorded in
`docs/performance/plotting-environment.json`.

After generation, `python3 scripts/perf/normalize_text.py` removes trailing padding
and blank EOF lines from text logs, native profiles and SVGs for Git review. It
preserves numeric data and leading stack indentation; JSON and binaries are untouched.
