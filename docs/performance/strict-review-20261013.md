# Strict correctness review evidence — 2026-10-13

This review starts from `fe4770b240e628418102b9c8964ac81d3273b730`, which is newer than the requested `aa318347aac8c8d45c99041d15bc0b2b0b978bc7` checkpoint. The original optimization baseline is `cae81a9ffc35e5f19b068014018ed3af27632c82`. The matched measurements compare the already optimized starting commit against correctness candidate `b4db4b7be6f3207aac34a9610eb0ce96d4f23e96`; they do not remeasure the original optimization campaign or establish production capacity. All 156 frozen source-file hashes match that candidate commit. See the [code findings and repairs](strict-review-20261013-code.md) for the detailed correctness review.

## Evidence integrity

The path cleanup in `0b88177bb6e0a786c5e5bc2901621382648bafa3` changed three committed text logs without updating their manifest references: `clippy-final.txt`, `workspace-tests-final.txt`, and `tls-interop.txt`. The [original campaign manifest](connection-density-20261013/manifest.json) now records the actual normalized byte counts and SHA-256 values, preserves the raw-output hashes, and records the verified pre-cleanup hashes and byte counts. All 24 committed evidence records match their references. All three original fuzz-log hashes were independently verified against the local `target/density-results` outputs multiple times earlier in this review. At final review those untracked raw outputs are unavailable and cannot be reread; their prior results and hashes are preserved, and the disappearance cause is unknown. The current MQTT state/recovery ASan evidence has complete archived raw/normalized hashes and is unaffected. Test results and binary/recovery data were not rewritten.

The [strict review manifest](strict-review-20261013/manifest.json) records raw and publishable log hashes separately. The [final integrity check](strict-review-20261013/final-evidence-integrity.json) and subsequent self-record check verify all 326 publishable records, 41 canonical nested log/source references, and every local report link. Publishable logs only replace machine-local paths with placeholders. The final nested check also updates two full-reader auth log references from the original bundle’s `<harness>` convention to the shared publisher’s `<tmp>` convention, preserving both raw hashes and the original nested manifest.

The source, report and evidence-tool staged diffs pass Git's whitespace check. The full staged archive reports whitespace in original log output, patch context and CSV CRLF endings; those bytes are preserved and covered by the evidence hashes. The earlier unstaged check did not inspect untracked evidence, and its scope is corrected in the final integrity record.

## Dependency and release verification

`cargo audit` against the original `cae81a9` lockfile fails with exactly the three specified `quinn-proto 0.11.17` vulnerabilities. This dependency version predates the optimization. The starting commit already contains the compatible `0.11.18` patch upgrade in both workspace and fuzz lockfiles. The RustSec records for [RUSTSEC-2026-0343](https://rustsec.org/advisories/RUSTSEC-2026-0343), [RUSTSEC-2026-0340](https://rustsec.org/advisories/RUSTSEC-2026-0340), and [RUSTSEC-2026-0338](https://rustsec.org/advisories/RUSTSEC-2026-0338) identify `>= 0.11.18` as patched.

The lockfile source is the optional `reqwest` HTTP/3 dependency chain, `reqwest -> quinn -> quinn-proto`. Enabled workspace features use JSON and Rustls TLS; `reqwest`'s `quinn?/ring` weak feature does not activate its optional QUIC dependency. Inverse dependency checks for native macOS and Linux x86_64/ARM64, including workspace `--all-features`, contain no active `quinn-proto` dependency. No features were removed to pass the audit.

| Check | Observed result | Evidence |
|---|---|---|
| Original baseline dependency audit | Exit 1; three specified QUIC vulnerabilities | [Log](strict-review-20261013/cargo-audit-cae81a9-final.txt) |
| Current `cargo audit` | Exit 0; zero vulnerability findings; one reported maintenance warning | [Log](strict-review-20261013/completion-before-profiler-owner-fix/audit.txt) |
| `cargo xtask check release --part audit` | Exit 0 | [Log](strict-review-20261013/completion-before-profiler-owner-fix/xtask-audit.txt) |
| `cargo xtask check release --part preflight` | Exit 0 against final helper sources; 33 release-script tests passed | [Log](strict-review-20261013/completion-final/preflight.txt) |
| Python performance unit tests | 27 passed against final helper sources | [Log](strict-review-20261013/completion-final/python.txt) |

The remaining warning is [`RUSTSEC-2025-0134`](https://rustsec.org/advisories/RUSTSEC-2025-0134): `rustls-pemfile 2.2.0` is unmaintained. This warning also appears in the original baseline audit. It is not suppressed, and an audit exit of zero does not mean that the dependency has resumed maintenance. The original/current comparison uses RustSec database commit `29d1a60c3361576d366b1740bba2254dc6173434` with 1,310 advisories. A later current audit and xtask audit also pass after fetching database commit `6aed84668c23d7f535077a9fe918bf1fa0f6a5bc`, again with 1,310 advisories; the original baseline was not rerun against this later database. Both earlier snapshots remain archived.

Final formatting, workspace Clippy with warnings denied, workspace all-feature tests and release preflight pass after the separate test-fixture repair at `a2025e6ca7742b458aec0a175a89799839e74fba`. The final workspace log contains 602 passed tests and 56 explicitly manual/ignored tests across 42 result blocks. The earlier `b4db4b7` log retains 599 passed tests; the new collision test is compiled into three integration targets. The first Clippy run found only a test match-pattern style warning; its failed log and passing reruns are retained. [Workspace evidence](strict-review-20261013/completion-before-profiler-owner-fix/workspace-tests.txt), [Clippy](strict-review-20261013/completion-before-profiler-owner-fix/clippy.txt), [final preflight](strict-review-20261013/completion-final/preflight.txt). The Rust code remains unchanged through the final helper repair; the final Python run passes 27 tests, and release preflight passes 33 tests against the recorded helper/test SHA-256 values. The first final preflight attempt fails because offline `anstyle 1.0.14` is absent from the local cache. Its [environment failure](strict-review-20261013/completion-final/preflight-offline-cache-failure.txt) is retained; the same source and lockfile pass after the network-assisted cache retry. The linked remote CI results identify `b4db4b7`/`a2025e6`; they do not validate a later report/helper commit.

The independent [protocol evidence](strict-review-20261013/mqtt/evidence-summary.json) passes 77 MQTT 3.1.1 release-gate cases covering 125 normative checks, raw and mature Mosquitto MQTT 5 clients, seven additional protocol regressions, 117 broker tests and the 27 final end-to-end tests. Two MQTT packet-ID reproductions fail before the repair and pass afterward across both protocol versions. Two late-repoll deadline reproductions likewise fail before and pass after the explicit post-I/O/authentication deadline checks. Broker state/recovery fuzz runs use ASan and bounded inputs for 30-second engine budgets; they do not cover the subsequently changed Reader deadline branch.

## Native Linux CI profiling

GitHub API results independently confirm that the starting `fe4770b` commit passed both [release verification](https://github.com/sskycn/netbaiot/actions/runs/38405158756) and [native production profiling](https://github.com/sskycn/netbaiot/actions/runs/38405157151). These results apply to that commit, not to the subsequent correctness fixes.

The profiling artifact inspection additionally verifies nonempty CPU stacks, folded data, flamegraphs, and heap allocation output on both native architectures. There are 374 x86_64 and 328 ARM64 `cpu-clock` samples. Folded values are perf period weights, not raw sample counts. This historical workload is 1,000 MQTT 5/TLS telemetry64 devices; it does not establish 10K heartbeat lock contention or performance of the new candidate.

ARM64 heaptrack records 4,772,887 allocation calls and a 20.58M peak, but its allocation stack attribution is entirely `0x0 / ??`. Allocation counters are present; allocation call-site attribution remains unavailable in that runner's heaptrack output. The local inspection keeps the raw artifacts, checks their expanded total size before selectively reading bounded text profiles, and records file hashes in the strict review manifest.

The frozen correctness candidate also passes [Rust release verification](https://github.com/sskycn/netbaiot/actions/runs/38415514229) and the complete [native production-load/profiling workflow](https://github.com/sskycn/netbaiot/actions/runs/38415787048), both at `b4db4b7`. The native x86_64 and ARM64 jobs complete all steps without a profile-only skip marker. The [candidate artifact inspection](strict-review-20261013/candidate-linux-profile-inspection.json) records 394 x86_64 and 334 ARM64 `cpu-clock` samples, with 301 and 247 folded stacks respectively; every folded entry has multiple frames and both flamegraphs are nonempty. Folded period weights are not sample counts.

Both CPU workloads hold 1,000 real online clients for 60 seconds: x86_64 submits, accepts and confirms 59,999 events; ARM64 submits, accepts and confirms 60,000. Each 30-second heap workload submits, accepts and confirms 29,999. All exit zero with empty final queues and zero final sessions. Heaptrack peaks are 20.72M/38.61M requested heap/RSS on x86_64 and 20.58M/36.51M on ARM64; RSS includes profiler overhead. x86_64 allocation call sites are resolved. The ARM64 heap report is only 981 bytes and still contains `0x0 / in ??` frames; names in its command header do not constitute symbol attribution. These profiles demonstrate real workload stacks and bounded completion, not million-device capacity or 10K heartbeat lock attribution.

A separate recovery workflow at `b4db4b7` has a retained [Ubuntu subprocess failure](strict-review-20261013/ci-recovery-original-failure.txt): the child management TCP listener reports `AddrInUse` on a temporary loopback port. The actual CI owner of that port was not logged. Controlled fixture collision injection reproduces the same error class before the repair and passes afterward while preserving spool/replay assertions. This test-only change is committed separately as `a2025e6`; its [new native recovery CI](https://github.com/sskycn/netbaiot/actions/runs/38492097445) succeeds on Linux and macOS. [Fixture source, patches and evidence](strict-review-20261013/recovery-fixture/manifest.json). The original failed run remains archived; a future documentation commit is not substituted for a tested source SHA. The final fixture source also passes all nine jobs in [Rust verification](https://github.com/sskycn/netbaiot/actions/runs/38492097494), including both native Linux archives, and both jobs in [native developer checks](https://github.com/sskycn/netbaiot/actions/runs/38492097476). The [three workflow results](strict-review-20261013/ci-fixture-runs.json) and complete job metadata identify the exact tested `a2025e6` source. The frozen Rust/Cargo snapshot includes 156 files and matches `b4db4b7` in full. Against `a2025e6`, 154 match and only the two integration-test fixtures differ; the measured production runtime inputs remain unchanged. [Source comparison](strict-review-20261013/candidate-vs-fixture-source-match.json).

## Matched connection measurement method

The measurement host is macOS ARM64, with 16 GiB physical RAM and 10 logical CPUs. Read-only kernel limits are 122,880 total file descriptors and 61,440 per process. The shell inherits a larger soft process limit, but this does not override the kernel limits. No global resource, swap, or system configuration was changed.

Both servers use the same current Rust toolchain, locked dependencies, release optimization, debug level 1, and the `netbaiot-server/allocation-profile` feature. The baseline is built in an owned detached worktree. A single frozen baseline load generator is used for both variants. Compiler and regression-test work is stopped during matched measurements. Runs are serial: before/after for repeat 1 and after/before for repeat 2. Each density case ramps at 500 connections/s and holds for 20 seconds; each server uses 10 Tokio workers, while the Rust load generator uses its fixed four-worker runtime. Every held sample must match the requested real online population, all clients must complete their owned disconnect, and nonzero exits/error samples invalidate the comparison.

The first continuous campaign completes all five MQTT cases and its first baseline TCP run, then stops on the candidate TCP run: only 6,332 clients connect and 3,668 fail with local `EADDRNOTAVAIL`. The invalid JSON keeps per-connection RSS/heap results null and remains archived. The inherited loopback ephemeral range is 49,152–65,535 (16,384 ports); read-only TCP MSL is 15,000 ms. A later diagnostic snapshot already shows natural recovery and is not presented as the failing instant's socket peak. The revised TCP comparison repeats all four TCP runs with the same explicit 35-second natural cooldown between runs and immediate loopback TIME_WAIT snapshots. No system limits or workload population are increased or reduced.

The unchanged repository `scripts/perf/connection_memory.py` records actual RSS deltas, process-wide requested live allocator deltas, online clients, P99 connection/heartbeat histograms, task deltas and completed disconnects. A separate owned local wrapper records macOS cumulative process CPU next to the harness RSS snapshots and host-wide `vm.swapusage`. It leaves Linux-only fields null. Host swap is not per-process swap, and neither logical admission allowances nor configured socket ceilings are presented as physical memory.

The 1,000-client baseline smoke held and cleanly disconnected all clients without errors. Its numeric memory results are excluded from the matched comparison because it only validates the harness while other review work remains active.

The preliminary candidate build is retained for source and binary provenance, but excluded from all comparisons because the final deadline checks were added afterward. The candidate used in the campaign must match the final frozen source hashes. No measurement substitutes the preliminary build for those fixes.

The archived [campaign controller](strict-review-20261013/evidence-tools/density_campaign.py) and [CPU sidecar](strict-review-20261013/evidence-tools/memory_with_sidecar.py) invoke the repository harness without changing its limits or Linux-only output. From the repository root, supply the three frozen binary paths as environment variables, then run:

```sh
python3 docs/performance/strict-review-20261013/evidence-tools/density_campaign.py \
  --repo . --baseline "$BASELINE_SERVER" --candidate "$CANDIDATE_SERVER" \
  --loadgen "$FROZEN_LOADGEN" --output "$OWNED_OUTPUT_DIRECTORY" \
  --connections 10000 --repeats 2
```

The output directory must not already exist. The controller records binary hashes, preserves each JSON result, stderr and sidecar, runs the six cases serially, and stops on a failed case. A deadline terminates only its owned child processes. It cannot establish Linux cgroup or kernel memory on this host.

The repeatable TCP correction uses `evidence-tools/tcp_density_campaign.py` with the same binary arguments and `--cooldown-seconds 35`; its output directory is separate from the original campaign. The archive preserves both the original port-exhaustion failure and the corrected method's measurements. The initial campaign controller remains unchanged.

After the frozen candidate has been committed and pushed to its task branch, the existing native Linux workflow can be dispatched with `gh workflow run production-readiness.yml --ref codex/performance-correctness-review -f capacity=false`. The run's head SHA must match that candidate, and both architecture jobs and their actual CPU/heap artifacts must be checked. Historical main-branch success is not candidate validation.

## Matched 10K density results

Each table cell separated by `/` gives repeat 1 and repeat 2, in that order. Every valid run reaches 10,000 actual online clients, completes 10,000 disconnects, and records zero client errors. Requested live heap and RSS are server-process deltas divided by actual online clients; they include shared runtime/auth/session state. RSS is not allocator capacity, and neither quantity includes client-process or kernel socket memory. The [24-run CSV](strict-review-20261013/density-runs.csv) and [machine-readable summary](strict-review-20261013/density-summary.json) preserve unrounded values and individual evidence links.

| Case | Variant | Actual online, repeat 1 / 2 | Requested heap B/connection, repeat 1 / 2 | RSS B/connection, repeat 1 / 2 | Task delta/connection | Disconnects; errors, both runs |
|---|---|---|---|---|---|---|
| mqtt311-idle | before | 10,000 / 10,000 | 10778.98 / 10779.09 | 11901.3 / 12022.6 | 1.0 / 1.0 | 10,000; 0 |
| mqtt311-idle | after | 10,000 / 10,000 | 10787.29 / 10787.42 | 12114.3 / 12168.4 | 1.0 / 1.0 | 10,000; 0 |
| mqtt5-idle | before | 10,000 / 10,000 | 10803.11 / 10803.05 | 12120.9 / 11801.4 | 1.0 / 1.0 | 10,000; 0 |
| mqtt5-idle | after | 10,000 / 10,000 | 10810.77 / 10811.05 | 12081.6 / 11811.2 | 1.0 / 1.0 | 10,000; 0 |
| mqtt311-tls | before | 10,000 / 10,000 | 18243.45 / 18243.40 | 19660.8 / 19659.2 | 1.0 / 1.0 | 10,000; 0 |
| mqtt311-tls | after | 10,000 / 10,000 | 18251.29 / 18251.62 | 19739.4 / 19610.0 | 1.0 / 1.0 | 10,000; 0 |
| mqtt311-heartbeat | before | 10,000 / 10,000 | 10967.81 / 10967.73 | 12158.6 / 11842.4 | 1.0 / 1.0 | 10,000; 0 |
| mqtt311-heartbeat | after | 10,000 / 10,000 | 10975.89 / 10975.90 | 12212.6 / 12161.8 | 1.0 / 1.0 | 10,000; 0 |
| mqtt5-persistent-subscribed | before | 10,000 / 10,000 | 12296.46 / 12296.50 | 13716.7 / 13713.4 | 1.0 / 1.0 | 10,000; 0 |
| mqtt5-persistent-subscribed | after | 10,000 / 10,000 | 12304.55 / 12304.40 | 13641.3 / 13672.4 | 1.0 / 1.0 | 10,000; 0 |
| tcp-idle | before | 10,000 / 10,000 | 8755.37 / 8755.42 | 9620.7 / 9704.2 | 1.0 / 1.0 | 10,000; 0 |
| tcp-idle | after | 10,000 / 10,000 | 8763.52 / 8763.37 | 9650.2 / 9640.3 | 1.0 / 1.0 | 10,000; 0 |

The paired requested heap increase is 7.66–8.32 B/connection across these cases, consistent with the isolated read-account increase of 8 B. The two-repeat RSS and latency variation does not establish an improvement or an absence of all performance regressions.

Cumulative `ps` CPU snapshots cover 24.058–24.099 seconds for ramp plus initial steady state, and 3.167–3.301 seconds for the later steady window. These windows do not span the entire 20-second hold. Percentages use one CPU core as 100%; zero reflects the finite cumulative CPU counter resolution. The detailed JSON records each actual window length. P99 histograms cover the load generator's run, including ramp; heartbeat uses a 2-second interval.

| Case | Variant | Ramp CPU %, repeat 1 / 2 | Steady CPU %, repeat 1 / 2 | CONNECT P99 ms, repeat 1 / 2 | Heartbeat P99 ms, repeat 1 / 2 |
|---|---|---|---|---|---|
| mqtt311-idle | before | 4.86 / 4.74 | 0.00 / 0.00 | 4.24 / 4.33 | N/A / N/A |
| mqtt311-idle | after | 4.78 / 4.78 | 0.00 / 0.00 | 4.22 / 4.38 | N/A / N/A |
| mqtt5-idle | before | 5.19 / 5.11 | 0.00 / 0.00 | 4.37 / 4.19 | N/A / N/A |
| mqtt5-idle | after | 4.94 / 5.15 | 0.00 / 0.00 | 4.66 / 4.38 | N/A / N/A |
| mqtt311-tls | before | 34.91 / 35.66 | 0.31 / 0.31 | 6.30 / 5.16 | N/A / N/A |
| mqtt311-tls | after | 34.36 / 35.60 | 0.31 / 0.31 | 6.68 / 5.01 | N/A / N/A |
| mqtt311-heartbeat | before | 15.12 / 14.75 | 16.10 / 16.68 | 3.64 / 3.97 | 2.30 / 3.26 |
| mqtt311-heartbeat | after | 16.58 / 15.71 | 16.31 / 17.25 | 3.43 / 3.80 | 1.92 / 3.29 |
| mqtt5-persistent-subscribed | before | 7.65 / 7.44 | 0.00 / 0.31 | 4.14 / 4.96 | N/A / N/A |
| mqtt5-persistent-subscribed | after | 7.73 / 7.56 | 0.32 / 0.31 | 4.76 / 5.48 | N/A / N/A |
| tcp-idle | before | 4.16 / 4.20 | 0.00 / 0.00 | 3.69 / 4.06 | N/A / N/A |
| tcp-idle | after | 4.07 / 4.45 | 0.00 / 0.00 | 3.27 / 3.20 | N/A / N/A |

All five host-wide swap snapshots per run report 0M used. Per-process swap/compression, private logical quota balances, and Linux cgroup/kernel socket bytes are unavailable here and remain null/N/A. A task delta of one per live connection is measured; Future sizes, idle channel allocations and cleanup ownership are separately checked by the isolated probes below. Persistent MQTT sessions are intentionally retained after clean transport disconnect, according to the selected session policy.

## Large wire payload and partial-frame cleanup

These serial cases use 32 real connected clients, 100 messages/s for 20 seconds, and a 65,000-byte device wire payload. The event harness does not export a held management online sample, so that field remains null; the real connected/disconnected counts come from the load generator. The payload contains bounded JSON fields with whitespace padding, so normalized business events are smaller. MQTT subscriber delivery is raw MQTT downlink, separate from `DeviceCommand`. This is a near-64-KiB wire test, not an exact maximum-frame or 10K large-payload capacity claim.

The initial MQTT 3.1.1 pair confirms all 2,000 publications, subscriber deliveries and business ACKs, but fails the task gate: cold startup has six tasks and post-load has eight. Both original results remain [invalid and archived](strict-review-20261013/large-original/outcomes.json). Source and the single active webhook connection suggest an HTTP connection driver and lazy pool interval; this is an [owner inference](strict-review-20261013/http-task-diagnostic.json), not stack attribution.

The revised method sends one separately counted 256-byte event on one owned connection, waits for its business ACK, zero online clients and zero required work, and requires three consecutive equal task snapshots before fixing the warm baseline. It then runs the main 2,000-event workload. Webhook totals are 2,001; the prewarm ACK is explicitly subtracted. There is no hardcoded task allowance. Intentional partial frames receive no sink prewarm. Each case has the same 35-second natural cooldown and read-only TIME_WAIT snapshots. All [seven revised cases](strict-review-20261013/large-wire-summary.json) pass their gates.

| Case | Variant | Real connected / disconnected | Main accepted / published | MQTT PUBACK / subscriber delivery | Main business sink ACK | Prewarm events | Tasks baseline → final | Final online / pending | Client / cleanup errors |
|---|---|---|---|---|---|---|---|---|---|
| mqtt311-large-subscription | before | 32 / 32 | 2,000 / 2,000 | 2,000 / 2,000 | 2,000 | 1 | 8 → 8 | 0 / 0 | 0 |
| mqtt311-large-subscription | after | 32 / 32 | 2,000 / 2,000 | 2,000 / 2,000 | 2,000 | 1 | 8 → 8 | 0 / 0 | 0 |
| mqtt5-large-subscription | before | 32 / 32 | 2,000 / 2,000 | 2,000 / 2,000 | 2,000 | 1 | 8 → 8 | 0 / 0 | 0 |
| mqtt5-large-subscription | after | 32 / 32 | 2,000 / 2,000 | 2,000 / 2,000 | 2,000 | 1 | 8 → 8 | 0 / 0 | 0 |
| tcp-large-ingress | before | 32 / 32 | 2,000 / 2,000 | N/A | 2,000 | 1 | 8 → 8 | 0 / 0 | 0 |
| tcp-large-ingress | after | 32 / 32 | 2,000 / 2,000 | N/A | 2,000 | 1 | 8 → 8 | 0 / 0 | 0 |
| tcp-partial-frame-disconnect | after | 16 / 16 | 0 / 0 | N/A | 0 | 0 | 6 → 6 | 0 / 0 | 0 |

The legacy partial-frame case intentionally closes 16 TCP connections after a declared 20-byte frame with only one body byte, over a 5-second scenario. Its configured 65,000-byte payload is not used by that branch. Zero publishes and zero ACKs demonstrate small partial-frame rejection/cleanup; they are not large-buffer evidence or publish throughput. All owned server/load/sink processes exit zero and all normal cases have zero pending acknowledgements at disconnect. Logical reader/global/tenant balances remain unavailable through management and are checked separately by ownership tests.

A separate [maximum-frame large-prefix socket probe](strict-review-20261013/large-prefix/corrected/result.json) authenticates 16 TCP clients using the original server defaults, declares a 65,536-byte body, sends only 32,768 body bytes on each connection, and then closes all clients. Sixteen connections remain online while the prefixes are held, no events are accepted, and process-wide requested live heap grows from 342,900 to 1,392,436 bytes. After disconnect it falls to 220,244 bytes, connections return to zero and runtime tasks return from 22 to the idle baseline of six. The 1,172,192-byte release includes connection ownership as well as reader buffers; the remaining positive auth cache means heap is not claimed to return exactly to the original cold idle value. The server exits zero. An initial attempt used the wrong synthetic credential representation and failed authentication; that invalid fixture attempt and its source/logs remain archived separately, rather than being called a product failure or successful memory measurement.

The legacy `event_load.py` CPU field is cumulative process CPU. It includes startup, the independent prewarm, main warmup/cooldown and other lifecycle work; the exact 20-second publication CPU field remains null. The [measured script snapshot](strict-review-20261013/evidence-tools/event_load-measured-source.py) preserves this behavior instead of retroactively changing the JSON. Per-process swap/compression and Linux kernel/cgroup/socket memory remain unavailable for these runs.

The final script repair preserves this legacy field and additionally exports the previously discarded loadgen CPU delta, its before/after CPU counters, monotonic endpoints, wall duration and explicit scope. The endpoint precedes optional profiler-tail waiting, metrics and shutdown. It covers the loadgen lifecycle including ramp, configured warmup and cooldown, rather than only the configured publication duration. [Two real-main mock regressions](strict-review-20261013/event-cpu/source-manifest.json) fail against the exact previous script and pass after the repair: CPU 100 to 105 yields load delta 5, while cumulative 108 remains compatible. They also check ordering around the profiler tail. The initial missing shared test dependency is preserved as a separate harness setup failure. These new fields are not retroactively inserted into the already measured large-wire results. A [one-client live functional smoke](strict-review-20261013/event-cpu/live-smoke.json) also exits zero with one PUBACK and one business sink ACK and exports the new counters/endpoints. Its 2.101-second load lifecycle has CPU counters of 0.01/0.01 seconds and therefore reports a zero delta at the counter resolution; this does not establish zero CPU use or any capacity. A further [profiler owner regression](strict-review-20261013/event-profiler-owner/manifest.json) tests a failed CPU read while the sampler still runs. The interim script with the new delta fields passes its two normal CPU cases but fails this cleanup case; it is not an original `b4db4b7` reproduction. The final script preserves the error and terminates/reaps all owned processes, including the profiler. That revision passes three main regressions and 26 performance-script unit tests against its recorded source hashes. The final [cleanup-timeout regression](strict-review-20261013/event-cleanup-timeout/manifest.json) uses the same four tests before and after a further bounded-cleanup repair. The interim script fails one case because a profiler wait timeout leaves server and sink untouched. The final script continues independent cleanup for the remaining owners and propagates the first cleanup error with the original CPU error context. The timed-out profiler is not claimed to be reaped. All four main cases and all 27 performance-script unit tests pass against the final script/test hashes; this proof compares measurement-script revisions, not the original Rust baseline.

To reproduce the revised seven cases, supply the frozen binary paths and a new output directory:

```sh
python3 docs/performance/strict-review-20261013/evidence-tools/event_campaign.py \
  --repo . --baseline "$BASELINE_SERVER" --candidate "$CANDIDATE_SERVER" \
  --loadgen "$FROZEN_LOADGEN" --output "$OWNED_OUTPUT_DIRECTORY"
```

## Isolated Reader resource evidence

The candidate [requested-heap probe](strict-review-20261013/candidate-reader-heap.txt) measures replacement allocation peaks with `stats_alloc`, independently of logical quotas. For a 65,540-byte maximum buffer and initial prefixes of 9, 64, 4,096, 32,768 and 65,539 bytes, actual live requested peaks are respectively 65,549, 65,604, 69,636, 98,308 and 131,079 bytes. Each peak equals the charged global and tenant bytes; stable live and charged bytes are 65,540, and both return to zero after cleanup. The probe also checks that the aggregate limit stays unchanged and that the reclaimed allocation is reusable.

The [growth failure reproduction](strict-review-20261013/reader-growth-source-manifest.txt) isolates the old and current Reader under the same current ordinary quota test harness. The old Reader fails by consuming its inline prefix after admission rejection; the repaired Reader passes five prefix lengths under both global and tenant pressure, including alias cleanup. This is an explicitly documented hybrid reproduction, not a rebuild of the frozen baseline or a historical production-load comparison.

The [full-reader ready-authentication reproduction](strict-review-20261013/full-ready-auth/manifest.json) compares both complete authentication functions under the identical candidate Reader/runtime. One read fills an unmetered 8-byte buffer without growth or a quota operation. The original authentication function returns `Invalid` before polling the ready provider (zero calls); the repaired function succeeds with exactly one provider call. This mixed-function harness isolates zero-room behavior; a baseline maximum-frame socket EOF alone cannot distinguish it from the separately demonstrated Reader growth-accounting defect.

The [poisoned-lock reproduction](strict-review-20261013/quota-poison-source-manifest.txt) extracts each version's complete connection-quota implementation into the same test. The old version exits 101 with 16 retained read bytes and a retained tenant entry after releasing the last read guard. The repaired version exits zero with read/tenant bytes and tenant entries at zero while the unrelated 128-byte global reservation and four connection slots remain intact. The source hashes and the shared test hash are preserved; no forced mutex-poison behavior is mixed into the socket load campaign.

An idle unmetered Reader still allocates no buffer. Its struct is 96 bytes and the `read_more` Future is 256 bytes. The metered read account grows from 56 to 64 requested heap bytes per Reader, an explicit 8-byte cost. The actual generic TCP connection Future grows from 1,760 to 1,768 bytes; the MQTT connection Future remains 4,992 bytes and accepted-listener Future remains 5,560 bytes. Lazy broker and command channel probes each remain two allocations totaling 800 requested bytes. The isolated 10K broker and live-session structural probes remain 2,015.681 and 1,614.552 requested bytes per entry respectively. These are isolated allocator and layout observations, not total process RSS or connection capacity.

## Actual 10K heartbeat and disconnect stacks

Two independent candidate workloads use the frozen binaries, one synthetic tenant, 500 connections/s, a 20-second hold and a two-second PING interval. Each macOS `sample` command observes 10,000 actual online connections before beginning its configured ten-second thread-stack capture. The [summary and complete normalized stacks](strict-review-20261013/sampling/summary.json) are separate from the quiet A/B results. Load-generator, sampler and wrapper exits are recorded as zero. The server is waited on and reaped, but its exit code is not exported by this harness and remains unverified.

| Phase | Connected / disconnected | PING sent / responses | CONNECT / PING P99 ms | Stack text bytes, before path normalization | Final online / tasks |
|---|---|---|---|---|---|
| Held heartbeat | 10,000 / 10,000 | 134,870 / 134,870 | 2.82 / 2.97 | 317,836 | 0 / 6 |
| Concentrated disconnect | 10,000 / 10,000 | 134,918 / 134,918 | 3.55 / 1.86 | 427,100 | 0 / 6 |

The PING totals and CONNECT/PING P99 values cover the full approximately 40-second load-generator run, including ramp and hold; they are not statistics for only the configured ten-second stack capture. Metadata records when sampler completion is observed, which may occur later than its capture: the heartbeat start-to-observed-finish interval is 16.297 seconds. This interval is not reported as actual sampling duration.

The disconnect capture starts about two seconds before the common planned close. Actual zero connections and six baseline tasks are observed 2.402 seconds after sample start, within its configured capture duration. That interval includes preceding held traffic and is not the duration of releasing 10,000 connections. Persistent MQTT protocol state is retained according to the selected session policy.

Resolved frames include read admission/release, connection-lease release and native mutex waits. The reported occurrence counts use only the call-graph section, excluding the repeated aggregate table. The prior derived summary is retained with a documented correction to that scope. These counts are nodes, not additive weights, CPU utilization, or lock hold/wait durations. Wall-clock thread sampling includes blocked threads and cannot establish that the global quota mutex is a CPU bottleneck. Connection-ramp and many-tenant authentication contention were not profiled by these two captures. No shared-state rewrite is justified from this evidence.

## Coverage and measurement limits

| Requested quantity or stage | Coverage |
|---|---|
| Idle MQTT 3.1.1/5.0, TLS, heartbeat, persistent subscriptions, TCP idle | All six cases complete two matched 10K rounds per variant; 24 valid runs, plus the separately retained original TCP diagnostic runs |
| Large MQTT ingress and subscribed delivery | Both protocol versions and both variants pass 32-client, 2,000-event 65,000-byte wire workloads with exact PUBACK, subscribed delivery and HTTP 204 ACK counts; prewarm excluded |
| TCP large ingress | Both variants pass the same 32-client, 2,000-event wire workload with ordered `EventAccepted` receipts and confirmed business ACKs; candidate partial-frame case cleans up all 16 intentional disconnects |
| Logical global byte-budget balance | N/A in management API; ownership/release must be checked by isolated quota regression tests |
| Future/channel allocation costs | Isolated release probes are kept separate from actual socket population measurements |
| Linux cgroup and kernel socket memory | N/A on Darwin; null does not mean zero |
| Per-process swap/compression attribution | N/A from the density harness; host-wide swap is supplementary only |
| 50K and 100K connections | Not run: static-credential harness is bounded to 25K, the unchanged single-loopback endpoint has only 16,384 ephemeral source ports, and no authorized externally provisioned gateway is available; 100K server/client pairs also exceed the total kernel descriptor limit and server alone exceeds the per-process limit |
| Million connections and long soak | Not run; no production capacity or crash-durability claim |

No shared synchronization state machine is rewritten based on these limited profiles. CPU sampling and structural/allocator costs are separate evidence from throughput, latency, and production capacity.
