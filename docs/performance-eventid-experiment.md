# EventId generation experiment (2026-09-22)

> Historical measurements from the revision stated below. Device HTTP has since
> been removed; see the [current migration and verification report](remove-device-http.md).

## Current contract, inspected before selecting the candidate

Starting local HEAD was `43d76c3ccb7271b6b82b1c058c3f879c383ebd22`, with a clean
working tree. The reference SHAs are correctness
`ce07b5b126b04f7ae95749c46e0263c7514fe491`, initial performance
`70b855f1a97ebe1222dc0212fe0847e91b07cf4b`, broker-route runtime
`cbbad11d827d3d8e04160819ab72378f6d9ae9a9`, and EventBus evidence
`43d76c3ccb7271b6b82b1c058c3f879c383ebd22`. The EventBus candidate was reverted.

| Question | Current source evidence and answer |
|---|---|
| Rust type? | `netbaiot-protocol/src/lib.rs`: `pub struct EventId(pub Uuid)`, transparent serde; `netbaiot-core` re-exports it. |
| Public UUID? | Yes, including its public tuple field, ordering/hash traits and `MessageId` alias. |
| Canonical UUID text? | Yes, UUID serde and Display emit lowercase hyphenated text; deserialization accepts supported UUID forms/versions. |
| Restart spool? | Yes, runtime spool records serialize the complete `DeviceEvent`, including the original ID. |
| NBMQ/other recovery? | NBMQ stores MQTT packet/session/Will state, not EventId. Accepted business events live in the separate EventBus restart spool. |
| Business consumers? | Yes: `EventAccepted`, `DeviceEvent`, stream envelopes/ACKs, webhook `Idempotency-Key`. |
| SDK UUID parsing? | Yes, client/device SDK share protocol models and their UUID-backed serde; CLI uses the client. Ordinary MQTT clients remain independent. |
| Tests require v4? | Existing generation calls v4, but wire tests even accept nil UUID. No parser restriction to v4; generated v4 remains observable behavior and is preserved. |
| Logs/metrics formatting? | Logs and HTTP idempotency headers use Display; metrics do not use EventId labels. Preserve formatting. |
| Generation placement? | `JsonV1::decode` constructs the event after payload/schema/time validation, before EventBus admission. Later capacity or endpoint-kind rejection may still consume an ID. |
| source_message_id distinct? | Yes: device-supplied bounded ASCII source identity, separate from the gateway's globally unique accepted-event identity. |
| Across restart/processes? | Required for business deduplication. No distributed routing/cluster supervisor is implemented, but independent processes and blue/green overlap must not reuse IDs. |
| Unpredictable or unique? | The explicit application contract is uniqueness/idempotence. No EventId capability, authorization token, or replay secret was found. Auth uses separate credentials/tokens; ACK correlation occurs on an authenticated stream. Preserve inherited cryptographic randomness anyway. |
| Explicit crypto requirement? | No EventId-specific requirement in current docs/types; `Uuid::new_v4` nevertheless currently uses OS randomness. This experiment must not silently weaken that property. |

Supporting paths: protocol UUID macro and public frames; codecs `JsonV1::decode`;
runtime `ingress.rs`, `event.rs`, `spool.rs`; server `business_handshake` and
`tests/official_client.rs`; transports MQTT `accept_iot_publish`/`bind_will` and
broker recovery; `docs/public-protocol.md`, `business-integration.md`,
`device-protocol.md`, `restart-spool.md`, `pure-event-bus-refactor.md`.

## Generation and identity ownership

`transport -> Ingress::ingest_inner -> JsonV1::decode -> EventId::generate ->
Uuid::new_v4 -> getrandom::fill -> getentropy` on this macOS host. Locked versions
are UUID 1.25.0 and getrandom 0.4.3. UUID asks for 16 random bytes per call.

Each new valid IoT logical event from HTTP, MQTT QoS0/1/2, TCP or UDP receives one
EventId. Invalid codec input receives zero. A validated event that subsequently
fails admission can consume one, so total generation need not equal acceptance
under rejection. A new IoT-bound valid Will event receives one; raw broker Will
state/retained deletion do not themselves generate EventIds. MQTT QoS2 duplicate
state transitions do not decode a new logical event. Device retransmission as a
new publication is distinct from redelivery of an already accepted event.

Sink retry, fanout to multiple sinks, restart replay and duplicate delivery of
the same accepted event generate **zero** EventIds: they share/restore the original
event. Other uses of `EventId::generate` (SDK source identifiers or server error
request IDs) are auxiliary calls, not another ID per accepted event. CommandId,
DeliveryId, SubscriptionId and spool temporary UUIDs are outside this experiment.

## Candidate selected after this contract review

Test exactly one replacement: an independent thread-local `rand::rngs::StdRng`,
seeded directly from OS randomness with a 256-bit seed, then format its 16-byte
outputs using UUID's v4 builder. Locked rand 0.10.2 uses ChaCha12 and is already
transitive; a direct dependency edge needs no new package/version. Keep the other
UUID identifier generators unchanged. No custom cipher, global mutex, shared
counter, timer, background task, network, database or unsafe is needed.

Check process identity before using TLS state so a fork cannot reuse its parent's
stream. Reseed on initialization, PID change or the explicit emission limit only;
there is no periodic reseeding. A bounded per-thread counter stops before wrapping.
TLS borrow/availability or seed failure falls back to the existing UUID generator,
preserving its existing infallible API (the upstream UUID implementation panics
if OS randomness fails; do not introduce deterministic fallback randomness).

This preserves cryptographic unpredictability, not just uniqueness. References:
[RFC 9562](https://www.rfc-editor.org/rfc/rfc9562.html) and
[rand 0.10.2 StdRng](https://docs.rs/rand/0.10.2/rand/rngs/struct.StdRng.html).
This does not turn EventId into an authorization capability.

As with other stateful CSPRNGs, exposure of generator memory could reveal future
outputs until a fresh seed; this is not a claim of forward secrecy after process
compromise. Snapshotting and cloning an entire running VM with identical process
and RNG state is outside the tested restart/exec model. Normal independent execs,
fork PID changes, rapid restarts, machine reboots and clock rollback do not reuse
generator state. No clock participates in the algorithm.

Lazy seeding is local to the generating thread; cold first-ID timing is measured
separately. Adding worker lifecycle hooks would broaden this experiment. State is
bounded per calling thread and has no per-ID heap allocation. External users own
their thread count; the gateway's existing worker count stays bounded.

## Measurement protocol

Fresh release builds, same Apple M4 10-core/16 GiB host, stable Rust 1.97.1,
locked dependencies, serial measurement processes. Frozen server and loadgen
binaries and raw evidence live under `target/eventid-experiment/{before,after}`.
This task serialized compilation and benchmark runs. Other host activity was
not controlled; concurrent documentation work was discovered later in the shared
checkout, and its contribution to the observed variability was not measured.

`eventid_probe` generates one million IDs **per thread**, 1/2/4/8/10 threads,
three process repetitions. It uses one barrier and two clocks per worker around
the whole loop, `black_box`, no formatting or allocation inside the timed loop.
Report medians; aggregate ns/ID is not per-thread latency. Child process CPU time
includes thread startup, first ID and 10k warmup IDs per thread. Cold first-ID
timing is reported separately. OS scheduling/performance-vs-efficiency cores
influence scaling on this shared host.

The network matrix uses 64 publishers, 256-byte JSON, concurrency eight, the
required audit sink, identical opt-in lock metrics, five-second warmup and
two-second cooldown. QoS1 20k uses three 20-second measurements; 25k/30k and
QoS0 25k use 15 seconds; QoS2 20k uses 20 seconds. A separate QoS1 30k run captures
10 seconds of macOS `sample`. Absolute server CPU seconds are `ps time` deltas
across the complete load process, including its setup/warmup/cooldown; measurement
duration and total window are identical before/after. CPU percent peaks are a
different, noisy statistic. These are shared-host observations, not capacity SLAs.

## Collision and exhaustion analysis

The v4 layout retains 122 pseudorandom bits. Under the CSPRNG assumption, the
birthday collision bound is approximately `n(n-1)/2^123`: 10 million IDs gives
`9.404e-24`, and one trillion gives `9.404e-14`. Independent 256-bit OS seeds have
a birthday collision bound `m(m-1)/2^257`; one billion generator lifetimes gives
`4.318e-60`. These bounds are not a mathematical guarantee of no UUID collisions.

The locked ChaCha implementation has a 64-bit block counter and 64-byte blocks,
or `2^66` consecutive 16-byte outputs before cycling. This candidate reseeds
before emitting ID `2^64` from one state. At one billion IDs/second **per thread**,
that limit takes 584.54 years; an explicit guard handles it rather than relying
on the estimate. A focused test forces both the emission limit and PID mismatch.
No wrapping public counter or timestamp is encoded into the UUID.

Uniqueness stress sorts ten million 128-bit IDs for each of 1/2/4/8/10 threads:
50 million checked within their respective runs, not a single combined 50m set.
Worker vectors plus an exactly reserved merged vector bound raw storage to 320 MB;
in-place sorting avoids a ten-million-entry hash table. Separately, 100 execs
in 50 barrier-released pairs contribute a combined one-million-ID set, and one
additional barrier-released pair contributes two million IDs to an overlap set.
These bounded sets provide regression evidence, not a proof of uniqueness.

## Direct generation BEFORE / candidate AFTER

| Threads | BEFORE IDs/s | AFTER IDs/s | BEFORE ns/ID | AFTER ns/ID | BEFORE CPU s | AFTER CPU s | BEFORE involuntary switches | AFTER switches |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,553,104 | 70,451,271 | 643.872 | 14.194 | 0.651181 | 0.016477 | 167 | 44 |
| 2 | 3,744,709 | 156,748,522 | 267.043 | 6.380 | 1.076362 | 0.027716 | 667 | 43 |
| 4 | 3,127,378 | 268,900,025 | 319.757 | 3.719 | 5.101853 | 0.055831 | 7,003 | 474 |
| 8 | 1,635,624 | 394,687,350 | 611.388 | 2.534 | 37.075710 | 0.132901 | 100,655 | 565 |
| 10 | 1,593,036 | 438,468,297 | 627.732 | 2.281 | 51.677105 | 0.173201 | 126,030 | 559 |

Single-thread ns/ID fell 97.80%; throughput rose 4,436.16%. The primary >=50%
microbenchmark condition passes. CPU and context switches are process totals,
not per-ID values. Source inspection finds zero per-ID heap allocations; no
allocator-hook count was collected. StdRng owns its fixed state/output buffer
inline. TLS initialization can allocate platform thread storage once per thread.
Cold first-ID median is 1.792 -> 2.083 microseconds at one thread, a 0.291 us
increase, making deferred thread-local initialization negligible in this test.
There is no timer or background seeding work.

An initial probe-only baseline (before finalizing the profile/process-barrier
helper) measured 707.43 ns/ID. The table uses the **rerun with the same timed loop
as AFTER**, 643.87 ns/ID. The helper's collecting/profile modes are separate from
the bench loop; their results are not substituted for microbenchmark throughput.
The original direct-profile JSON used the nominal input count for its
`worker_ns_per_id` field; ignore that field in those two profile records. Timing
results above use `micro.json`, whose count is correct. The retained helper now
divides by the actual generated count in every mode.

## End-to-end matrix BEFORE / candidate AFTER

| Case | Phase | Accepted/s | P50 ms | P95 ms | P99 ms | Server CPU s | Peak CPU % | Peak RSS KiB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| q1-20k-1 | BEFORE | 19,998.80 | 0.16 | 0.26 | 0.30 | 13.07 | 72.6 | 7,840 |
| q1-20k-1 | AFTER | 19,916.60 | 0.18 | 0.34 | 0.67 | 13.60 | 70.2 | 8,304 |
| q1-20k-2 | BEFORE | 19,999.15 | 0.16 | 0.28 | 0.43 | 14.67 | 92.8 | 7,856 |
| q1-20k-2 | AFTER | 19,998.65 | 0.16 | 0.26 | 0.32 | 13.78 | 69.5 | 7,952 |
| q1-20k-3 | BEFORE | 19,981.40 | 0.33 | 1.06 | 1.58 | 21.29 | 129.6 | 7,888 |
| q1-20k-3 | AFTER | 19,998.95 | 0.16 | 0.26 | 0.31 | 14.09 | 71.5 | 7,984 |
| primary_median | BEFORE | 19,998.80 | 0.16 | 0.28 | 0.43 | 14.67 | 92.8 | 7,856 |
| primary_median | AFTER | 19,998.65 | 0.16 | 0.26 | 0.32 | 13.78 | 70.2 | 7,984 |
| q1-25k | BEFORE | 24,161.60 | 0.34 | 1.06 | 1.86 | 16.39 | 134.1 | 7,904 |
| q1-25k | AFTER | 24,390.27 | 0.21 | 0.34 | 0.43 | 12.35 | 84.7 | 7,840 |
| q1-30k | BEFORE | 26,584.00 | 0.35 | 1.04 | 1.95 | 17.41 | 141.2 | 8,128 |
| q1-30k | AFTER | 27,096.53 | 0.20 | 0.32 | 0.38 | 13.81 | 94.5 | 7,936 |
| q0-25k | BEFORE | 24,087.47 | n/a | n/a | n/a | 16.42 | 124.8 | 7,856 |
| q0-25k | AFTER | 24,271.13 | n/a | n/a | n/a | 10.99 | 75.9 | 8,144 |
| q2-20k | BEFORE | 19,976.70 | 0.63 | 1.32 | 2.11 | 28.94 | 155.4 | 8,048 |
| q2-20k | AFTER | 19,989.20 | 0.37 | 0.57 | 0.66 | 23.88 | 124.2 | 8,208 |
| profile-30k | BEFORE | 25,904.30 | 0.33 | 0.86 | 2.04 | 23.13 | 133.9 | 8,160 |
| profile-30k | AFTER | 27,066.40 | 0.20 | 0.35 | 0.63 | 18.74 | 95.5 | 8,240 |

The primary median changes are accepted/s -0.00075%,
CPU time -6.07%, P99 -25.58%,
and peak RSS +128 KiB (+1.63%).
There were no admission rejections, protocol violations or sink retries in these
matrix runs. The first AFTER primary has 46 client-window-full observations and
30.245 ms maximum load-generator scheduling lag; its lower delivered traffic is
retained in the table, not discarded. The offered-load plateau remains in the
25--30k region; accepted QoS1 at offered 30k improves 1.93%, not the required 5%.
This coarse sweep cannot establish a precise knee or a production limit.

BEFORE primary CPU ranges 13.07--21.29 s and P99 0.30--1.58 ms. Thus the initial
median crosses the CPU/P99 thresholds, but is not by itself robust evidence of a
causal gain. Three additional 20-second pairs use BEFORE/AFTER, AFTER/BEFORE,
BEFORE/AFTER order with the same frozen binaries/configuration. These results are
reported separately and are not pooled to select a favorable median.

## Profile comparison

MacOS `sample` measures wall-stack presence, including blocked threads, **not**
exclusive CPU. Network non-park denominator removes `__psynch_cvwait` and
`kevent`; direct probe removes the main thread's `__ulock_wait` join. Collapsed
top-of-stack rows omit symbols with fewer than five samples, so category counts
are lower bounds. Categories use disjoint symbol groups, except unspecified
other work remains outside the table.

| Network top-of-stack category | BEFORE count / share | AFTER count / share |
|---|---:|---:|
| entropy | 64 / 3.02% | 0 / 0.00% |
| mutex_wait | 540 / 25.52% | 1708 / 36.06% |
| send_recv | 284 / 13.42% | 730 / 15.41% |
| timekeeping | 253 / 11.96% | 455 / 9.61% |
| malloc_free_copy | 165 / 7.80% | 331 / 6.99% |
| json_codec | 54 / 2.55% | 151 / 3.19% |
| tokio | 184 / 8.70% | 330 / 6.97% |
| generator_rng | 0 / 0.00% | 0 / 0.00% |

Network denominators are 2,116 BEFORE and 4,737 AFTER non-park samples. The
BEFORE call tree attributes entropy to `JsonV1::decode -> Uuid::new_v4`.
AFTER has no observed getentropy row/call stack during the measured steady-state
window. Initialization occurred before sampling. The absolute server CPU of the
profiled runs is in the matrix; increasing mutex share is not proof that the
candidate adds a mutex. The generator source has none, and its isolated sample
has zero mutex waits.

Direct generation: getentropy is 8,677/8,680 worker samples (99.97%) BEFORE and
0/8,666 AFTER. AFTER is primarily the inlined EventId/RNG implementation (7,616),
getpid plus its stub (714), and TLS lookup (300). No sampled malloc/free or mutex
waits appear in the isolated loop. This corroborates removal of per-ID entropy
calls, but does not measure all allocations or exact syscall counts. OS entropy
still applies on cold initialization, PID change, exhaustion and exceptional
fallback; the optimization claim concerns normal steady-state generation.

## Alternating confirmation and decision

| Pair | Phase | Accepted/s | P50 ms | P95 ms | P99 ms | CPU s | Peak RSS KiB |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | before | 19,998.20 | 0.19 | 0.34 | 0.47 | 14.27 | 7,952 |
| 1 | after | 19,908.05 | 0.22 | 0.69 | 2.38 | 13.54 | 8,288 |
| 2 | before | 19,978.65 | 0.24 | 0.66 | 1.19 | 16.29 | 8,096 |
| 2 | after | 19,953.60 | 0.22 | 0.62 | 1.04 | 15.04 | 8,096 |
| 3 | before | 19,998.95 | 0.16 | 0.26 | 0.31 | 13.38 | 7,856 |
| 3 | after | 19,991.30 | 0.20 | 0.34 | 0.45 | 13.70 | 7,984 |

Confirmation medians: accepted/s 19,998.20 -> 19,953.60 (-0.223%), CPU seconds
14.27 -> 13.70 (-3.99%), P99 0.47 -> 1.04 ms (+121.28%), RSS 7,952 -> 8,096 KiB
(+144 KiB). Pair three CPU actually rises 13.38 -> 13.70 s. Both first two
candidate runs have higher scheduling/tail variability; no run was discarded.
This does not prove the RNG causes the tail regression, but it fails to establish
the required no-regression outcome on this host. No further workload or generator
was tuned to get a favorable result.

**Decision: REVERT.**

**EXPERIMENT REVERTED — insufficient end-to-end gain**

The isolated >=50% threshold passes decisively (97.80% lower ns/ID). The nominal
>=3% end-to-end CPU threshold passes in both sets of medians; the first matrix
also passes the >=5% P99 improvement threshold. However, confirmation reverses
the P99 result and reduces throughput, so the mandatory secondary requirements
are not met. CPU improvement is not uniform across pairs. The knee does not
improve >=5%. A TLS CSPRNG with PID/exhaustion/fallback handling is not simpler
than the existing UUID call, so the simplicity exception does not apply.

Candidate production code and the direct rand dependency edge were removed.
Final runtime EventId generation remains `Uuid::new_v4`, including per-event OS
entropy. Existing transitive rand packages are unchanged. The numbers labeled
AFTER in this report describe the **discarded candidate**, not the final tree.
There is no EventId optimized baseline. Runtime baseline remains
`cbbad11d827d3d8e04160819ab72378f6d9ae9a9` plus EventBus and EventId measurement/test
commits. No EventBus, broker, codec production logic, parser, I/O, scheduling,
spool format or allocation optimization was retained.

## Retry, replay, duplication and compatibility verification

- Added `retries_and_required_fanout_preserve_exact_accepted_event_id`: two required
  sinks each fail twice then ACK; all six observed deliveries and EventAccepted
  contain the exact original EventId. Final count/byte accounting is empty.
- Added `generated_event_ids_keep_uuid_v4_and_canonical_serde_contract`: distinct
  IDs, RFC4122 variant, version Random/v4, canonical Display, UUID parsing, JSON
  round-trip and unchanged nil-UUID acceptance.
- Extended codec coverage so each of the four valid event kinds decodes to one
  event; decoding a new event with the same source_message_id produces a distinct
  EventId. Invalid/malformed/bounds cases remain in the same test.
- Existing official-client reconnect test explicitly checks duplicate/unacked
  replay preserves EventId while delivery_id changes. Existing spool round-trip,
  repeated recovery and server subprocess planned-restart tests compare exact
  pre-restart accepted IDs with recovered IDs.
- Candidate-only forced emission-limit/PID-change and reentrant-TLS fallback
  tests passed with protocol/codecs/runtime focused tests (45 passed, one ignored
  benchmark). They were removed with the candidate. The candidate's 50m topology
  stress, 100-exec stress and 2m overlap test all found zero duplicates.
- Parser/serde implementation was unchanged. No new parser fuzz campaign was
  run; existing malformed-input, serialization and protocol suites are the
  relevant regression checks. No claim is made about actual OS RNG failure
  injection, a real fork test, whole-VM snapshot cloning or exhaustive uniqueness.

## Retained bottleneck ranking

This ranks the final unchanged runtime using the fresh BEFORE evidence, not the
reverted candidate profile. Rankings describe this shared-host, no-subscriber,
required-audit-sink workload only.

1. **Serialized EventBus/shared-state contention.** Mutex wait is 25.52% of
   non-park network samples; this aggregates multiple process mutexes and cannot
   all be assigned to EventBus. Dedicated measurements show 4.763 EventBus state
   acquisitions/event at 20k, state wait mean 1.387 us and P99 <=50 us; publish
   wait mean 3.688 us and P99 <=100 us. At the profiled near-knee point, publish
   P99 reaches <=250 us. No locking change is authorized by these observations.
2. **Kernel I/O, timekeeping and Tokio scheduling.** Send/receive 13.42%,
   timekeeping 11.96%, Tokio top frames 8.70%. These remain substantial shared-host
   work; percentages are wall samples, not additive measured CPU savings.
3. **Allocation/copy and JSON/codec work.** At least 7.80% plus 2.55% visible
   top-of-stack samples. No full allocator attribution or optimization was done.

EventId OS entropy remains a smaller measured tax: 3.02% of non-park samples.
Its isolated removal is easy to measure; a reliable overall benefit meeting all
retention conditions was not established. Do not infer capacity from IDs/s.

## Reproduction and retained artifacts

- Build release workspace with `CARGO_INCREMENTAL=0 cargo build --release --locked`.
- Build the probe with `cargo build --release --locked -p netbaiot-protocol --example eventid_probe`.
- Freeze each server/probe binary before edits. Keep the same frozen loadgen for
  both phases. `scripts/perf/eventid_micro.py --binary <probe> --output <dir>
  --stress` runs direct timing, topology stress and subprocess overlap.
- `scripts/perf/eventid_campaign.py --server-bin <server> --loadgen-bin <loadgen>
  --output <dir>` runs the matrix. `eventid_profile.py` captures the direct
  generation profile. `eventid_summary.py <dir>` regenerates the summary.
- `eventid_paired.py --before <server> --after <server> --loadgen <loadgen>
  --output <dir>` performs the three alternating confirmation pairs.
- Raw JSON, profiles, frozen binaries, candidate source/patch and build/test logs
  remain locally in `target/eventid-experiment/` (ignored build artifacts).
  The report commits the meaningful measurements; it does not commit binaries.
- `scripts/perf/event_load.py` now also records absolute `server_cpu_seconds` from
  cumulative process CPU, without adding runtime instrumentation or metric labels.
- Full final-tree Rust/MQTT/restart gate logs are in
  `target/eventid-experiment/final-gates`. The final evidence commit is identified by
  Git history, with message `perf: evaluate event id generation cost`; no push.


## Checkout isolation

During final verification, unrelated README, tutorial, SDK example and guide
edits appeared in the shared checkout. Only this experiment's 13 known files were
moved into the managed `eventid-evidence/netbaiot` worktree, with content hashes
checked before removing/restoring this task's own original edits. All concurrent
work was preserved. The final local branch is `codex/eventid-generation-evidence`.
Production sections in all three changed Rust files were compared byte-for-byte
with starting HEAD; they match. Cargo.lock and dependency manifests also match.
The full gates were repeated against this isolated source tree to avoid mixing
concurrent edits. Ignored build/evidence artifacts remain linked to the original
checkout's `target/eventid-experiment` directory; they are not Git changes.


## Correctness gates (final isolated source)

| Gate | Result |
|---|---|
| Rust 1.88.0 `fmt --all -- --check` | PASS |
| Rust 1.88.0 locked workspace/all-target/all-feature Clippy, `-D warnings` | PASS |
| Rust 1.88.0 locked workspace/all-feature tests | 140 passed, 0 failed, 4 ignored |
| Stable 1.97.1 fmt | PASS |
| Stable locked workspace/all-target/all-feature Clippy, `-D warnings` | PASS |
| Stable locked workspace/all-feature tests | 140 passed, 0 failed, 4 ignored |
| MQTT core (`--netbaiot-only`) | 31/31 PASS |
| MQTT release (`--release-gate`) | 76/76 PASS; 125/125 normative requirements covered, external Mosquitto client/reference included |
| 60-second / 12-generation restart soak | PASS on targeted retry (62.05 s); zero missing required accepted IDs, exact replay identity retained; initial port-collision failure documented below |
| SIGKILL regression | PASS on both toolchains; intentional three-event in-memory loss window unchanged |
| Protocol/client/device-sdk/CLI compatibility | PASS in full workspace gates |
| Sink panic, required retry, accounting, slow-sink isolation, spool failure/replay | PASS in full workspace gates |
| Final probe/process helper smoke | 10,000 IDs over four threads plus 2,000 across two processes; zero duplicates |
| Python helper parsing and `git diff --check` | PASS |

The four default ignored tests are the 60-second restart soak, EventBus queue-depth
probe, MQTT recovery throughput benchmark and MQTT route-preflight benchmark.
The soak is run separately; the three unrelated manual benchmarks were not rerun.
No new parser/serde fuzz campaign, long-duration production soak, real fork test
or independent physical load host was used. The scoped network and collision
measurements above are the actual measurements performed.

The initial complete gate run in the shared checkout passed the 60-second,
12-generation restart soak (61.93 seconds, no missing accepted IDs). The isolated
repeat first failed **before initial readiness**: its saved configuration assigned
`device_http` and `tcp` both to `127.0.0.1:62345`. Existing `free_address` drops each
reservation before the next bind, allowing this fixture collision. No EventId was
accepted in that failed startup. The original failure log and address-only evidence
were retained; the runtime and fixture were not modified for this experiment.

The targeted isolated soak retry passed in 62.05 seconds (73.13 seconds including
its build), with all 12 generations and exact accepted/replayed ID checks intact.
Missing required accepted IDs = 0. `restart-soak-retry.log` and `soak-retry.json`
record the pass separately; `results.json` retains the original startup failure
rather than overwriting history. The SIGKILL test still demonstrates the intentional
non-spooled memory loss boundary; this work adds no abrupt-crash durability.
