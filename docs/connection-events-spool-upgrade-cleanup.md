# Connection event cleanup and restart spool upgrade diagnostics

Baseline: `6595ebe6775ed630d691ca93f3551278525387b0` (local `main`, initially clean).

## Scope and producer inventory

The baseline's only production references to the removed types were the public
protocol definitions/conversion, the JSON codec's explicit rejection arm, and CLI
filter parsing. No MQTT, TCP, UDP, session registration, or disconnect path produced
these business events. Historical performance JSON uses `connected` and
`disconnected` counters; those are load-generator connection statistics and remain.

Removed `DeviceConnected`, `DeviceDisconnected`,
`DeviceEventKind::Connected`, `DeviceEventKind::Disconnected`,
`EventType::Connected`, and `EventType::Disconnected`. The client inherits the
public protocol types, so its filter surface shrinks without a separate client
implementation change. CLI `events subscribe --type connected|disconnected` now
returns the existing unknown-event-type usage error. Public serde event types,
subscription filters, normalized event kinds, and device JSON uplinks reject those
strings. No synthetic event producer was added.

The four remaining normalized variants have actual JSON codec producer contracts:
`Telemetry`, `DeviceEvent` (uplink `event`), `Heartbeat`, and `CommandAck`.
Management `Sessions`, `Presence`, `ConnectionSummary`, `DeviceConnectionInfo`,
connection query routes, MQTT/TCP session generations and replacement fences, and
UDP `last_seen` observations are unchanged. SDK connection state, MQTT sessions,
Will behavior, recovery snapshots, and connection admission caps are unchanged.

## Compatibility and bounded diagnosis

This is a breaking Rust/public business-event enum and subscription-filter change.
Supported MQTT/TCP/UDP wire transports, device JSON v1 kinds, `DeviceCommand`, and
`CommandAck` keep their existing encodings and semantics. No wire version is bumped
because no supported device upload or stream framing changes. Consumers that
reference the removed Rust variants or filter strings must remove them before
upgrading. Previously serialized lifecycle events, if created externally, are now
invalid; the gateway had no producer for them.

EventBus NBSP framing versions remain v1 (append-only) and v2 (authoritative
snapshot with generation). They version the container, not the embedded event
schema independently. Supported historical and current records remain readable;
this task does not redesign the spool or change independent NBMQ compatibility.

For each selected recovery record the existing length, count, and SHA-256 checks
run first. Only a failed current `SpoolRecord` decode invokes the diagnostic. An
explicit 64-level nesting guard precedes a small serde projection of exactly
`event.kind.kind`; unknown fields are skipped, not materialized into a JSON tree.
This uses the existing record byte ceiling and builds no old domain object.
Malformed JSON, duplicate discriminator fields, wrong paths, unknown kinds, corrupt
checksums/framing, unknown container versions, and oversized records retain their
normal invalid/overload behavior. Diagnostics with excessive nesting are invalid,
not a claim of legacy compatibility.

A known removed discriminator returns `Error::IncompatibleSpool`. The server logs
its static Display message and returns before listener binding or `mark_running`:

```text
EventBus restart recovery failed; startup blocked error=restart spool contains legacy ConfigAck records created by an older NetbaIoT version; drain or complete the old spool with the previous release before upgrading; committed files are preserved
```

No record contents, credentials, device IDs, or application payloads appear in this
message. Recovery never skips, converts, deletes, or overwrites the incompatible
work. A subsequent commit also refuses to replace the incompatible snapshot.

## Upgrade procedure

Use the [operator procedure](restart-spool.md#legacy-configack-restart-spool-compatibility):
restore the previous binary with its original recovery directory and consumers,
stop new device traffic externally, wait for required replay acknowledgements and
`pending_required=0`, and verify the gateway removes committed EventBus spool files.
Then run the previous release's `netbaiot server drain --yes` (or SIGTERM), verify
successful exit with no EventBus spool remaining, and upgrade. A graceful exit alone
can leave pending work safely spooled and is not enough to clear this prerequisite.
Never delete the spool to bypass startup refusal; retain independent MQTT state.

## Regression evidence

Historical fixtures are generated with actual Rust protocol and SpoolRecord
serializers from `8ec59f37530659d65fe4ea398aba831b156d1d6b`, before ConfigAck removal.
See [fixture source and regeneration](../tests/fixtures/restart-spool/README.md).
Both NBSP versions cover legacy ConfigAck refusal and still-supported heartbeat,
telemetry, and CommandAck records with stable event IDs, sink IDs, revisions, and
retry attempts. Runtime tests compare file bytes after failure and attempted commit.
A real server subprocess verifies the diagnostic, failed exit, and unchanged file;
the composition-root test proves failure occurs before listener binding.

The official client integration covers management MQTT connect/disconnect, TCP
connect/disconnect/reconnect with increasing generations, online commands and their
normal CommandAck event path, offline command errors, and an unfiltered stream with
no synthetic lifecycle event. Existing session replacement, UDP reliable receipt,
MQTT protocol/recovery, management authorization, slow-sink, restart, and corruption
tests remain part of the regression run.

All commands below ran with Rust 1.88.0 except cargo-fuzz, which used the installed
nightly toolchain and cached dependencies. Both complete Rust suites passed
**180 tests, 0 failures, 4 ignored**. The ignored restart soak was selected separately
and passed; the remaining three are the manual queue-depth, MQTT recovery size, and
MQTT route-preflight performance probes. The all-targets command also executes the
existing foundation benchmark in debug mode; those timings are not capacity evidence.

| Command | Result |
| --- | --- |
| `cargo +1.88.0 fmt --all -- --check` | PASS |
| `cargo +1.88.0 check --workspace --all-targets` | PASS |
| `cargo +1.88.0 clippy --workspace --all-targets --all-features -- -D warnings` | PASS |
| `cargo +1.88.0 test --workspace --all-targets --all-features` | 180 passed, 0 failed, 4 ignored |
| `cargo +1.88.0 test --workspace --all-features` | 180 passed, 0 failed, 4 ignored; doc tests pass |
| `cargo +1.88.0 test -p netbaiot-runtime spool::tests` | 9 passed |
| `cargo +1.88.0 test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture` | 1 passed, 62.71 seconds |
| `RUSTUP_TOOLCHAIN=1.88.0 python3 tests/mqtt_conformance/run.py --release-gate` | 76/76 checks, 125/125 normative requirements; includes raw conformance, Mosquitto differential and external clients |
| `RUSTUP_TOOLCHAIN=1.88.0 bash scripts/tutorial_smoke.sh` | PASS |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run restart_spool /tmp/netbaiot-connection-cleanup/spool-corpus -- -max_total_time=30 -max_len=1048576` | 1,101,361 executions, 31 seconds, no failure |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run json_codec -- -max_total_time=30 -max_len=65537` | 3,679,339 executions, 31 seconds, no failure |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run device_classifier -- -max_total_time=30 -max_len=65537` | 520,866 executions, 31 seconds, no failure |
| `CARGO_NET_OFFLINE=true cargo +nightly fuzz run udp_envelope -- -max_total_time=30 -max_len=1201` | 352,936 executions, 31 seconds, no failure |

The complete suites include 10 UDP ACK unit/real-socket tests, 37 MQTT broker tests,
management HTTP and connection-cap isolation, slow-required-sink isolation, spool
failure repair, SIGKILL loss semantics, and subprocess QoS1/QoS2 restart recovery.
The spool fuzz corpus includes all four historical fixtures plus valid-checksum
unknown-kind and escaped-string diagnostic cases. Fuzz smoke is not a security proof.

An initial check/clippy attempt found a new test calling `clone()` on non-Clone
`Config`; the test now reconstructs its owned input via serde. The first MQTT gate
attempt exited before its vectors with server `Error::Unavailable` during listener
startup. A clean rerun of the same command passed all 76 checks. The harness releases
ephemeral TCP port reservations before startup and does not reserve matching UDP;
a transient bind conflict is plausible but was not independently proven.

No separate-host capacity test, large mixed-protocol matrix, multi-hour soak,
new power-loss experiment, or standalone idle-connection memory campaign was run.
This cleanup adds no ingress hot-path work; the compatibility projection runs only
on startup/commit recovery decode failure.

## Short regression comparison

Same-host loopback; three paired 10-second measurement windows per protocol/build,
2-second warmup, 1-second ramp, 256-byte payloads, required AuditSink, plaintext
streams, ten server workers and the same frozen four-worker load generator.
MQTT/TCP offer 20,000 events/s with 32 clients; UDP offers 30,000/s with eight
workers. Default ingress admission bounds remain in place. Baseline/candidate runs
are interleaved and order alternates by repeat. No tests, builds, or fuzz campaigns
ran concurrently with these measurements.

| Protocol | Baseline accepted/s | Candidate accepted/s | Delta | p99 ms baseline → candidate |
| --- | ---: | ---: | ---: | --- |
| MQTT | 19,908.4 | 19,591.3 | -1.59% | 0.86 → 9.01 |
| TCP | 19,902.8 | 19,900.3 | -0.013% | 1.31 → 1.43 |
| UDP | 29,999.9 | 29,999.8 | -0.0003% | 1.48 → 0.87 |

These are medians of run metrics, not pooled percentiles. Admission pressure causes
stream disconnects/unconfirmed work; UDP can shed scheduled sends at its bounded
client window. Successful-event latency excludes unconfirmed work. All 18 runs
ended with clean server exit and no pending EventBus spool; none is a loss-free or
production capacity claim.

Initial MQTT accepted-rate ranges were 19,855.7–19,911.5/s (baseline) and
19,434.8–19,931.1/s (candidate). Its p99 ranges were 0.79–6.96 ms and 1.62–10.91 ms.
The initial median is worse despite overlapping ranges, so it is not reported as
an improvement or erased as noise. A second set of three identical MQTT pairs was
selected to check whether this difference reproduced, without changing production
code, limits, offered rate, clients, window, or binaries.

The confirmation medians were **19,925.8 → 19,932.8 accepted/s (+0.035%)** and
**1.67 → 1.26 ms p99**. Baseline accepted-rate range: 19,924.6–19,936.2/s;
candidate: 19,909.6–19,943.7/s. All six confirmation runs also exited gracefully
without pending EventBus spool. The initial MQTT difference did not reproduce;
these short measurements do not establish a sustained throughput regression or a
capacity improvement. Both batches remain in the evidence file.

## Code surface

Implementation commit: `a93b7eee24ed8ac94a8abfefda0d6756dc10ea64`.
The implementation/migration/fixture commit changes **24 files, +562 / -64 lines**,
including **9 Rust files, +342 / -51 lines**. Four binary spool fixtures are included
in the file count but have no text line count. The final report and JSON evidence
are two additional files. Total task diff: **26 files, +2,144 / -64 lines**.

## Artifact evidence

Machine-readable [validation and measurement results](connection-events-spool-upgrade-results.json)
record binary hashes, validation log hashes, fuzz execution counts, and the short
comparison. The release server changes from **8,375,536** to **8,377,600 bytes**
(+2,064 bytes); this includes the new migration diagnostic. Binary size is not a
runtime memory or capacity measurement.

Full local command logs are retained in `target/connection-events-spool-upgrade/validation/`
(ignored build artifacts); their hashes are recorded in the JSON evidence.

## Residual surface audit

Production Rust contains no removed connection event symbols. Occurrences in this
migration report and `remove-device-config.md` describe historical API removal.
ConfigAck references are confined to the failure diagnostic, tests/fixtures,
migration docs, and historical reports. No current domain variant or successful
runtime delivery path accepts ConfigAck.

## Known limitations

- NetbaIoT does not emit durable online/offline DeviceEvents.
- Connection state remains runtime/control-plane state.
- Business systems needing durable presence history must own it.
- Legacy ConfigAck restart spool must be drained by an older release before upgrade.
- Abrupt crash may lose bounded in-memory work; restart spooling is planned-restart
  recovery, not a crash-durable event store.

## Decision and completion

**KEEP.** Removed event kinds have no production producer, protocol/filter rejection
is explicit, presence and command contracts pass, recovery fails closed with a
specific migration diagnostic and intact files, supported records still recover,
and required validation including restart soak is green. The initial MQTT timing
difference did not reproduce in the identical confirmation batch; limitations and
both sets of measurements are retained above.

The task is committed and merged into local `main`; no reset or push is performed.
The final Git SHA is reported in the task response. The merged task branch is
removed, and no additional worktree was created.
