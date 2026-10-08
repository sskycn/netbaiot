# Engineering optimization (0.2.3 baseline)

This staged change preserves EventAccepted, required delivery ownership, MQTT wire
behavior, NBSP v3 and NBMQ v6 recovery, and the database-free bounded runtime.

## Phase 1: portable, safe release archives

The POSIX manifest/link fix in `7df10ba` is retained. Native developer CI now builds
release binaries and executes `cargo xtask package --no-build --target <host>` on
Windows, Linux and macOS before a tag is created. Validation rejects backslashes
and noncanonical member names, so Windows extraction cannot reinterpret a member
as traversal and aliases cannot evade duplicate-member checks.

Tests cover nested/percent-encoded links, relative parents, POSIX and Windows
manifest paths, absolute members, traversal, symlinks and duplicate members.
PASS: 13 release-tool tests, `cargo xtask check` (format, Clippy, workspace tests,
evidence check), and native macOS `package --no-build`. Native Windows/Linux
full package jobs will be verified after pushing the staged changes.

## Phase 2: incremental required responsibilities

`State.pending_required` counts sink responsibilities, not events. Publish and
whole-batch restore preflight checked additions before commit. Only removal of a
present required sink on a terminal ACK decrements it. Retries, permanent errors,
timeouts, panic, cancellation and spool snapshots retain ownership. Duplicate or
unrelated completions cannot alter inflight or byte accounting.

The test-only `assert_eventbus_invariants` recomputes required responsibilities,
active bytes, per-sink count/bytes and ready/delayed/inflight accounting. Existing
admission, restore, duplicate, retry, panic, isolation and spool tests invoke it.
PASS: 30 EventBus tests and `cargo xtask check`. The serial release benchmark uses identical before/after
workloads at 1/64/256/1024/4096/16384 active events. Median usage latency (ns) was
42/125/250/875/4667/20334 before and 41/41/41/41/41/41 after. These are isolated
subsystem measurements on this macOS host, not capacity or SLA claims. Publish,
complete, retry and restore allocation/timing rows are retained in the final
measurement summary; their costs remain visible rather than being inferred from
the usage improvement.

## Phase 3: borrowed HTTP envelope

The private `WebhookEnvelope` borrows IDs and `DeviceEventKind`; timestamps and
the event type are copied scalars. The request serializes once into reqwest's
body. No event-lifetime JSON cache or additional payload tree is retained.
Exact JSON-value tests cover every event kind, null and populated occurrence
timestamps, UUIDs, numeric/boolean/text telemetry and escaped Unicode. Object
member ordering is not an API contract; all nine fields and nested values match
the old serializer.

The same reqwest request-construction benchmark compares the retained legacy
`json!` implementation and the borrowed envelope (512 samples, three repeats):

| Payload bytes | Old/new median ns | Old/new allocations | Old/new allocated bytes |
| --- | --- | --- | --- |
| 32 | 2292 / 1125 | 37 / 13 | 3451 / 1327 |
| 1024 | 2708 / 1542 | 38 / 15 | 6271 / 3405 |
| 16384 | 8334 / 6416 | 38 / 15 | 52351 / 34125 |
| 65536 | 20625 / 17584 | 38 / 15 | 199807 / 132429 |

This excludes network time and does not imply the same end-to-end improvement.
PASS: JSON compatibility tests, the release benchmark and `cargo xtask check`.

## Phase 4: failure diagnostics, bounded retry hints and pause

The original `SinkError::{Retryable,Permanent}` and `EventSink::deliver` remain
source-compatible. An additive, defaulted `deliver_detailed` hook and sanitized
`SinkFailure` metadata are necessary for the server crate to report retry hints
and a closed reason vocabulary to the runtime without another shared mutex.
Reasons distinguish network, timeout, 429, 5xx, auth, other 4xx, malformed/oversized
responses, panic and unspecified custom-sink errors. No secrets or remote error
bodies enter diagnostics.

429/503 delta-seconds Retry-After is parsed with a checked u64 conversion and
clamped to `retry_max_ms` in both HttpSink and EventBus. Invalid values are ignored;
HTTP-date support was evaluated but not implemented. Local backoff still provides
a nonzero minimum. Redirects and the 4096-byte response-body ceiling remain.

Each required sink pauses after a permanent failure or three consecutive failures,
using the existing local maximum retry interval. At expiry, one recovery probe is
allowed; ACK clears the pause. Previously started requests complete normally.
Nothing releases count, bytes or required ownership until ACK. No new task, lock,
background timer or per-event breaker is introduced. Shutdown still drains or
spools accepted required work, even while a sink is paused.

A current inflight operation token rejects duplicate/stale completions, including
saturated persisted attempt counts. The token is node-local and transient; no
spool format changes are needed. Inflight token entries are bounded by existing
delivery concurrency. A count-bounded timestamp bucket index per sink provides
oldest age without scanning active events on metrics/status reads.

Administrative status adds bounded per-sink diagnostics. Prometheus exposes
aggregate queue/count/bytes/inflight/pause/age/hint-use values and a fixed reason
vocabulary. Sink/device/event IDs and URLs are never metric labels.

Fixtures exercise 200, 500, 429/503 with hints, 401/403/404, deadline, refused
connection, invalid response, declared and chunked oversized bodies, and recovery
to 200. Tests verify spool ownership, stable event IDs, cleanup, one probe,
duplicate completion protection and nonzero drain reporting during an outage.
PASS: 32 EventBus tests, HTTP classification/outage tests and `cargo xtask check`.
