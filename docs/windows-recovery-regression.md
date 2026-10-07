# Windows recovery regression investigation

Date: 2026-10-07. Original main: `1bd5cf8e15293bbed0ea8f80929f5405cf5a2751`.
Optimization baseline: `a91a9103f13a32058c35c177bb4765026c277040`.
Windows measurements used GitHub Actions `windows-latest`, Rust 1.88.0.
Local validation used macOS, Rust 1.99.0.

## Root Cause

### Authentication/presence assertion: confirmed test lifecycle defect

The test publishes a second required event and deliberately stops calling
`events.recv()` while authenticating device `four`. That authentication succeeds,
but the test then leaves the event in the SDK's one-item receive queue while
forcing a revision-gap reconnect. The required event is replayed into that full
queue. The SDK correctly reports `Overloaded`, closes the connection, and
reconnects. Each replacement provider's reset sync revokes existing authorization
and MQTT sessions, including the session that the test has just recovered.

The test also retains older reconnecting MQTT SDK clients with the same
authenticated device and ClientId `rpc-one`. These clients repeatedly take over
the connection whose lifetime the assertion assumes is stable.

Captured main failure sequence:

1. Provider epoch 1 applies revision 3, then the intentional revision gap revokes
   authorization and disconnects device `one`.
2. Provider epoch 2 serves revision 5. MQTT generations 7, 9, 10, 12, 13, 14, 15,
   and 16 register or replace each other.
3. The unacknowledged event fails another delivery, and provider epoch 3 starts a
   reset sync. Generation 16 is cancelled and its current lease drops.
4. The management query observes `live_exists=false`, `presence_exists=true`,
   `presence_connected=false`, and no session generation. The SDK reports
   `provider_ready=false` and `device_connected=false`.
5. The query finishes only 3.7258ms after initiating shutdown, well inside the
   1,500ms grace interval. This is actual session loss following reset sync,
   not presence TTL pruning or stale-generation cleanup of a newer session.

The same failure occurs on the optimization baseline: 3/50 repetitions. The
immutable auth binding, registration/invalidation fence, presence semantics,
and generation checks remain unchanged.

### Restart replay: demonstrated deadline-policy mismatch; original trace limited

The original replay timeout at `receiver.recv()` was not directly reproduced
in the subsequent stage comparisons. The original CI log records only
`Elapsed(())`, so its precise attempts/deadlines cannot be reconstructed.
Do not present the following deterministic counterexample as a captured trace
of that original failure.

A restored record carries its historical attempt count. EventBus starts its
delivery timeout while Business RPC is still waiting for a subscriber. With
the fixture's 300ms sink timeout, a legal record on its final normal attempt can
time out before subscriber registration. The required record then enters the
configured 30-second backoff. RPC subscriber readiness does not reset that
deadline: after a further five seconds the receive queue is still empty, while
the required record and its event_id remain owned. It delivers after backoff
and releases accounting only after the application's ACK.

Virtual-clock tests demonstrate this sequence exactly, and demonstrate that
the existing normal 5-second sink budget delivers to a subscriber arriving
after three seconds without retry. Thus a short shutdown-oriented fixture
deadline cannot guarantee replay within five seconds of subscriber readiness.
This is an invalid test budget assumption, not demonstrated record loss or a
lost Notify wakeup.

A separate startup defect was observed during the stage sweep: reserving a
TCP ephemeral port alone does not prove it is available for UDP on Windows.
The V2 fixtures now use the repository's existing UDP-first TCP/UDP pair
reservation helper. This addresses first-subscription startup failures; those
failures must not be counted as the original post-restart replay timeout.

## First Bad Commit

No deterministic bad optimization commit was established. In particular,
baseline `a91a910` and main `1bd5cf8` both have 3/50 authentication assertion
failures in the extended comparison. The first sweep's 2/40 failures on
`302912c` do not establish it as the introducing commit.

The two integration test sources are identical throughout the optimization
range before this fix. The initial sweep uses unmodified historical trees.
The extended comparison adds output-only diagnostics, preserving each
historical presence and queue implementation; its probe is pinned to the
pre-fix investigation commit.

### Unmodified Windows stage sweep

Each target starts with ten repetitions; a failing group expands to forty.
Durations exclude the initial build and sum test command elapsed time.
The last column is serial V2 suite / default parallel V2 suite / workspace.

| Commit | Auth pass/fail | Rate | Seconds | Replay pass/fail | Rate | Seconds | Suites |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| a91a910 | 10/0 | 0% | 25.391 | 10/0 | 0% | 14.203 | PASS/PASS/PASS |
| 5d612af | 10/0 | 0% | 33.891 | 21/19 | 47.5% | 129.844 | PASS/PASS/PASS |
| 41eb71f | 10/0 | 0% | 24.156 | 10/0 | 0% | 14.907 | PASS/PASS/PASS |
| 6b559ac | 10/0 | 0% | 33.453 | 10/0 | 0% | 14.094 | PASS/PASS/PASS |
| 4ac37d7 | 10/0 | 0% | 22.218 | 10/0 | 0% | 14.532 | PASS/PASS/PASS |
| 355b974 | 10/0 | 0% | 26.641 | 10/0 | 0% | 13.375 | PASS/PASS/FAIL |
| 7b4e3c4 | 10/0 | 0% | 24.203 | 10/0 | 0% | 13.672 | PASS/PASS/PASS |
| 302912c | 38/2 | 5% | 93.937 | 10/0 | 0% | 14.313 | PASS/PASS/PASS |
| 1bd5cf8 | 10/0 | 0% | 24.749 | 10/0 | 0% | 13.813 | PASS/PASS/FAIL |

The 19 failures at `5d612af` occur at the initial V1 subscription, before replay.
The `355b974` workspace failure is the independent CLI demo startup
`RecvError`, not either requested assertion. Main's workspace failure is the
requested authentication assertion.
[Stage sweep](https://github.com/sskycn/netbaiot/actions/runs/37554424834).

### Extended Windows comparison, all-features, output-only diagnostics

| Commit | Auth pass/fail | Rate | Seconds | Replay pass/fail | Rate | Seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| a91a910 | 47/3 | 6% | 115.171 | 50/0 | 0% | 72.610 |
| 5d612af | 49/1 | 2% | 118.453 | 50/0 | 0% | 77.047 |
| 7b4e3c4 | 50/0 | 0% | 121.938 | 50/0 | 0% | 77.126 |
| 302912c | 49/1 | 2% | 122.047 | 50/0 | 0% | 79.688 |
| 1bd5cf8 | 47/3 | 6% | 118.000 | 50/0 | 0% | 82.375 |

[Extended comparison and state traces](https://github.com/sskycn/netbaiot/actions/runs/37556748762).
Ten default parallel suites passed. Forty all-features parallel suites produced
38 passes and two failures before the fix.
[Parallel comparison](https://github.com/sskycn/netbaiot/actions/runs/37556423125).

## Fix

- Complete the no-recv authentication check, then explicitly receive/ACK the
  second event and observe required-work cleanup before forcing a reconnect.
  Observation uses a bounded 10ms cadence and the existing five-second deadline.
- Drop retired MQTT SDK clients before starting the replacement generation,
  preventing background reconnection from taking over its ClientId.
- Keep 300ms shutdown drain and the original five-second receive assertions.
  Restore the normal 5,000ms delivery budget for the V2 restart phase instead
  of inheriting the V1 fixture's 300ms timeout. Runtime defaults, attempt counts,
  retry delays, and historical attempt metadata are unchanged.
- Reserve a port valid for both TCP and UDP in all seven V2 fixtures.
- Retain bounded failure-tail capture and credential-free session/provider,
  attempt, routing-revision, and event_id diagnostics.

No Windows ignore, skipped target, increased retry policy, continue-on-error,
or runtime queue/presence algorithm rollback was introduced.

## Regression Test

- A real multiplexed RPC replay into a full receive queue reports Overloaded,
  closes that connection, and preserves the older application-owned delivery.
- An invalidated old session lease cannot remove replacement presence or its
  generation; active presence survives an expired last_seen and a UDP touch.
- Restored work delivers before worker startup and after a worker has parked,
  including a late business sink claim and notification before delivery polling.
  Its id/revision/attempt metadata and required ownership survive until ACK.
- Virtual-clock tests prove short-deadline backoff retains the required event
  after subscriber readiness, and that the normal budget admits the late
  subscriber without retry. Neither test depends on scheduler sleeps.

## Performance

Existing touch, presence lookup, and connection lookup retain average O(1).
RateLimiter's existing-IP path retains average O(1). Ready dequeue remains
VecDeque pop_front with the delayed BTreeMap intact; no full-queue min/remove
scan was restored. MQTT Bytes payload sharing and logical ownership quotas
remain unchanged.

The existing ready/retry release benchmark passed all depths 0/100/1000/4096
and retry mixes 0/10/50%. This is a local microbenchmark verification, not a
production capacity measurement. No new performance optimization was attempted.

## Validation

PASS on the final Rust implementation `968fe5a`:

- cargo fmt --all -- --check
- cargo clippy --workspace --all-targets --all-features -- -D warnings
- cargo test --locked --workspace --all-features
- cargo xtask check
- cargo test --locked -p netbaiot-runtime spool::
- cargo test --locked -p netbaiot-transports mqtt_recovery
- cargo test --locked -p netbaiot-server --test business_rpc_v2
- cargo test --release --locked -p netbaiot-runtime ready_retry_queue_scaling
  -- --ignored --nocapture --test-threads=1

Windows post-fix: authentication 30/30 (73.844s), replay 30/30 (59.046s).
Serial V2 suite PASS (42.672s), default parallel PASS (22.469s), workspace
PASS (220.141s).
[Post-fix repetitions](https://github.com/sskycn/netbaiot/actions/runs/37561300990).

Native recovery and lifecycle: Windows PASS, Ubuntu PASS, macOS PASS.
[Three-platform validation](https://github.com/sskycn/netbaiot/actions/runs/37561300959).
Rust checks, including MSRV/stable and MQTT reference-client verification, PASS.
[Rust validation](https://github.com/sskycn/netbaiot/actions/runs/37561301529).

FAIL: historical pre-fix failures above, plus an intermediate ACK-observation
polling failure fixed by bounding its cadence. The first sweep attempt failed
to check out short SHAs and is excluded; the measured sweep uses full SHAs.

BLOCKED: no required validation. Additional long-running fuzz/load/soak and
connection-memory measurements were not run; decoder/transport algorithms were
not changed. Exact attribution of the original replay Elapsed to the demonstrated
budget mechanism remains unproven, as explicitly noted above.

Manual stage-sweep and thirty-repeat workflows are retained for reproducibility.
Temporary diagnostic overlays and automatic investigation workflows are removed.
