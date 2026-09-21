# Correctness baseline freeze

Baseline SHA: `564583edb5e805ba8d626ed7b774f7cce55c87b7`.

Final SHA: the local commit containing this report; the exact content-derived SHA is
recorded in the task result because a commit cannot contain its own hash.

## Decision

**Correctness Baseline: FROZEN.** No P0 defect remains. All six confirmed P1
findings and all three confirmed P2 findings in this final pass are fixed. Protocol
functionality, resource pressure, restart/recovery integrity, persisted
authorization, and external interoperability each pass their mandatory local gate.

## Final finding ledger

| Finding | Severity | Confirmed | Root cause | Fix | Deterministic evidence | Result / remaining limitation |
|---|---|---:|---|---|---|---|
| Accepted Will lost under subscriber pressure | P1 | yes | abnormal-disconnect publication treated an already accepted Will like a rejectable producer publish | CONNECT reserves bounded Will responsibility; failed atomic route moves it to broker-owned pending state, retried once on capacity change and recovered on planned restart | `WILL-SUBSCRIBER-PRESSURE-001`, `will_pending_restart_001`, existing DISCONNECT tests | PASS; responsibility is bounded and exactly one logical publication settles it |
| Revoke/attach race | P1 | yes | `MqttBroker::attach` occurred after the auth registration gate was released | one `auth_registration` boundary now covers candidate freshness, `Sessions::register`, and attach; invalidation covers cache, live sessions, and MQTT state under the same gate | `AUTH-MQTT-ATTACH-REVOKE-RACE-001` covers device, credential version, auth generation, and all | PASS; global epoch may conservatively reject an unrelated stale candidate |
| Large NBMQ v1 compatibility | P1 | yes | the smaller current ceiling was checked before the file version was read | read the fixed prefix first, then apply the exact legacy v1 ceiling or current v2/v3 ceiling | `MQTT-RECOVERY-V1-LARGE-COMPAT-001` uses a prior-format encoder and a v1 file larger than a lowered current-format test ceiling | PASS; legacy v1 decoding may allocate its bounded JSON payload |
| Whole-image recovery integrity | P1 | yes | v2 had per-record hashes but no authoritative manifest | write NBMQ v3 with deterministic record order and a final count, byte count, and SHA-256 stream digest; v1/v2 remain read-only | `MQTT-RECOVERY-WHOLE-IMAGE-INTEGRITY-001`, v2 compatibility and v3 round-trip tests | PASS; old v2 cannot gain v3 whole-image guarantees and is reset conservatively when provenance is incomplete |
| Codec provenance omitted | P1 | yes | stored MQTT transactions recorded credential provenance but not decoding semantics | session profile now includes `codec_id` and `codec_version`; either mismatch resets state and returns Session Present=0 | `MQTT-PERSISTENT-CODEC-PROVENANCE-001` | PASS for ID change, version change, and exact-match resume |
| Route preflight amplification | P1 | yes | every match deep-cloned `StoredSession`, including payloads, and repeated global scans | one global accounting pass plus compact per-target projected deltas and read-only packet-ID selection; commit still occurs atomically under one broker lock | `MQTT-ROUTE-PREFLIGHT-BOUNDED-MEMORY-001` and manual 100/1,000/2,000-target benchmark | PASS; planning is O(all bounded sessions + matches), not matches × all sessions |
| Recovery ACL/ownership gaps | P2 | yes | recovered state was structurally valid but some topic/session combinations were impossible under the identity profile | restore shares canonical live ACL helpers and checks subscription, offline, outbound, inbound QoS2, retained, and pending-Will ownership; incomplete legacy profiles are not indexed | `MQTT-RECOVERY-ACL-OWNERSHIP-001` | PASS; legacy state without full provenance resets at authenticated attach |
| Mixed invalidation counts | P2 | yes | active disconnects and removed persistent MQTT sessions were summed | additive fields report cache entries, network connections, and MQTT sessions separately; legacy `disconnected` remains connection-only | `AUTH-INVALIDATION-COUNTING-001` | PASS; old clients tolerate additive response fields |
| External release CI absent | P2 | yes | normal CI intentionally exercised only the fast NetbaIoT core | `.github/workflows/mqtt-interop.yml` installs Mosquitto only for testing and runs the complete release gate on dispatch, MQTT-relevant main/tag changes, and releases | local full gate plus workflow inspection | PASS locally; the new remote workflow cannot run until this unpushed commit is pushed |

## Ownership and ordering invariants

Will lifecycle is explicit: CONNECT acceptance arms and reserves bounded
responsibility; MQTT DISCONNECT suppresses it; abnormal disconnect transfers it to
broker-owned pending publication; overload preserves it without partial fanout;
capacity change retries it; successful atomic publication settles it; planned
restart persists and restores it. No task-per-Will or retry loop exists.

Auth lock order is:

```text
auth_registration gate -> AuthCache -> Sessions -> caller finalizer/MqttBroker
```

Attach and invalidation both follow this direction. No reverse acquisition path was
found. MQTT attach cannot recreate state from a matching candidate after
invalidation completes.

Route preflight retains atomic all-target behavior while holding no payload clones.
It builds tenant/global usage once, stores one compact decision per match, inspects
only each target's bounded packet-ID set, then commits retained and target mutations.
A closed active receiver leaves QoS responsibility stored for reconnect.

## NBMQ v3

NBMQ v3 retains the checksummed header and per-record framing/hash and adds a fixed
52-byte `NEND` trailer with authoritative record count, total record bytes, and an
incremental SHA-256 digest over the header and complete ordered record stream. The
reader verifies all three values. Removing an offline, outbound, or inbound QoS2
record, truncating at a record boundary, reordering records, or appending a complete
unknown record is rejected.

The legacy v1 read ceiling is `1,342,177,280` bytes. The current v2/v3 read/write
ceiling is `202,178,660` bytes under default limits. Record length is checked before
allocation and the largest temporary record buffer measured is 67,072 bytes.
Production planned restart streams v3 directly; `snapshot()` remains only a bounded
test/legacy helper. Temp-file mode 0600, file fsync, atomic rename, and directory
fsync remain unchanged, so a failed new write does not replace the last committed
image.

## Measurements

Route planning with 1,000 sessions each holding 8 KiB of offline data retained more
than 8 MiB of stored payload while its temporary plan remained below 512 KiB and
below 1/16 of stored payload. The explicit benchmark was:

| Matching targets | Planning time | Temporary plan bytes |
|---:|---:|---:|
| 100 | 500 us | 10,090 |
| 1,000 | 1,979 us | 101,890 |
| configured maximum 2,000 | 4,126 us | 204,890 |

Planning occurs under the broker mutex, so the planning time approximates the
measured lock-held planning phase.

The NBMQ v3 streaming benchmark was:

| Logical payload | File bytes | Encode | Decode |
|---:|---:|---:|---:|
| 10 MiB / 10,485,760 B | 10,498,525 | 580 ms | 572 ms |
| 50 MiB / 52,428,800 B | 52,492,592 | 2,882 ms | 2,908 ms |
| 100 MiB / 104,857,600 B | 104,985,942 | 5,951 ms | 5,913 ms |

`/usr/bin/time -l` reported 236,224,512 bytes maximum RSS for the complete debug
benchmark process and 40,059,384 peak memory footprint; that process holds source
and restored brokers simultaneously and is not the 67,072-byte codec record buffer.

## Validation

- Rust 1.88.0: fmt PASS, clippy `-D warnings` PASS, workspace tests PASS (132
  executed, 3 ignored; both manual benchmarks were run separately and passed).
- Rust stable 1.97.1: fmt PASS, clippy `-D warnings` PASS, workspace tests PASS
  (132 executed, 3 ignored).
- Raw MQTT: 31/31 PASS.
- Mosquitto broker differential: 11/11 PASS.
- Mosquitto MQTT 3.1.1 client matrix: PASS.
- Verified TLS matrix: PASS.
- Full release gate: 76/76 PASS; normative mapping 125/125 PASS.
- Fuzz: `mqtt_packet`, `mqtt_state`, and `restart_spool` 1,000 runs each;
  `mqtt_recovery` 10,000 runs, including v1/v3 framing and v3 trailer corruption
  paths; no crash. macOS emitted non-fatal external-symbolizer warnings.

## Mosquitto 2.0.18 persistent unsubscribe interoperability remediation

The first remote external run exposed a harness compatibility defect, not a broker
state defect. Mosquitto 2.0.18 rejects an invocation containing `-U` without any
`-t`, while the newer local 2.1.2 client accepts it. The old harness ignored the
2.0.18 nonzero exit and then re-subscribed the target topic during verification,
mixing session resume with a new subscription.

`PERSISTENT-UNSUB-RECONNECT-001` now proves at raw packet level that Session Present
is one, exact UNSUBACK is received, no post-unsubscribe offline message is queued,
and only a fresh post-resubscribe publication is delivered. The test-only broker
unit regression additionally verifies that the filter is absent from both
`StoredSession` and `SubscriptionTrie`. `MOSQUITTO-PERSISTENT-UNSUB-001` uses
a different authorized topic to satisfy both 2.0.x and 2.1.x parsers, observes
`UNSUBACK` in the debug trace as the correctness boundary, and checks the exact
forbidden payload rather than requiring empty stdout. It passes with isolated
official 2.0.18 clients and local 2.1.2 clients. No production broker code changed;
the next remote Mosquitto 2.0.18 workflow is expected to pass for this protocol
reason.

## Remaining risks

The broker remains intentionally memory-first between planned snapshots: SIGKILL,
OS crash, power loss, or hardware loss can discard recent state. Business delivery
is at-least-once. Legacy v1 read compatibility has a hard but comparatively large
1.25 GiB ceiling. Planned v3 commit and route commit use the single broker mutex;
the measured bounded scans remain a future performance topic, not a correctness
gap. The corrected external workflow has not yet been rerun remotely because this
commit is intentionally unpushed. MQTT 5, clustering, shared subscriptions,
bridges, crash-durable
exactly-once delivery, a runtime database, and an external runtime broker remain
outside the claimed profile.
