# Correctness baseline freeze

Baseline SHA: `953f428d6398d473f8c265f3a204a4ee6295bd35`.

Final SHA: the local commit containing this report; the exact content-derived SHA is
recorded in the task result because a commit cannot contain its own hash.

## Decision

No P0 defect remains. All five confirmed P1 findings and all four P2 findings in
this remediation are fixed. Functional MQTT conformance, resource-pressure
conformance, and restart-state conformance each pass their current local gates.

## Finding ledger

| Finding | Confirmed | Root cause | Fix | Evidence | Remaining limitation |
|---|---:|---|---|---|---|
| Persistent subscriber overload | yes | route committed one subscriber at a time and swallowed overload/unavailable | all-target route preflight followed by one lock-held atomic commit; QoS0 alone remains best effort | `MQTT-PERSISTENT-OVERLOAD-QOS1-001`, `MQTT-PERSISTENT-OVERLOAD-QOS2-001` | route preflight scans only bounded matching/session state |
| QoS2 CleanSession incarnation race | yes | operation token fenced socket takeover but not destruction/recreation of the same session key | persistent monotonic incarnation plus packet ID and operation token; CleanSession=0 keeps the incarnation, CleanSession=1 replaces it | `QOS2-CLEAN-SESSION-INCARNATION-001`, existing takeover test | end-to-end delivery remains at-least-once across abnormal crash |
| Stale persistent authorization | yes | stored subscriptions/offline/QoS state had no authorization provenance | credential version, auth generation and permissions are stored; mismatch resets the session and Session Present=0; management invalidation removes matching bounded state | `MQTT-AUTHZ-PERSISTENT-RESET-001` | authorization-equivalent credential rotation is intentionally conservative and resets state |
| Structural MQTT recovery shutdown | yes | structural commit errors returned before EventBus drain/spool | MQTT recovery status is separated from EventBus safety; structural failure is logged critical only after required work drains/spools, then the process remains alive and unready | `MQTT-RECOVERY-STRUCTURAL-EVENTBUS-SAFETY-001` | forced SIGKILL remains the documented crash-loss boundary |
| Monolithic JSON recovery | yes | full broker clone plus whole-image JSON buffer/read | NBMQ v2 streams typed raw-binary records with header and per-record checksums; write-only v2, read v1/v2 | v2/v1 round trips, corruption tests, benchmark, decoder fuzz | legacy v1 decoding allocates its bounded legacy payload |
| Recovery semantics | yes | framing/checksum checks did not reject all impossible MQTT states | validate topics/filters, sizes, packet IDs, duplicates/order, durable QoS, outbound/inbound state QoS, retained consistency and auth compatibility | `MQTT-RECOVERY-SEMANTIC-INVALID-001` | recovered legacy sessions without provenance are reset on next authenticated attach |
| Retained replacement reservation | yes | every reservation charged one new entry and full bytes | reservation stores exact positive global/tenant count and byte deltas | `RETAIN-REPLACEMENT-RESERVATION-001` | simultaneous same-topic reservations may conservatively over-reserve bytes but cannot under-reserve |
| Actual decoder fuzz | yes | prior target fuzzed JSON snapshot deserialization rather than NBMQ framing | public pure bounded decoder; target fuzzes raw images and valid-v2-header record streams | 10,000-run `mqtt_recovery` smoke | smoke duration is not a substitute for continuous fuzzing |
| EventSink panic | yes | `DeliveryRecord` moved into a task and JoinError lost the runnable copy | panic is caught inside the owned delivery future and returned as retryable; normal completion path requeues and fixes inflight accounting | `EVENTBUS-SINK-PANIC-RECOVERY-001` | panic hook output remains visible by design |

## Publication and ownership ordering

For QoS1, authorization and the complete retained/subscriber route plan succeed,
then broker responsibilities commit, then the IoT EventBus crosses EventAccepted,
and only then is PUBACK written. For QoS2, authorization plus inbound transaction
and retained reservation precede PUBREC. PUBREL work crosses EventAccepted once,
then atomically commits the route plan and removes the stored transaction before
PUBCOMP. A failed plan leaves the QoS2 EventAccepted transaction owned and no
PUBCOMP is emitted. Multi-subscriber failure cannot leave an earlier peer committed.

## Persistent session boundary

Transaction identity is `(SessionKey, session_incarnation, packet_id,
operation_id)`. Session incarnation persists in NBMQ v2; process-local delivery
ownership does not. Restart converts `Delivering` to `AwaitPubrel`. A same-session
CleanSession=0 takeover can finish the original operation, while old work from a
destroyed CleanSession=1 incarnation receives `Conflict` and cannot mutate the new
packet-ID lifecycle.

Permission downgrade, credential-version change, and auth-generation change all
use the safe default: reset the old persistent session, return Session Present=0,
and deliver no old offline data. Device/product/tenant/version/generation/all
management invalidations explicitly remove matching persistent MQTT sessions; an
unrelated invalidation does not match unrelated keys/provenance.

## NBMQ v2 and recovery benchmark

NBMQ v2 has a checksummed 48-byte header and typed records containing a type,
32-bit checked length, raw payload, and SHA-256. The maximum temporary record buffer
under default limits is 67,072 bytes, independent of total image size. Commit keeps
0600 temp-file, file fsync, atomic rename, and directory fsync semantics. The
configured maximum is 201,588,784 bytes (192.25 MiB), derived from admitted compact
state rather than JSON expansion.

The ignored/manual benchmark was explicitly run on this host:

| Logical payload | File bytes | Encode | Decode |
|---:|---:|---:|---:|
| 10 MiB / 10,485,760 B | 10,495,673 | 371 ms | 294 ms |
| 50 MiB / 52,428,800 B | 52,478,556 | 1,481 ms | 1,470 ms |
| 100 MiB / 104,857,600 B | 104,957,922 | 2,914 ms | 3,001 ms |

`/usr/bin/time -l` reported 483,278,848 bytes maximum RSS for the complete debug
test process, which deliberately holds source and restored brokers simultaneously;
it is not the codec temporary-buffer size. The measured temporary record ceiling is
67,072 bytes.

## Validation

- Rust 1.88.0: fmt PASS, clippy with `-D warnings` PASS, workspace tests PASS
  (121 executed, 2 ignored; the recovery benchmark was then run explicitly and
  passed).
- Rust stable 1.97.1: fmt PASS, clippy with `-D warnings` PASS, workspace tests PASS
  (121 executed, 2 ignored).
- Raw MQTT: 30/30 PASS.
- Mosquitto broker differential: 11/11 PASS.
- Mosquitto MQTT 3.1.1 client matrix: PASS.
- Verified TLS matrix: PASS.
- Full release gate: 65/65 PASS; normative mapping 125/125 PASS.
- Fuzz: `mqtt_packet`, `mqtt_state`, and `restart_spool` 1,000 runs each; actual
  NBMQ `mqtt_recovery` decoder 10,000 runs after a 1,000-run initial smoke; no crash.
  macOS emitted non-fatal external-symbolizer warnings.

## Remaining risks

The broker is intentionally memory-first between planned snapshots. SIGKILL, OS
crash, power loss, or hardware loss can discard recently accepted MQTT state.
Business delivery remains at-least-once. NBMQ v1 compatibility is bounded by the
new configured recovery ceiling. Planned v2 commit holds the broker mutex while
streaming the coherent image, after ingress has quiesced. MQTT 5, clustering,
shared subscriptions, bridges, crash-durable exactly-once delivery, a runtime
database, and an external runtime broker remain outside the claimed profile.
