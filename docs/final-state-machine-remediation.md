# Final MQTT state-machine remediation

Date: 2026-09-21

Baseline SHA: `6e047b0a1be6b4a3a78aacefb3084e77f40e201e`.

Final SHA: the local commit containing this report; the exact SHA is recorded in
the task result because a Git commit cannot embed its own content-derived identity.

## Decision

The confirmed P1 MQTT state-machine, authentication registration, attachment,
tenant scheduling, recovery sizing, and Will responsibility defects are fixed.
The related EventBus scheduler and release-gate P2 defects are also fixed. No P0
defect was found in this follow-up. The local MQTT 3.1.1 release gate is 65/65 PASS,
including machine verification of all 125 applicable normative requirements.

## Finding ledger

| Issue | Severity | Confirmed | Root cause | Fix | Deterministic evidence | Result / limitation |
|---|---|---:|---|---|---|---|
| Wrong outbound ACK | P1 | yes | boolean `qos2` completion made PUBACK valid for every state except AwaitPubcomp | exact `OutboundAck` × `OutboundState`; mutation occurs only on a valid pair | `QOS2-OUT-WRONG-PUBACK-001`, `QOS2-OUT-WRONG-PUBCOMP-001` | PASS; invalid ACK uses the existing protocol-error close policy |
| Inbound QoS2 takeover | P1 | yes | EventAccepted finish required the old active generation | stored `Delivering { operation_id }`; token-fenced finish and session-scoped route | `QOS2-IN-SESSION-TAKEOVER-001` | PASS; abrupt process loss remains at-least-once |
| Auth return/revoke/register | P1 | yes | auth epoch protected cache completion but not the gap before `Sessions::register` | candidate carries epoch; registration and invalidation share one mutex gate | `AUTH-REGISTER-REVOKE-RACE-001` covers device, product, tenant, credential version, auth generation, and all | PASS; global epoch conservatively rejects unrelated stale candidates |
| MQTT attachment cleanup | P1 | yes | active broker ownership was installed before fallible CONNACK/write paths without a guard | generation-fenced `Attachment` RAII; explicit detach is idempotent | `MQTT-CONNACK-WRITE-FAIL-CLEANUP-001` uses a real closed duplex peer | PASS; CleanSession=1 leaves no history, persistent state remains inactive and later reports Session Present=1 |
| Tenant inflight wake | P1 | yes | ACK only called `next_offline` on its own session | bounded per-tenant deduplicated pending-session queue; direct channel wake on capacity release | `MQTT-TENANT-INFLIGHT-WAKE-QOS1-001`, `MQTT-TENANT-INFLIGHT-WAKE-QOS2-001` | PASS; no global scan or task-per-message |
| Recovery size | P1 | yes | v1 cloned and JSON-expanded the complete state | streaming compact NBMQ v2 records; 192.25 MiB configured bound; v1 read compatibility | `MQTT-RECOVERY-V2-ROUNDTRIP-001`, `MQTT-RECOVERY-V1-COMPAT-001`, actual decoder fuzz | PASS; legacy v1 decoding still needs its bounded payload allocation |
| Accepted Will loss | P1 | yes | retained resources could be consumed after CONNECT acceptance | retained reservation plus armed `WillGuard`; publication and release share one lock | `WILL-RESOURCE-FAILURE-001`, raw takeover/shutdown Will cases | PASS; optional IoT binding after MQTT publication remains best effort |
| EventBus full-concurrency spin | P2 | yes | a ready queued record produced a zero-delay timer while every delivery slot was full | full-concurrency branch waits only for stop or JoinSet completion | `EVENTBUS-CONCURRENCY-NOSPIN-001` | PASS |
| CI toolchain selection | P2 | yes | installing a matrix toolchain did not prove Cargo selected it | matrix `RUSTUP_TOOLCHAIN`, rustc/cargo version logging, locked clippy/tests | local 1.88.0 and stable validation below | PASS locally; remote Actions needs the user to push |
| Conformance gate | P2 | yes | zero selected tests and missing external tools could silently succeed; normative mapping was prose | exact/duplicate ID checks, required dependency failures, stable Rust evidence IDs, exact 125-entry JSON map | `--only NON_EXISTENT_TEST` nonzero; `--release-gate` 65/65; `NORMATIVE-COVERAGE-001` 125/125 | PASS |

## Ownership and cancellation audit

- `SessionLease` still generation-fences the node-local command endpoint and drops
  on every MQTT/TCP post-registration return.
- `Attachment` is constructed before fallible resumed-frame enqueue and detaches on
  Drop; setup failures while the broker mutex is held roll back in place to avoid a
  destructor lock cycle.
- `WillGuard` is unarmed until broker attach succeeds, armed immediately before the
  success CONNACK, suppressed only by DISCONNECT, and otherwise published on Drop.
- retained reservations cover inbound QoS2 and accepted retained Wills and are
  released once on route, removal, restore validation, or guard suppression.
- QoS2 Delivering ownership is a bounded stored state with one operation ID per
  transaction; restore changes an interrupted Delivering state back to AwaitPubrel.
- outbound packet IDs and `outbound_order` are unchanged on every invalid ACK.
- offline count/bytes do not change when a pending session is merely scheduled;
  promotion moves the existing charge between offline and outbound responsibility.

## Recovery invariant

`Limits::validate` requires `mqtt_recovery_max_bytes >=
mqtt_recovery_upper_bound()`. Logical session and retained accounting already
charges a 64-byte envelope for every variable record. Multiplying all admitted
logical bytes by six covers worst-case JSON string escaping and numeric byte-array
encoding; checked per-session/retained wrappers and the 52-byte file envelope are
then added. Therefore every legal admitted runtime state has a configured recovery
representation. `Overloaded`, `Configuration`, or `Invalid` during planned commit
is treated as a structural invariant failure instead of an infinite retry; storage
failures retain the existing repair-and-retry behavior.

## Validation record

- Raw MQTT core: 30/30 PASS.
- Isolated Mosquitto broker differential: 11/11 PASS.
- Mosquitto 3.1.1 client and verified TLS matrices: PASS.
- Full release gate: 65/65 PASS; normative coverage 125/125.
- Targeted Rust fault invariants: 10/10 PASS.
- Fuzz smoke: `mqtt_state`, `mqtt_recovery`, and `restart_spool`, 1,000 runs each,
  no crash. The recovery run had non-fatal macOS symbolizer warnings.
- Exact Rust 1.88.0 and current stable fmt/clippy/workspace-test commands are
  recorded in the final task result after the final tree is validated.

## Remaining risk

The broker remains memory-first: SIGKILL, OS crash, power loss, or hardware loss can
discard responsibility accepted after the last planned snapshot. NBMQ v2 removes
whole-image JSON and bounds temporary encode/decode storage to one 67,072-byte
record; legacy v1 input remains bounded by `mqtt_recovery_max_bytes`.
Single-node ownership, at-least-once business delivery, bounded slow-subscriber
shedding, and the documented 24-hour disconnected-session policy remain deliberate
profile constraints. MQTT 5, WebSocket, bridges, shared subscriptions, clustering,
and crash-durable exactly-once delivery are not claimed.

## Correctness baseline freeze follow-up

The next remediation closes the remaining persistent-state boundaries: QoS1/QoS2
fanout now uses all-target preflight and atomic commit; QoS2 async work is fenced by
session incarnation plus operation token; persistent state is reset on authorization
provenance change or matching management invalidation; structural MQTT recovery
failure cannot bypass EventBus drain/spool; retained replacement reserves only its
positive delta; and EventSink panics become supervised retryable failures. NBMQ v2
replaces new JSON writes while retaining v1 reads. Evidence is consolidated in
`docs/correctness-baseline-freeze.md`.
