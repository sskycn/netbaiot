# Production reliability remediation

Date: 2026-09-20

Reviewed baseline: `e0687a63a9d99abb6e0ff0d43bc2621e69843d97`

Local starting point: `a48192ceb3477824ef27c25ae8388ac7187fdf7c`

The local starting point already contained the cancellation-safe auth cache, auth
epoch fencing, UDP verifier cache, EventBus degraded scheduling and race fixes,
single-generation EventBus spool, MQTT QoS2 recovery, dedicated MQTT recovery size,
public protocol compatibility, ConfigRevision and ConfigCache corrections, and the
business-stream global-required Model B fix. This pass verified those changes and
completed the remaining production blockers without adding a database or external
broker.

## Finding ledger

| ID | Severity | Confirmed | Root cause | Code change | Regression / actual result | Remaining limitation |
|---|---|---:|---|---|---|---|
| P0-SPOOL | P0 | yes | shutdown stopped workers and returned when restart-spool commit failed | work and management listener ownership split; workers retain pending state; MQTT/EventBus commit retries while alive and unready; failed temporary files removed | real child remains alive, readiness is 503, repair completes shutdown, restart delivers the same `event_id` | SIGKILL while blocked can lose memory-only work |
| P0-STREAM | P0 | yes on reviewed baseline; fixed at local start | filter mismatch returned success for a global required sink | chose Model B; mismatch is retryable and the stable responsibility survives subscriber/filter changes | mismatch, competing subscriber, changed-filter and explicit ACK test passes | one active logical subscriber; filter is eligibility rather than multi-route durability |
| AUTH-CANCEL | P1 | yes on reviewed baseline; fixed at local start | dropped leader left the single-flight key owned forever | operation-ID RAII leader lease removes and wakes on drop/timeout | cancellation, followers, timeout and resource-baseline tests pass | process abort does not run destructors, but no memory survives it |
| AUTH-FENCE | P1 | yes on reviewed baseline; fixed at local start | delayed pre-invalidation success could repopulate cache | monotonic invalidation epoch fences completion | delayed success, invalidate-all, version and generation tests pass | global epoch may cause a harmless extra lookup |
| AUTH-SESSION | P1 | yes | disconnect candidates came only from positive cache entries | bounded active-session scan matches bound identity by device/product/tenant/version/generation/all | direct no-cache revocation test passes | node-local sessions only, by design |
| UDP-VERIFY | P1 | yes on reviewed baseline; fixed at local start | signed message/tag formed the cache key | provider resolves `DeviceVerifier`; every packet performs local HMAC/version/replay checks | 10,000 valid datagrams use exactly one provider call; negative/replay/invalidation tests pass | verifier provider must support the documented response contract |
| EVENT-FREEZE | P1 | yes on reviewed baseline; fixed at local start | required work became permanently unrunnable after normal retries | bounded degraded/parked retry cadence retains ownership | sink recovery after retry exhaustion passes | bounded pending work may remain until dependency recovery or spool |
| RETRY-HOL | P1 | yes on reviewed baseline; fixed at local start | deque front controlled readiness | readiness-based bounded delayed selection | ready-behind-delayed tests pass | bounded O(queue) selection is intentional |
| DRAIN-RACE | P1 | yes on reviewed baseline; fixed at local start | failed-open usage and check-before-notify race | `Result<bool>` and pre-registered notification | deterministic drain interleaving test passes | timeout remains explicit `Ok(false)` |
| QUIESCE-RACE | P1 | yes on reviewed baseline; fixed at local start | admission release could race waiter registration | lifecycle pre-registers notification before checked read | deterministic lifecycle boundary test passes | none |
| ACCEPT-TOUCH | P1 | yes | fallible presence touch followed EventAccepted | touch moved before EventBus publish | forced full-active presence rejects and EventBus usage remains zero | last-seen is intentionally volatile |
| PRESENCE | P1 | yes | disconnected historical devices stayed forever | active entries are pinned; offline entries expire by TTL and oldest offline is evicted at capacity | 100-device churn with limit 2 passes; two active entries cannot be evicted | presence is not a durable device registry |
| EVENT-GEN | P1 | yes on reviewed baseline; fixed at local start | append-only generations duplicated restored responsibility | one version-2 authoritative generation, atomic replace, generation-fenced cleanup | three unavailable generations retain one file and stable IDs | version-1 files are migration-only |
| MQTT-SIZE | P1 | yes on reviewed baseline; fixed at local start | MQTT state inherited the 1 MiB EventBus record ceiling | independent 256 MiB `mqtt_recovery_max_bytes`, validated against legal state ceilings | legal recovery image larger than 1 MiB passes | one bounded image is memory-loaded during planned recovery |
| MQTT-QOS2 | P1 | yes on reviewed baseline; fixed at local start | inbound state was forgotten before every side effect was safe | `AwaitPubrel -> EventAccepted -> routed/released`, retained reservation before PUBREC | duplicate/restart/retained stage tests and four-generation raw-socket recovery pass | MQTT receiver semantics only; business remains at-least-once |
| MQTT-PUBLISH | P1 | yes | regular path EventAccepted before fallible retained/routing work | broker routing/retained admission now precedes IoT EventAccepted; no broker fallibility follows it | broker bounds and workspace integration tests pass | an IoT decode/admission failure after MQTT routing can produce normal at-least-once MQTT replay |
| MQTT-SUB | P1 | yes | subscription/trie mutated before retained replay could fail | target-session preflight covers channel, inflight, offline and byte bounds; scoped rollback handles closure | forced retained channel-capacity failure leaves no session subscription, trie route, or future live delivery | retained matching scans the bounded retained store |
| MGMT-TLS | P1 | yes | non-loopback management HTTP allowed bearer auth without TLS | server requires TLS and an admin secret off-loopback; official client rejects non-loopback `http://` | loopback HTTP, public plaintext rejection, public TLS config and client tests pass | one shared server TLS configuration is used today |
| PROTO-COMPAT | P2 | yes on reviewed baseline; fixed at local start | output models rejected harmless added fields | response/event models allow additive fields; mutation requests remain intentionally strict | additive response compatibility test passes | enum/required semantic changes still need a protocol version |
| CONFIG-REV | P2 | yes on reviewed baseline; fixed at local start | public tuple/default allowed zero | private nonzero revision with constructors and validated serde | zero construction/deserialization tests pass | none |
| CONFIG-BYTES | P2 | yes on reviewed baseline; fixed at local start | invalidation did not release device charge | exact per-entry and route/product logical byte deltas | repeated insert/invalidate returns to baseline | allocator overhead is not counted |
| CONFIG-SERIAL | P2 | yes | device invalidation bypassed the mutation lock | invalidate now shares `control_lock` with upsert, snapshot and routes | workspace HTTP/control tests pass | serialization is node-local, matching the database-free design |
| SDK-SUBACK | P2 | yes | SDK marked Connected after queuing SUBSCRIBE | connection becomes observable only after one successful SUBACK; `0x80` is terminal | fake broker CONNACK + SUBACK failure returns `Forbidden`, never Connected | QoS0 or QoS1 grant is accepted for the requested QoS1 subscription |
| AGENT-SCOPE | P2 | yes | contributor rules contradicted implemented MQTT support | authoritative scope now lists QoS2, persistent sessions, retained, LWT and wildcards | documentation inspection | MQTT 5 and clustering remain out of scope |
| CI | P2 | yes | obsolete PostgreSQL service/storage test and stable-only policy | PostgreSQL removed; matrix is declared Rust 1.88 plus latest stable with fmt/clippy/tests | local stable validation passes; remote Actions awaits push | GitHub Actions is not verified because this commit is not pushed |

## Boundary and ownership decisions

`EventAccepted` is the final producer-facing fallible boundary. Presence/accounting
and MQTT regular retained/routing admission happen before it. QoS2 uses its stored
EventAccepted stage plus pre-PUBREC retained reservation so duplicate PUBREL and
planned restart do not re-emit the DeviceEvent.

The EventBus spool is `NBSP | version 2 | generation | records + SHA-256` at the
single authoritative `eventbus-recovery.spool` path. Planned shutdown cannot finish
while required work is neither ACKed nor represented by that fsynced snapshot.
Management remains available while a failed commit blocks shutdown.

Presence is bounded volatile state: active sessions are never eviction candidates;
offline entries have `presence_ttl_ms` and oldest-offline eviction. Authentication
revocation walks only the already bounded active-session map and does not depend on
historical cache membership.

## Validation record

- `cargo fmt --all -- --check`: PASS.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS on
  the installed stable toolchain.
- `cargo test --workspace --all-features`: PASS, 100 passed, zero failed, one
  explicitly ignored soak.
- `subprocess_graceful_restart_sixty_second_soak -- --ignored`: PASS in 61.92 s.
- Spool-failure repair/replay and forced shutdown-blocked SIGKILL subprocess tests:
  PASS.
- The auth/UDP/MQTT stress tests measured one provider call for 10,000 valid UDP
  packets and one authentication for 10,000 real MQTT publishes: PASS.
- MQTT recovery test stored 20 legal 60,000-byte offline payloads (1,200,000 payload
  bytes) and verified a recovery image larger than 1 MiB: PASS.
- `python3 tests/run_mosquitto_cli_interop.py`: PASS using `mosquitto_pub` and
  `mosquitto_sub` 2.1.2 (libmosquitto 2.1.0) only as clients. Auth success/failure,
  QoS0/1/2, exact/`+`/`#`, unsubscribe, retained replace/delete/wildcard replay,
  persistent reconnect/offline QoS1/2, Will QoS0/1/2 with retain, and
  normal-disconnect Will suppression passed. Persistent offline replay provides CLI
  semantic evidence; the Session Present bit is asserted directly by the raw-socket
  restart test because mosquitto_sub does not expose that flag in machine output.
- Paho was not run because `paho-mqtt` is not installed on the host.
- Nightly libFuzzer smoke: `mqtt_packet`, `mqtt_state`, `restart_spool`, and
  `business_stream` each completed 1,000 runs without a crash. The host emitted only
  a non-fatal symbolizer warning for `business_stream`.
- GitHub Actions is not verified because this commit is not pushed.

## Remaining risk

The documented abnormal-crash window remains: SIGKILL, OS crash, power loss or
hardware loss can discard accepted state that is still only in RAM. Business
delivery is at-least-once and consumers must deduplicate stable `event_id`. The
system intentionally remains single-node, database-free, and bounded; it does not
claim broker clustering, crash-durable exactly-once, or durable offline commands.
