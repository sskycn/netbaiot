# Reliability follow-up audit

> Historical report: describes the revision measured when it was written, not the
> current device protocol surface. Device HTTP has since been removed. Current
> behavior, migration, tests and measurements: [removal report](remove-device-http.md).
> Original measurements are retained; old HTTP benchmark tools can be retrieved
> from baseline `945fe5e386d623c32e2c7d2d0568fe0c058107ec`.

Date: 2026-09-20  
Baseline: `e0687a63a9d99abb6e0ff0d43bc2621e69843d97`  
Scope: auth cache, UDP authentication, EventBus retry/recovery, MQTT QoS2 and
recovery, confirmed business stream, public protocol, config cache, SDK and real
client interoperability.

## Finding ledger

| ID | Severity | Confirmed on baseline | Location / reproduction | Root cause | Fix | Regression / validation result | Remaining limitation |
|---|---|---:|---|---|---|---|---|
| REL-01 | P1 | yes | `runtime/auth.rs::AuthCache::authenticate`; abort the first task while followers wait | inflight ownership was removed only on the normal return path | operation-ID RAII leader lease removes and signals on cancellation/unwind; followers re-elect | `cancelled_leader_releases_followers_and_all_inflight_resources`, `timed_out_leader_does_not_poison_retry` | process abort cannot run destructors, but no in-memory state survives it |
| REL-02 | P1 | yes | invalidate while a delayed provider success is outstanding | invalidation removed existing entries but did not fence a stale completion | monotonic auth epoch captured by every miss; stale completions fail closed and cannot cache | `invalidation_fences_delayed_success_and_retry_uses_new_generation`; existing management path cancels affected live sessions | epoch is deliberately global, so unrelated invalidation may force an extra provider lookup |
| REL-03 | P1 | yes | signed UDP cache key included message and tag | every datagram was a unique remote-auth cache miss | provider resolves opaque `DeviceVerifier` once by credential; every packet performs local constant-time HMAC plus version and replay validation | `ten_thousand_signed_packets_use_one_verifier_lookup`; replay-window tests | external verifier endpoint must support the documented verifier response |
| REL-04 | P1 | yes | restart twice while required sink remains unavailable | append-only spool files replayed both the old and newly re-spooled responsibility | version-2 single authoritative snapshot with generation, fsync and atomic replace; v1 migration coalesces stable IDs | spool generation unit tests; real three-unavailable-generation subprocess test; final 62.15 s soak | planned-restart durability only; SIGKILL window remains documented |
| REL-05 | P1 | yes | required delivery reaches max attempts/age | ownership stayed charged but no worker ever retried it | bounded low-frequency degraded retry lane using the existing single sink worker | `required_delivery_recovers_after_normal_retry_exhaustion` | dependency failures can retain bounded work until drain/spool |
| REL-06 | P1 | yes | event A backs off at queue front, event B is immediately ready | `VecDeque::front` controlled all readiness | bounded queue scans for the earliest ready record and sleeps until the earliest deadline | `delayed_retry_does_not_block_later_ready_delivery` | scan is O(queue bound), intentionally trading bounded CPU for simple ownership |
| REL-07 | P1 | yes | legal MQTT snapshot exceeds 1 MiB | MQTT reused EventBus record/segment limits | independent `mqtt_recovery_max_bytes` (256 MiB default), validated against global session + retained ceilings | `recovery_file_supports_legal_state_larger_than_event_spool_record`; inconsistent-limit test | one planned-restart image is still bounded and memory-loaded during restore |
| REL-08 | P1 | yes | inbound QoS2 EventBus acceptance succeeds and later broker routing fails | state was deleted before retained/subscriber responsibility completed | explicit `AwaitPubrel` / `EventAccepted` state; EventAccepted stage survives snapshot and resumes routing without another ingress call | QoS2 stage recovery unit test; real four-generation subprocess QoS2 restart test | no claim of crash-durable exactly-once outside planned snapshots |
| REL-09 | P1 | yes | retained QoS2 reaches PUBREL near retained capacity | capacity was checked only during post-accept routing | count/byte/tenant retained reservation is taken before PUBREC and consumed atomically with QoS2 route completion | retained bounds plus QoS2 snapshot/restore tests | reservations are conservative for replacement at a completely full store |
| REL-10 | P2 | yes | confirmed subscriber closes while no events exist | owner waited only on the delivery channel | owner also waits for socket readability/EOF; listener accepts bounded handshake tasks concurrently | official-client reconnect/replay integration test | protocol still permits one active logical subscriber |
| REL-11 | P2 | yes | second subscriber connects while one is active | serial accept loop made behavior depend on old connection activity | deterministic `Conflict` rejection; active owner has monotonically increasing generation and RAII cleanup | server/official-client integration suite | replacement policy is reject, not takeover |
| REL-12 | P2 | yes | subscriber reconnects with a filter that excludes pending required work | filter mismatch incorrectly returned `SinkAck` | mismatch returns retryable/unavailable semantics; stable required sink remains pending for a later matching subscriber | EventBus degraded retry and official reconnect tests | filter is eligibility on one required sink, not durable multi-subscription routing |
| REL-13 | P2 | yes | drain usage read fails or ACK lands between check and wait registration | `map_or(true, ...)` failed open and `Notify` registration followed the check | API returns `Result<bool>`; notify future is registered before every checked read | EventBus drain tests and all graceful-restart subprocess tests | timeout remains an explicit `Ok(false)` outcome |
| REL-14 | P2 | yes | older SDK deserializes a response with a new field | response structs used `deny_unknown_fields` | server response/stream payload structs accept additive fields; client request frames remain strict | `revision_rejects_zero_and_response_types_accept_additive_fields` | new enum variants and changed required semantics still require a protocol version |
| REL-15 | P2 | yes | deserialize or directly construct revision zero | public tuple field and derived `Default` allowed invalid state | private field, `new`, `get`, `TryFrom<u64>`, validated custom deserializer, no `Default` | protocol zero/additive compatibility test; all callers migrated | none |
| REL-16 | P2 | yes | repeated config invalidate/reinsert | invalidation retained the removed entry's byte charge; routes were not charged | per-device exact charge plus product/route byte components; upsert/invalidate/route replacement use checked deltas | `invalidation_releases_exact_byte_charge_across_reinsert_cycles` | logical serialized bytes exclude allocator overhead by design |
| REL-17 | P2 | yes | lifecycle and subscriber/auth operations cancelled at sensitive waits | several check-then-notify and cleanup paths depended on normal completion | pre-registered lifecycle/drain notifications, auth and subscriber RAII ownership, bounded JoinSets and child cancellation tokens | lifecycle, auth cancellation, official reconnect, graceful-shutdown suites | filesystem fsync cannot be cancelled once dispatched to `spawn_blocking` and is awaited before success |

Severity totals are P0: 0 confirmed / 0 fixed / 0 already fixed / 0 remaining;
P1: 9 / 9 / 0 / 0; P2: 8 / 8 / 0 / 0. All findings were reproducible
on the local baseline and none were already fixed. There is no known remaining
P0/P1/P2 defect in this audit scope.

## State machines and ownership contracts

Authentication inflight ownership is `(cache key, operation id, auth epoch)`. Only
that owner may publish a result. Drop removes only its own operation. Invalidation
advances the epoch before removing entries, so a pre-invalidation future cannot
authenticate a new session or repopulate positive/negative/verifier state.

UDP remote work is identity/verifier resolution, not packet validation. Cached
verifier material is count/byte/TTL bounded with the existing AuthCache. HMAC,
credential version, timestamp, boot/sequence replay window and ingress admission are
checked for every packet. Cached hits continue during provider outage; unknown or
expired identities fail closed.

Required EventBus records always remain in exactly one of queued, inflight,
degraded-retry, or authoritative-spool ownership. Attempts are serialized into the
spool. Version 2 stores one generation; a stale cleanup token compares the on-disk
generation before unlinking.

Inbound MQTT QoS2 is `AwaitPubrel -> EventAccepted -> routed/released`. Duplicate
PUBLISH and PUBREL reuse the same packet state. `EventAccepted` prevents a retry of
the business side effect while retained/subscriber routing is still outstanding.
Retained count/bytes are reserved before the broker returns PUBREC.

## Business stream and public compatibility

The confirmed business sink is one stable required route target. A subscriber
filter controls which active client is eligible to ACK that responsibility; it does
not change already accepted routing or turn a mismatch into success. One subscriber
owns the sink. A second valid handshake receives `Conflict`. Idle EOF is observed,
and owner cleanup is generation-fenced.

Incoming requests remain strict: stream client frames, control snapshots, route
updates, device uplinks and other mutation inputs reject unknown fields. Output-only
response shapes accept additive fields. Required fields, tagged variants and
`PROTOCOL_VERSION` remain strict.

## Validation record

- `cargo test -p netbaiot-runtime --lib --all-features`: 27 passed.
- `cargo test -p netbaiot-transports --lib --all-features`: 23 passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo test --workspace --all-features`: 93 passed, zero failed; one 60-second test is intentionally ignored in the normal suite.
- `cargo test -p netbaiot-server --all-features --tests`: 15 regular tests passed across the server library and integration targets; the soak is ignored in the normal suite.
- `subprocess_graceful_restart_spools_and_replays_every_accepted_event_id`: passed with three consecutive unavailable generations, one authoritative spool file, a pending EventBus delivery and persistent MQTT state in the same snapshots.
- `subprocess_graceful_restart_sixty_second_soak -- --ignored`: passed on the final code in 62.15 seconds across 12 generations.
- Nightly libFuzzer smoke: `restart_spool` and `mqtt_state` each completed 1,000 runs without a crash.
- Exact client versions inspected before use: `mosquitto_pub 2.1.2` / `mosquitto_sub 2.1.2`, both using `libmosquitto 2.1.0`.
- Real mosquitto MQTT 3.1.1 checks passed: CONNECT success; wrong-password rejection with CONNACK 4; QoS0 retained delivery; persistent offline QoS1/QoS2; exact, `+`, and `#`; retained replace/delete; abnormal retained QoS2 Will; normal DISCONNECT Will suppression; Session Present; and same ClientId isolation for two authenticated devices. Exact duplicate/reconnect protocol stages are additionally covered by raw-socket subprocess tests.
- No external broker was started. All clients connected to the embedded NetbaIoT broker.
- `tests/mqtt_paho_interop.py` was attempted but the host lacks `paho-mqtt`; no package was installed and this is not counted as a pass.
- Focused release benchmark: auth cache hit 1.04 Mops/s (P50/P95/P99 916/1,042/1,083 ns), UDP verifier-cache hit plus HMAC-SHA256 554 kops/s (1,708/2,333/2,375 ns), local provider miss 10.5 kops/s, EventBus publish 820 kops/s (1,125/1,292/2,000 ns), MQTT QoS1 route+ACK 1.39 Mops/s (667/750/958 ns), inbound QoS2 accept+route 2.53 Mops/s (375/417/417 ns), MQTT decode 7.07 Mops/s and MQTT encode 10.04 Mops/s. No broad unexpected regression was observed; these are local in-process measurements, not production capacity claims. Retry scheduling is also covered by ready-behind-delayed and ready-while-another-delivery-is-inflight deterministic tests.

## Security and compatibility notes

No credential, verifier key, authorization header, password, raw device secret or
TLS key is written to logs or recovery state. The HTTP verifier endpoint is accepted
only over HTTPS or loopback HTTP and remains timeout/concurrency/response-size
bounded. Existing MQTT/TCP/HTTP secret authentication wire behavior is unchanged.
The added external-auth verifier request is an intentional provider contract
extension required for efficient signed UDP.

The recovery changes are forward-only on write and backward-compatible on read:
v1 EventBus segments migrate into v2; MQTT recovery keeps its existing format but
uses its own explicit byte limit. Abrupt crash durability and exactly-once business
delivery remain explicitly out of scope.
