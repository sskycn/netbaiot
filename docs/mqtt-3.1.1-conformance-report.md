# MQTT 3.1.1 conformance release-gate report

## Decision and scope

The embedded server passes the MQTT 3.1.1 with Errata 01 release gate at the
audited tree that follows baseline `b337c1127d1640b32f0518e38e016872dede6653`.
The immutable final commit is recorded in the task result because a commit cannot
contain its own hash. This decision covers the TCP/TLS MQTT server profile, not
MQTT 5, MQTT-SN, WebSocket transport, bridges, shared subscriptions, or clustering.

Normative source: [OASIS MQTT Version 3.1.1 Plus Errata 01](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.html).

## Environment

| Item | Audited value |
|---|---|
| Host | macOS 26.6.2, arm64 |
| Rust | rustc/cargo 1.88.0 MSRV and 1.97.1 current stable |
| Mosquitto clients | mosquitto_pub/sub 2.1.2, libmosquitto 2.1.0 |
| Reference broker | Mosquitto 2.1.2, isolated loopback temporary configuration |
| Paho | unavailable in the current Python environment; no package was installed |
| Broker profile | authenticated DeviceKey, canonical IoT topics, bounded in-memory state, planned-restart snapshot |
| Raw artifact | `target/mqtt-conformance/results.json` |
| Stable catalog | `tests/mqtt_conformance/catalog.json` (125 exact normative mappings plus 10 named Rust fault invariants) |

## Normative traceability summary

Appendix B contains 141 numbered statements (including the two identifiers printed
with historical punctuation typos). Of these, 125 apply to this MQTT server profile.
All 125 are PASS; none is FAIL, PROFILE, or UNTESTED. Sixteen are NOT_APPLICABLE.
Product-profile restrictions are reported separately below and are not used to turn
an MQTT MUST into a profile exception.

| Result | Count |
|---|---:|
| Applicable | 125 |
| PASS | 125 |
| FAIL | 0 |
| PROFILE | 0 |
| UNTESTED | 0 |
| NOT_APPLICABLE | 16 |

## Normative requirement matrix

Each row enumerates every identifier in its area; evidence IDs resolve to the raw
suite, the Mosquitto differential suite, or named Rust tests. `source audit` means
the packet encoder/decoder or state transition was inspected in addition to tests.

| Requirement IDs | Area | Applicable | Result | Evidence / note |
|---|---|---|---|---|
| MQTT-1.5.3-1..3 | MQTT UTF-8 | yes | PASS | `UTF8-001`, packet UTF-8 unit vectors, fuzz |
| MQTT-2.2.2-1..2 | fixed-header flags | yes | PASS | `HEADER-001`, `DIFF-FLAGS-001`, packet unit test |
| MQTT-2.3.1-1,4..7 | server packet identifiers | yes | PASS | `SUB-002`, `QOS1-001`, `QOS2-IN-001`, outbound lifecycle/restart tests |
| MQTT-2.3.1-2..3 | client identifier allocation | no | NOT_APPLICABLE | client-only statements |
| MQTT-3.1.0-1..2 | first/second CONNECT | yes | PASS | `SEQUENCE-001`, `CONNECT-005` |
| MQTT-3.1.2-1..6,8..22,24 and MQTT-3.1.2.7 | CONNECT flags/session/Will/keepalive | yes | PASS | `CONNECT-002..004`, `SESSION-001`, `WILL-001`, `WILL-SHUTDOWN-001`, `KEEPALIVE-001` |
| MQTT-3.1.2-23 | client keepalive sender | no | NOT_APPLICABLE | client-only statement |
| MQTT-3.1.3-1..6,8..11 | CONNECT payload and ClientId | yes | PASS | CONNECT matrix, UTF-8 unit tests, source audit |
| MQTT-3.1.3-7 | zero ClientId client rule | no | NOT_APPLICABLE | client-only; server enforcement is tested by `CONNECT-004` |
| MQTT-3.1.4-1..5 | CONNECT server processing | yes | PASS | `CONNECT-001..005`, `WILL-TAKEOVER-001` |
| MQTT-3.2.0-1, MQTT-3.2.2-1..6 | CONNACK | yes | PASS | `CONNACK-001`, `SESSION-001`, `DIFF-SESSION-001` |
| MQTT-3.3.1-1..12 | DUP/QoS/retained flags | yes | PASS | QoS/restart and retained raw/differential matrices |
| MQTT-3.3.2-1..3 | Topic Name and matching | yes | PASS | packet/topic unit tests, `SUB-001`, routing tests |
| MQTT-3.3.4-1, MQTT-3.3.5-1..2 | publish response and granted QoS | yes | PASS | `QOS0-001`, `QOS1-001`, `QOS2-IN-001`, ACL close behavior |
| MQTT-3.6.1-1 | PUBREL flags | yes | PASS | `HEADER-001`, QoS2 duplicate/restart tests |
| MQTT-3.8.1-1, MQTT-3.8.3-1..3, MQTT-3-8.3-4 | SUBSCRIBE validity | yes | PASS | `HEADER-001`, `SUB-001/002`, parser tests |
| MQTT-3.8.4-1..6, MQTT-3.9.3-1..2 | SUBACK/re-subscribe/QoS | yes | PASS | `SUB-001`, `SUB-RESUB-001`, `DIFF-SUB-001` |
| MQTT-3.10.1-1, MQTT-3.10.3-1..2, MQTT-3.10.4-1..6 | UNSUBSCRIBE/UNSUBACK | yes | PASS | `HEADER-001`, `UNSUB-001`, `DIFF-SUB-001` |
| MQTT-3.12.4-1 | PINGRESP | yes | PASS | `KEEPALIVE-001`, `DIFF-PING-001` |
| MQTT-3.14.1-1, MQTT-3.14.4-3 | DISCONNECT flags and Will deletion | yes | PASS | `HEADER-001`, `WILL-001`, CLI matrix |
| MQTT-3.14.4-1..2 | client behavior after DISCONNECT | no | NOT_APPLICABLE | client-only statements |
| MQTT-4.1.0-1..2 | session lifetime | yes | PASS | `SESSION-001`, persistent/offline/restart matrices; bounded idle expiry is permitted administration |
| MQTT-4.3.1-1, MQTT-4.3.2-1..2, MQTT-4.3.3-1..2 | QoS state machines | yes | PASS | raw QoS matrix, broker unit/property and restart tests |
| MQTT-4.4.0-1 | persistent retransmission | yes | PASS | QoS1/QoS2 restart tests preserve ID and DUP/state |
| MQTT-4.5.0-1 | matching delivery enters Session state | yes | PASS | offline queue, routing and resource tests |
| MQTT-4.5.0-2 | client acknowledgement | no | NOT_APPLICABLE | client-only statement |
| MQTT-4.6.0-1,5..6 | retransmission/topic order | yes | PASS | `persistent_reconnect_retransmits_outbound_in_original_order`, offline ordering tests |
| MQTT-4.6.0-2..4 | client acknowledgement order | no | NOT_APPLICABLE | client-only statements |
| MQTT-4.7.1-1..3, MQTT-4.7.2-1, MQTT-4.7.3-1..4 | topic/filter matching | yes | PASS | matcher/parser tests, `SUB-001/002`; root `+`/`#` regression |
| MQTT-4.8.0-1..2 | protocol/transient error close | yes | PASS | malformed and illegal sequence raw matrix, fuzz |
| MQTT-6.0.0-1..4 | MQTT over WebSocket | no | NOT_APPLICABLE | WebSocket transport not implemented or claimed |
| MQTT-7.0.0-1 | server acting as MQTT client/bridge | no | NOT_APPLICABLE | no outbound broker connection or bridge mode |
| MQTT-7.0.0-2 | no required nonstandard extension | yes | PASS | standard MQTT 3.1.1 wire protocol; authentication/profile uses standard fields/topics |
| MQTT-7.1.1-1 | ordered lossless byte stream | yes | PASS | TCP/TLS, `FRAMING-001` |
| MQTT-7.1.2-1 | conformant client transport | no | NOT_APPLICABLE | client-only statement |

## NetbaIoT product-profile matrix

| Product behavior | Result | Evidence / consequence |
|---|---|---|
| Username/password resolves an authenticated DeviceKey | PASS | auth success/failure CLI and raw cases; one auth per connection |
| Session ownership is `(DeviceKey, ClientId)` | PASS | `SECURITY-SESSION-001`; cross-identity ClientId cannot resume or clear state |
| Canonical publish/subscribe namespace ACL | PASS | `SECURITY-ACL-001`; rejected before retained/QoS2 side effects |
| One live IoT command endpoint per DeviceKey | PROFILE | multiple persistent ClientIds may be stored, but a new same-device live connection replaces the prior local endpoint |
| Bounded sessions/queues/inflight/retained state | PASS | quota, rollback, slow-consumer and recovery validation tests |
| 24-hour disconnected-session idle policy | PROFILE | documented MQTT 3.1.1 administrative deletion policy, not MQTT 5 Session Expiry |
| EventAccepted before QoS1 PUBACK | PASS | end-to-end admission ordering tests; not a business persistence claim |
| Live-only business commands | PROFILE | deliberately separate from MQTT persistent subscription delivery |
| Planned restart snapshot | PASS | valid >1 MiB image, corruption/limit checks, sessions/QoS/retained restoration |
| Abrupt crash durability | PROFILE | bounded recent in-memory loss is documented; no crash-durable broker claim |

## Test matrices and outcomes

- Raw/state-machine: 30/30 PASS. It covers CONNECT/CONNACK, first-packet rules,
  flags, fragmentation, Remaining Length, UTF-8, sessions, subscribe/unsubscribe,
  QoS0/1/2 duplicates, retained, Will, keepalive, slowloris, illegal sequences,
  ACL, cross-identity ownership, and planned-shutdown Will recovery.
- Mosquitto broker differential: 11/11 PASS. Unknown PUBACK differs: NetbaIoT
  closes while Mosquitto gives no response; the specification permits the
  NetbaIoT protocol-error policy, so this is `IMPLEMENTATION_DEFINED_ALLOWED`.
- Mosquitto client: PASS with explicit MQTT 3.1.1 for authentication, QoS0/1/2,
  exact/`+`/`#`, unsubscribe, retained, persistent offline QoS1/2, and Wills.
- TLS client: PASS with certificate/hostname verification; untrusted CA rejected;
  no `--insecure` success path.
- Paho: unavailable for this run. Historical Paho 2.1.0 evidence exists, but is
  intentionally not counted as current evidence.
- Restart: PASS for persistent subscription/Session Present, retained, outbound
  QoS1, outbound/inbound QoS2 stages, >1 MiB legal state, and repeated generations.
- Resource/failure: PASS for retained subscription transaction rollback, QoS2
  pending-route/recovery, count/byte quotas, slow packet deadline and malformed fuzz.
- Final remediation release gate: 55/55 PASS. This includes raw and differential
  cases, Mosquitto client/TLS, restart, the ten stable audit IDs, and
  `NORMATIVE-COVERAGE-001` proving 125/125 current PASS evidence. Unknown `--only`
  fails before build; missing release dependencies are `SKIPPED_REQUIRED` and make
  the process nonzero.
- Fuzz: the final remediation smoke completed 1,000 runs each for `mqtt_state`,
  `mqtt_recovery`, and `restart_spool` (3,000 total) without a crash. The new
  recovery target emitted only non-fatal local symbolizer warnings.
- Soak: PASS, the ignored 60-second test completed 12 planned child-process
  generations in 61.93 seconds without duplicate/lost recovered responsibility.

## Defects discovered and fixed

1. Root `+` incorrectly matched a `$`-prefixed first topic level. The matcher now
   applies the same root-dollar exclusion as `#`, with regression tests.
2. Recovered outbound QoS1/QoS2 PUBLISH packets were emitted by packet-ID map order,
   not original send order. Persistent sessions now snapshot explicit outbound
   ordering and validate it on restore, with backward-compatible old-image recovery.
3. Planned server shutdown suppressed live Wills. Shutdown now publishes the Will
   before snapshot, while MQTT DISCONNECT remains the only normal suppression path.
4. Publish ACL was not fenced before every broker-side retained/QoS2 side effect.
   Authorization now precedes mutation and cross-device regressions cover QoS1/2.
5. The first harness version used two concurrent connections with one DeviceKey,
   conflicting with the documented live-endpoint profile. Those cases now use
   self-subscription/sequential sessions; this was `TEST_HARNESS_ERROR`, not a
   protocol implementation change.
6. The requested invalid-filter examples included `a/#`; MQTT 3.1.1 defines it as
   valid. The harness keeps the specification-correct behavior.
7. Outbound completion used a boolean QoS2 discriminator, allowing PUBACK to remove
   AwaitPubrec. It now uses exact state×ACK transitions and mutation-free failures.
8. Inbound EventAccepted completion was tied to a superseded connection generation.
   A stored Delivering operation token now survives same-session takeover.
9. CONNACK write failure could leak broker attachment ownership. Attachment and Will
   responsibility are now generation-fenced RAII guards with real write-failure tests.
10. Tenant capacity release polled only the acknowledging session. A bounded
    per-tenant ready scheduler now wakes another active session for QoS1/QoS2.
11. The recovery file limit ignored JSON expansion. Startup now validates a checked
    conservative encoded upper bound, and worst-case binary patterns are exercised.

## Remaining limitations and release practice

Retained wildcard replay scans a hard-bounded store. The broker is memory-first and
not crash durable. `$SYS`, WebSocket, MQTT 5, bridges, clustering, and shared
subscriptions are not claimed. The four-hour mixed MQTT soak is a manual release
exercise; the repository's ignored 60-second multi-generation restart soak is the
bounded local substitute used in this audit. These limitations do not leave an
applicable MQTT 3.1.1 MUST untested or failing.

Mosquitto remains a test/reference implementation only. It is not a NetbaIoT
runtime or production dependency.
