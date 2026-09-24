# MQTT protocol audit: M01–M07

Baseline: `d4f7612a350d8c66e670ec897f3e7263ed2f00ff` on `main`. The script's `e9c9b066...` is a historical audit reference, not the code tested here. The working tree was clean before `tests/mqtt_protocol_regressions.py` was copied from the repository's existing local audit branch. No historical checkout was used.

Normative sources: [MQTT 3.1.1 OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html) and [MQTT 5.0 OASIS Standard](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html). The root `AGENTS.md` still describes MQTT 5 as out of scope; the current implementation and `docs/mqtt.md` implement and document it. This audit preserves both versions.

The rebuilt baseline binary produced seven `FAIL` results in `target/mqtt-audit/before.json`. These are observations, not seven independent normative violations. Every case used an isolated loopback broker and synthetic credentials.

Final rebuilt-binary evidence is `target/mqtt-audit/after.json`: M01 `v311_started_expiry`, M02 `assigned_id_collision`, M03 `qos2_business_validation`, M04 `receive_maximum_resume`, M05 `will_no_local_takeover`, M06 `v311_duplicate_packet_id`, and M07 `payload_format_reason` each changed from **FAIL** to **PASS**.

| ID | Conclusion | Protocol requirement and trigger | Root cause and correction | Regression |
| --- | --- | --- | --- | --- |
| M01 | **CONFIRMED** | MQTT 5 §3.3.2.3.3, MQTT-3.3.2-5: the server **MUST** delete a subscriber copy only if expiry precedes the start of onward delivery. A v5 retained publication delivered to a 3.1.1 subscriber is legitimate. | The 3.1.1 connection checked expiry immediately before writing but never crossed the broker's `started_outbound` boundary. It could drop an already-started QoS1/2 exchange on expiry. Both versioned send paths now encode, then atomically cross that boundary under session generation before writing. Truly unsent expired copies are removed with accounting and Packet Identifier release; started exchanges retain their ACK/retransmission state. No MQTT 5 properties are sent on 3.1.1 wire. | `v311_started_expiry`: **FAIL → PASS**; existing broker tests for started QoS1/QoS2, unsent expiry and restart recovery. |
| M02 | **CONFIRMED** | MQTT 3.1.1 §3.1.3, MQTT-3.1.3-6: accepting an empty ClientId requires a unique assigned ID (**MUST**), and requires CleanSession=1. MQTT 5 §3.1.3/§3.2.2.3.7, MQTT-3.1.3-6/7 and MQTT-3.2.2-16: the assigned ID **MUST** be unused by any current Session and returned in CONNACK. | `generated-{generation}` could name a client-selected persistent Session and Clean Start could delete it. Candidate selection now checks the entire broker Session namespace under the same lock as attachment; it bounds retries to 1,024 and checks the client-ID byte limit. MQTT 5 preflights the Assigned Client Identifier CONNACK before mutating Session state. Explicit IDs still use authenticated `(DeviceKey, ClientId)` ownership. | `assigned_id_collision`: **FAIL → PASS**; Rust recovery/subscription preservation and 16-way concurrent allocation tests. The script's `generated-2` is a deterministic fixture collision, not a protocol-required naming scheme. |
| M03 | **CONFIRMED as a project reliability defect** | MQTT 5 §3.5/§4.3.3 permits a negative PUBREC before ownership; MQTT 3.1.1 §3.3.5.2 permits closing an unauthorized/rejected flow. Business JSON schema is **not** MQTT wire or Payload Format Indicator validation. The sender's QoS2 rules and receiver ownership rules do not require accepting every application payload. | Successful PUBREC stored a message whose JSON codec would always fail at PUBREL, causing a persistent poison transaction. New QoS2 messages now run a deterministic codec/permission validation before PUBREC; duplicate transactions do not revalidate or reingest. `JsonV1::validate_payload` parses without creating a discarded `event_id`; the actual event is created once at PUBREL and still needs normal EventAccepted admission. MQTT 5 returns PUBREC `0x83` for invalid business format; 3.1.1 closes before PUBREC. Transient EventBus capacity remains a separate retryable post-PUBREC responsibility. | `qos2_business_validation` checks no EventAccepted for invalid payload, ID reuse, and one later acceptance. Existing takeover/recovery QoS2 tests continue to exercise ownership. |
| M04 | **CONFIRMED local accounting gap; original normative claim corrected** | MQTT 5 §3.3.4, MQTT-3.3.4-7 gives the **sender** the Receive Maximum **MUST NOT**. §4.9, MQTT-4.9.0-1/2/3 resets send quota per network connection. The standard describes receiver DISCONNECT `0x93` on an excess; this is not a license to treat all persisted transaction IDs as slots already used on a new connection. The script's final QoS1 is intentionally from an over-quota sender. | Broker correctly cleared connection-local `inbound_window` on reconnect, but duplicate QoS2 PUBLISH bypassed charging it on that new connection. A generation-fenced broker operation now charges a recovered `AwaitPubrel` transaction once per connection when an actual PUBLISH retransmission is received. Repeating it on the same connection does not charge twice. PUBCOMP releases the slot; Session QoS2 ownership survives separately. | `receive_maximum_resume`: **FAIL → PASS**; Rust test verifies fresh window, duplicate idempotence, full window and release. `0x93` is asserted as this broker's bounded receive policy for the deliberately excessive sender, not as the sole legal reaction to a conforming client. |
| M05 | **CONFIRMED origin loss; Will interpretation stated** | MQTT 5 §3.1.2.5/§3.1.3.2.2 governs Will publication and delay; MQTT-3.8.3-3 says No Local **MUST NOT** forward to a connection with the publishing ClientID. Treating the Will as the disconnected client's publication is the broker's explicit interpretation of that rule. Zero-delay takeover sends the old Will; positive-delay same-Session resume suppresses it under MQTT-3.1.3-9. | Will routing passed `origin=None`, so a resumed No Local subscriber received its own Will. `WillGuard`, immediate and pending Will routing, retained origin and recovery now preserve the original `SessionKey` separately from `cancel_on_resume`. Other subscribers still receive the Will. ClientId bytes are charged to the bounded Will responsibility. NBMQ recovery changes from v5 to v6 for the new pending-Will field; v1–v5 remain readable, with old immediate Wills lacking origin metadata and old delayed Wills deriving origin from their preserved cancellation key. | `will_no_local_takeover`: **FAIL → PASS**; Rust tests confirm own No Local suppression, another subscriber delivery, retained origin, delayed Will recovery, and raw v5 pending-Will record compatibility. |
| M06 | **CONFIRMED receiving-side defect for abnormal sender input** | MQTT 3.1.1 §4.3.3, MQTT-4.3.3-1 says the sender **MUST** keep the same message and stop PUBLISH after PUBREL. MQTT-4.3.3-2 says the receiver **MUST** resend PUBREC for the same Packet Identifier until PUBREL and **MUST NOT** duplicate onward delivery. The changed payload in the script is an abnormal sender, not a conforming retransmission. | Connection ACL and broker byte-for-byte equality could reject the second PUBLISH before the existing transaction was classified. It now recognizes only `AwaitPubrel` as a retransmission before ACL and replies PUBREC, retaining the original message and budgets. `Delivering`/`EventAccepted` are classified as identifier-in-use, and completion allows new ID use with fresh ACL. Wire framing/UTF-8 validation still precedes this branch. | `v311_duplicate_packet_id` now also checks exactly one EventAccepted; existing broker accounting/original-message/ID-reuse tests. |
| M07 | **CONFIRMED** | MQTT 5 §3.3.2.3.2 allows the receiver to validate indicated UTF-8 (**MAY**) and, if rejecting, use PUBREC/PUBACK/DISCONNECT `0x99`. §3.5.2.1 lists `0x99` for PUBREC and `0x91` only for an identifier already in use. | `valid_broker_message` returned generic `Error::Invalid`; the connection mapped every such error to false `0x91`. The new QoS2 path checks PFI=1 before ownership and returns PUBREC `0x99`; a separate identifier classification produces `0x91` only for an occupied non-`AwaitPubrel` phase. Business schema rejection uses `0x83`. Rejected messages consume no ID/window/retained reservation. | `payload_format_reason` verifies `0x99` (or legal DISCONNECT policy), reuse and one later EventAccepted. |

## Test corrections and evidence

Changed source by issue: M01 `crates/netbaiot-transports/src/mqtt/mod.rs` and `v5_connection.rs`; M02 those connection files and `broker.rs`; M03 `crates/netbaiot-core/src/lib.rs`, `crates/netbaiot-codecs/src/lib.rs`, `crates/netbaiot-runtime/src/ingress.rs`, and both connection files; M04–M06 `broker.rs` plus their connection callers; M07 `codec/v5.rs` and `v5_connection.rs`. Recovery bounds/comments changed in `crates/netbaiot-runtime/src/limits.rs`; `docs/mqtt.md` and `docs/mqtt-session-recovery.md` now describe v6. The recovery fuzz harness, Python regression runner and MQTT interoperability workflow were updated as tests and CI.

- The historical script was not present in `main`; it was recovered intact from the local audit branch into the requested `tests/` path before the baseline run. Its imports resolve to the current conformance helpers. `--help` and Python compilation passed.
- The ACK helper now accepts both legal compact success encoding (two-byte Packet Identifier only) and reason-code encoding with an explicit, correctly bounded Property Length. The DISCONNECT helper accepts compact normal encoding and validates the explicit reason/property form. Both reject truncated or surplus property bytes.
- M03, M06 and M07 now check EventAccepted counts as well as wire ACKs. M03 also verifies that a rejected QoS2 Packet Identifier can be reused.
- M04 retains the deliberate over-quota input but labels `0x93` as the broker's selected bounded receive policy. The correction avoids presenting the sender's MUST as a receiver MUST for a conforming sender.
- The Rust ACL classification test now expects `IdentifierInUse` while PUBREL delivery is in progress. At that phase the ID remains occupied but the packet is no longer an `AwaitPubrel` retransmission; only after PUBCOMP is it a new message whose topic must pass ACL again.
- The fixture uses a fresh broker per case. `generated-2` is a known collision for the original generation scheme in that fixture; the Rust test additionally checks persisted-session preservation after recovery. The timing case waits 4.2 seconds for a three-second expiry and maintenance tick under a 30-second keepalive.
- The script closes clients and stops each server in `finally`; CONNECT evidence redacts credentials. CI builds `target/debug/netbaiot-server` before running it, triggers on script changes, fails on regressions, and uploads the JSON on success or failure.

## Compatibility and remaining scope

The wire protocol, MQTT 3.1.1 packet encoding, and MQTT 5 packet encoding do not change. NBMQ planned-restart recovery writes v6 records; older v1–v5 readers remain in the new binary, while an older binary cannot read a new v6 snapshot. `DeviceCodec` gains an additive default `validate_payload` method. `JsonV1` overrides it so deterministic preflight produces no event ID and allocates no retained broker state. Neither preflight nor PUBREC claims EventAccepted; that boundary remains at PUBREL after required sink admission.

The extra Will origin is bounded by the configured ClientId limit and charged to Will count/byte capacity. The additional connection receive slots are bounded by existing per-session inflight limits. No external broker, database, task per packet, or unbounded queue was added. MQTT QoS2 is not a business exactly-once or crash-durability guarantee.

The original machine-readable before/after packet evidence is preserved in
`docs/audit-evidence/mqtt-m01-m07-before.json` and `mqtt-m01-m07-after.json`.
Those older files name the baseline checkout but do **not** contain a binary
hash or a working-tree digest, so they are historical observations rather than
complete provenance for a release candidate. The release-acceptance evidence
below supplies those identifiers for fresh runs.

## Release acceptance, 2026-09-24

The M01–M07 implementation patch is commit `65f6ba6a8dbc0796a1d14d8f67d33815372c9a8f`
against `d4f7612a350d8c66e670ec897f3e7263ed2f00ff`. The subsequent broker
hotspot patch is `a75faded1c770fac8d67c0e9c226dfcc22ff384c` and is a
separate performance change. `main` and `origin/main` advanced to that commit
while this acceptance task was running; this task did not request or perform a
commit or push. The remaining acceptance edits are identified by the final
working-tree manifest in `docs/audit-evidence/mqtt-release-candidate-manifest.json`.
HEAD alone is **not** the tested version while those edits remain uncommitted.

The M01–M07 table above identifies source corrections. The ACK and DISCONNECT
parsers, EventAccepted counts, M04 receiver-policy wording, identifier-in-use
expectation, collision fixture, and expiry wait described under “Test
corrections and evidence” are test oracle or fixture changes; they do not
constitute separate broker fixes. The corrected M03, M04, M06 and M07 raw cases
were rerun against the isolated `d4f7612` server binary (SHA-256
`a879b781823bc4f56d71373df7a98bdd4e7a5bba5ba86e7f17380acb7ae22bea`).
All four still failed. Their fresh JSON files are
`docs/audit-evidence/mqtt-old-baseline-*.json`; the archived source checkout is
not a Git worktree, so its test JSON says `checkout_head: unknown`, while the
source SHA and rebuilt binary hash here identify the old implementation.

Fixed NBMQ v1–v5 samples under
`tests/mqtt_conformance/fixtures/mqtt_recovery/` were emitted by binaries from
five pinned historical commits. Their README records each source commit, binary
hash and fixture hash. v1–v4 empty samples prove structural read compatibility.
Additional nonempty v1–v4 samples contain a persistent subscription, a retained
QoS1 publication, and inbound QoS2 Packet Identifier 7 awaiting PUBREL. The v5
sample contains that QoS2 state, a No Local subscription, and a delayed Will.
The Rust tests read the fixed bytes, rewrite each state as v6, read it again,
verify QoS and retained state, and verify that the v5 Will
reaches another subscriber without forwarding to its original No Local client.
For old immediate Wills without a cancellation key, the origin remains unknown;
No Local cannot be reconstructed from a ClientId that was never recorded.

The default-limit near-capacity test admitted 128 persistent sessions with
14,080 offline QoS1 messages, 256 pending delayed Wills carrying maximum-length
ClientIds, and 1,024 retained messages. Its v6 file was 199,727,012 bytes,
below the final 202,195,044-byte bound (98.8%), and recovered with consistent
accounting. Acceptance review also found a new configurable-bound defect:
the v6 delayed-Will record stores both cancellation and origin ClientIds, but
only one was charged in the file bound and neither was included in the per-record
ceiling. The patch adds the second ClientId to the checked whole-file formula
and both ClientIds to the record ceiling. The default whole-file ceiling is now
202,195,044 bytes. A legal 40,000-byte ClientId with a 20,000-byte Will payload
now commits and recovers; its record exceeds the previous ceiling.

The nonempty historical v2 sample exposed a second upgrade defect. Its session
has credential/permission provenance but no codec ID/version, which v2 never
recorded. The new reader restored it, but `encode_session_meta` rejected it when
writing v6, blocking planned shutdown. The v6 writer now records such incomplete
legacy provenance as unknown. It carries the QoS and retained state into the v6
image without fabricating a codec; authenticated reconnect still resets the
session under the existing conservative profile rule. The regression verifies
both the v2→v6→v6 read path and that reset behavior. These two recovery-boundary
fixes are the only new implementation changes made during acceptance.

For rollback, a v6 file from the rebuilt candidate was copied to an isolated
temporary directory and opened by the genuine `d4f7612` server binary. The old
process exited with `Error: Invalid`, and the copied file's SHA-256 stayed
`b4bbdd15bd1f2a2b58ba734426f183a6e2f38a8347087da2bf0e1559f39727af`
before and after. That file was an empty but valid v6 planned-shutdown image.
The source recovery directory was untouched. The operational upgrade and
rollback conditions, including the distinction between no new state change and
newly processed business, are in `docs/mqtt-session-recovery.md`.

The existing Rust checks workflow runs all workspace tests for Rust source and
fixture edits. `mqtt-interop.yml` explicitly watches
`tests/mqtt_protocol_regressions.py` and `tests/mqtt_conformance/**`, so the
updated raw runner, common binary selector and fixed samples trigger the
external MQTT gate on a push. The final local gate used the witnessed isolated
binary `/private/tmp/netbaiot-accept-final-target/debug/netbaiot-server`
(SHA-256 `93a1dcee678a4aa60eb9ec011049378cd2f14e6ba634e322837a2fb869137e32`);
its hash was unchanged before and after each Python suite. The final local
results are Rust fmt/Clippy/workspace tests on 1.88.0 and stable **PASS**,
MQTT release gate **76/76 PASS** with normative coverage **125/125**, MQTT 5
raw and Mosquitto clients **PASS**, seven M01–M07 cases **PASS**, recovery fuzz
5,000 runs **PASS**, and the 98.8%-of-bound v6 commit/recover test **PASS**.
The 60-second soak and a production-state upgrade rehearsal were **NOT RUN**
in this acceptance pass.
No GitHub workflow can validate the still-uncommitted acceptance diff; remote
CI for that exact candidate is **NOT RUN**. Exact local command results and
artifact hashes are recorded in the candidate manifest, with unrun checks
marked **NOT RUN**.
