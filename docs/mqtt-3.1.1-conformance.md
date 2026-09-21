# MQTT 3.1.1 conformance checklist

Statuses describe current evidence, not the presence of code. `PROFILE` is reserved
for an intentional NetbaIoT restriction that does not contradict an MQTT MUST.
The normative source is MQTT 3.1.1 with Errata 01.

| MQTT 3.1.1 area | Status | Current evidence / limitation |
|---|---|---|
| Incremental fixed header and Remaining Length | PASS | `FRAMING-001`, `REMAINING-001`, packet unit tests, `mqtt_fixed_header`, `mqtt_remaining_length`, `mqtt_packet` fuzz targets |
| Required fixed flags | PASS | `HEADER-001`, `DIFF-FLAGS-001`, `all_control_packet_flags_and_identifiers_are_strict` |
| UTF-8 and binary fields | PASS | `UTF8-001`, CONNECT/topic/filter unit vectors reject NUL, controls, noncharacters, surrogates and invalid encodings |
| CONNECT validation / one CONNECT | PASS | `CONNECT-001..005`, `SEQUENCE-001` |
| CONNACK / Session Present | PASS | `CONNACK-001`, `SESSION-001`, `DIFF-SESSION-001`, `MQTT-CONNACK-WRITE-FAIL-CLEANUP-001`, restart subprocess tests |
| CleanSession=1 | PASS | `SESSION-001`; prior state is removed and Session Present remains zero |
| CleanSession=0 | PASS | `SESSION-001`, `SESSION-OFFLINE-001`, restart subprocess tests |
| Duplicate ClientId | PASS | `DIFF-DUPLICATE-CLIENT-001`, `WILL-TAKEOVER-001`, generation-fencing unit tests |
| QoS0 | PASS | `QOS0-001`, `DIFF-QOS-001`, Mosquitto CLI matrix |
| QoS1 / PUBACK / DUP / reconnect | PASS | `QOS1-001`, `QOS1-DUP-001`, original-order reconnect unit test, subprocess restart test |
| Inbound QoS2 | PASS | `QOS2-IN-001`, `QOS2-IN-SESSION-TAKEOVER-001`, end-to-end one-DeviceEvent test, state/failure/recovery unit tests |
| Outbound QoS2 | PASS | `QOS2-OUT-WRONG-PUBACK-001`, `QOS2-OUT-WRONG-PUBCOMP-001`, reconnect-stage and four-generation restart tests, Mosquitto CLI matrix |
| Packet Identifier lifecycle | PASS | zero-ID raw vectors, allocator collision/limit/unit and restart tests |
| SUBSCRIBE / SUBACK | PASS | `SUB-001`, `SUB-002`, `DIFF-SUB-001`, retained-capacity transaction rollback test |
| Exact / `+` / `#` matching | PASS | raw and differential subscription cases plus trie tests |
| `$` wildcard rule | PASS | matcher regression covers root `#`, root `+`, and explicit `$` filters; no `$SYS` service is claimed |
| Re-subscribe retained replay | PASS | `SUB-RESUB-001`, `DIFF-SUB-001` |
| UNSUBSCRIBE / UNSUBACK | PASS | `UNSUB-001`, `DIFF-SUB-001`, Mosquitto CLI matrix |
| Persistent offline QoS1/QoS2 | PASS | `SESSION-OFFLINE-001`, `MQTT-TENANT-INFLIGHT-WAKE-QOS1-001`, `MQTT-TENANT-INFLIGHT-WAKE-QOS2-001`, CLI matrix, bounded queue and restart tests |
| Retained create/replace/delete/replay | PASS | `RETAIN-001`, `DIFF-RETAIN-001`, CLI and recovery tests |
| Will QoS0/1/2 and RETAIN | PASS | `WILL-001`, `DIFF-WILL-001`, `WILL-RESOURCE-FAILURE-001`, Mosquitto CLI matrix |
| DISCONNECT suppresses Will | PASS | `WILL-001`, Mosquitto CLI matrix |
| EOF/timeout/protocol/takeover/shutdown Will | PASS | `WILL-001`, `WILL-TAKEOVER-001`, `WILL-SHUTDOWN-001`, keepalive and malformed cases |
| Keep Alive / PING | PASS | `KEEPALIVE-001`, `DIFF-PING-001`, `DIFF-KEEPALIVE-001` |
| Whole-packet deadline | PASS | `SLOWLORIS-001`; independent of MQTT keepalive |
| Malformed and illegal sequences | PASS | `CONNECT-003`, `HEADER-001`, `REMAINING-001`, `SUB-002`, `SEQUENCE-001/002`, packet unit/fuzz suites |
| ACL and identity isolation | PROFILE (PASS) | `SECURITY-ACL-001`, `SECURITY-SESSION-001`; canonical authenticated device namespace |
| Planned restart recovery | PASS | session/QoS1/QoS2/retained/Will subprocess, >1 MiB snapshot, and `MQTT-RECOVERY-BINARY-WORSTCASE-001` |
| TLS interoperability | PASS | `tests/run_mosquitto_tls_interop.py`; test CA/hostname verification succeeds and untrusted CA fails |
| Abrupt crash durability | PROFILE | recent memory state may be lost; no crash durability claim |
| MQTT 5, WebSocket, shared subscriptions, bridges | NOT_APPLICABLE | explicitly outside this embedded MQTT 3.1.1 profile |

The dependency-free raw suite is `tests/mqtt_conformance/run.py`; its stable expected
IDs and the exact 125-requirement evidence map are in
`tests/mqtt_conformance/catalog.json`. Each run writes diagnostic results to
`target/mqtt-conformance/results.json`. Developer core mode has 30 NetbaIoT raw
cases. `--release-gate` requires Mosquitto clients, TLS, the isolated 11-case broker
differential, restart evidence, and ten named Rust fault invariants; missing required
dependencies are `SKIPPED_REQUIRED` and fail the gate. The current release run is
55/55 PASS, including `NORMATIVE-COVERAGE-001` at 125/125.

The real-client suite is `tests/run_mosquitto_cli_interop.py`, which forces
`-V mqttv311`. It covers authentication, QoS0/1/2, exact/`+`/`#`, unsubscribe,
retained replace/delete/replay, persistent offline QoS1/2, and Will QoS0/1/2.
Paho is optional and was unavailable in the current environment; historical Paho
2.1.0 output is not counted as current release-gate evidence.
