# MQTT 3.1.1 conformance checklist

Status is based on code plus the cited automated or Paho 2.1.0 test. “Profile” means
the protocol feature is implemented but NetbaIoT authorization intentionally limits
the tenant/device namespace. This is not an MQTT 5 implementation.

| MQTT 3.1.1 area | Status | Evidence / limitation |
|---|---|---|
| Incremental fixed header and Remaining Length | Supported | `mqtt::packet` split/multi-packet/boundary tests; `mqtt_fixed_header`, `mqtt_remaining_length`, `mqtt_packet` fuzz targets |
| Required fixed flags | Supported | `all_control_packet_flags_and_identifiers_are_strict` |
| UTF-8 and binary fields | Supported | CONNECT/UTF-8 tests reject NUL, controls, noncharacters, invalid encodings; Will/password remain binary where required |
| CONNECT validation / one CONNECT | Supported | packet tests and connection state-machine test |
| Protocol level rejection | Supported | unsupported level receives CONNACK 1 |
| CONNECT authentication | Supported | bound AuthContext; real 10,000-PUBLISH test observes one provider call |
| Empty ClientId | Supported | generated only for CleanSession=1; CleanSession=0 rejected with CONNACK 2 |
| CONNACK / Session Present | Supported | broker state tests and real subprocess restart tests |
| CleanSession=1 | Supported | old owned state removed; Session Present=0 test |
| CleanSession=0 | Supported | subscriptions, queues, QoS and packet allocator persist; Paho offline and subprocess restart tests |
| Duplicate ClientId | Supported | generation-fenced replacement; old owner cancelled; SessionKey includes DeviceKey |
| PUBLISH QoS0 | Supported | Paho exact test and socket integration |
| PUBLISH/PUBACK QoS1 | Supported | Paho and 10,000 real publish test; restart DUP/same-ID test |
| PUBLISH/PUBREC/PUBREL/PUBCOMP QoS2 inbound | Supported | duplicate socket sequence, Paho, broker model test, four-generation restart test |
| QoS2 outbound | Supported | Paho, explicit broker stage test, restart before PUBREC and before PUBCOMP |
| Packet Identifier lifecycle | Supported | zero rejected; bounded allocator skips active outbound IDs; restart tests retain ID |
| DUP retransmission | Supported | QoS1/QoS2 PUBLISH restart asserts DUP; PUBREL uses mandatory fixed flags and stored retransmission state |
| SUBSCRIBE with multiple filters | Supported | parser validates nonempty/filter count/QoS; broker updates each filter and returns per-filter grant/failure |
| Exact subscription | Supported (profile) | Paho exact subscribe/unsubscribe; bound namespace ACL |
| `+` wildcard | Supported (profile) | trie unit test and Paho test |
| `#` wildcard | Supported (profile) | trie unit test and Paho test |
| `$` wildcard rule | Supported | trie and matcher unit tests; no `$SYS` service is exposed |
| Re-subscribe | Supported | same filter replaces QoS and does not increase count |
| UNSUBSCRIBE/UNSUBACK | Supported | packet parser and Paho unsubscribe |
| Publish/subscription QoS minimum | Supported | `routing_uses_minimum_qos...` state test |
| Persistent offline QoS1/QoS2 | Supported | bounded queue; Paho offline reconnect test |
| Retained store/replace | Supported | broker state test, Paho retained test, recovery round trip |
| Zero-length retained delete | Supported | broker and Paho tests |
| Retained wildcard replay | Supported | Paho test; bounded-store scan is a known scaling limitation |
| Last Will QoS0/1/2 | Supported | raw QoS1 socket test and Paho QoS0/1/2 matrix |
| Will RETAIN | Supported | Paho retained Will test |
| DISCONNECT suppresses Will | Supported | socket and Paho tests |
| Abnormal EOF publishes Will | Supported | socket and Paho tests |
| Keep Alive / PING | Supported | PING round trip in restart test; 1.5× inactivity timeout; separate whole-frame deadline |
| Slow consumer bounds | Supported with shedding policy | bounded sender/inflight/offline count+bytes; overflowing subscriber is cancelled/shed, not allowed to grow |
| Planned restart recovery | Supported | atomic versioned snapshot; real QoS1/QoS2/retained/session subprocess tests |
| Abrupt crash durability | Not promised | recent memory state can be lost; no continuous disk persistence |
| MQTT 5 packets/properties | Intentionally unsupported | connection closes on unsupported packet/version; no MQTT 5 semantics |

The Paho matrix lives at `tests/mqtt_paho_interop.py`. It is deliberately not a
Cargo dependency: install a mature client in an isolated test environment and run
it against a local server. The recorded validation used `paho-mqtt 2.1.0` with
protocol `MQTTv311`.
