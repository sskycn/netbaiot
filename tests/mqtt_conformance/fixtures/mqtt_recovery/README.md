# Current NBMQ v6 golden fixtures

These files contain the current format only. NBMQ v1–v5 fixtures and their migration tests were removed with the current-only compatibility break. Minimal eight-byte unsupported headers test rejection without keeping historical decoders.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `v6-empty-rollback.nbmq` | 100 | `b4bbdd15bd1f2a2b58ba734426f183a6e2f38a8347087da2bf0e1559f39727af` |
| `v6-session-qos-will.nbmq` | 696 | `232277300a4cd6311702271032d8d6d82d741f6554c0db7922eca0bab56345c1` |

`v6-empty-rollback.nbmq` is the previously committed current-format empty snapshot; its historical acquisition/rollback evidence remains in `docs/audit-evidence/mqtt-v6-rollback.json`.

`v6-session-qos-will.nbmq` freezes bytes emitted by the unchanged pre-cleanup NBMQ v6 writer at `ed7336f` (the same writer as baseline `21a6945`). The fixed state contains a persistent MQTT 5 session, No Local subscription, inbound QoS2 awaiting PUBREL, and a delayed Will with an explicit origin. The source state was loaded by the pre-cleanup implementation before its historical readers were removed. This is a current-format golden, with no generator or migration implementation retained.

`broker/tests/recovery_current.rs` verifies the current decoder, semantic restoration and byte-for-byte re-emission of both fixtures. Current writer framing, checksum, count/byte trailer and payload bytes stay unchanged.
