# Legacy cleanup dependency audit

Baseline: `21a694576a84436c1d27a69f59ea1d1a32e5006b`, after the independently validated second-round optimization. All four Actions and Native recovery/DX Ubuntu/macOS/Windows jobs passed. Baseline checks and frozen binaries are recorded under `target/legacy-cleanup/baseline/`.

Version namespaces are distinct: Business Stream V1, Business RPC V2 and current Business RPC V3; MQTT recovery NBMQ v1–v6 (current v6); EventBus restart spool NBSP v1–v3 (current v3). Current HTTP/device JSON schema version 1 and MQTT 3.1.1/5.0 are independent and remain supported. The user's current-only cleanup instruction supersedes the previous historical recovery compatibility requirement.

| Classification | Surface | Action / dependent invariants |
|---|---|---|
| DELETE | `apps/netbaiot-server/src/business_stream_v1.rs` and V1 listener/dispatcher branches | Remove framing, hello/subscription/event/ACK loop and V1 token setting; preserve event ACK/restart correctness via current RPC |
| DELETE | Protocol `StreamClientFrame` / `StreamServerFrame` | V1-specific envelopes, hello/ready validators and compatibility tests removed; shared EventDelivery/EventAck/EventFilter remain |
| REWRITE | Root client `EventStream` and CLI event consumer | Preserve bounded, manually acknowledged subscription interface using the existing current RPC client; remove V1 transport driver |
| DELETE | V2 `BusinessRpcFrame`, Hello/Ready `BusinessLimits`, version/event-window constants | No standalone length-prefixed JSON V2 transport or negotiation remains |
| KEEP | Method DTOs, authenticated-device wire identity, structured RpcError, authorization role semantics | Current V3 uses these shared types; retain their JSON bytes and validation without V2 frame wrappers |
| REWRITE | Parent server `business_rpc.rs` | Keep principal/TLS validation, owned lifecycle leases, control/auth/command state, admission fencing; remove V2 reader/writer/event/command connection logic and reply adapters |
| REWRITE | Parent client `business_rpc.rs` | Keep common TLS/errors/auth-handler helpers used by V3; remove V2 config/client/driver/event-delivery and fallback helpers |
| KEEP | Current V3 binary bootstrap, framing, stream mux, current request/body/reply wire shape | No wire-format change; version remains needed to reject old/future bootstrap |
| DELETE / REWRITE | Config `version`, `allow_v1`, optional current-protocol dispatch | Replace with a single current listener/config path. Old config fields have no aliases and fail under deny_unknown_fields policy |
| REWRITE | V2/V1 tests, SDK integration, loadgen, examples, Windows diagnostic probes, xtask gates | Delete compatibility assertions; migrate relevant auth/replay/ACK/commands/cancellation/shutdown/quotas to current RPC. Preserve fail-cleanly old/future handshake tests |
| DELETE | NBMQ v1–v5/legacy JSON recovery readers, version ceilings, migrations and fixtures | Keep only bounded streaming NBMQ v6 decoder/writer/checksum/trailer, auth/profile/QoS validation and coherent snapshots |
| DELETE | NBSP v1/v2 compatibility decoding and fixture blobs | Keep current v3 generation/count/digest/trailer and durable fsync/rename/ACK ownership behavior |
| REWRITE | Recovery tests/fuzz | Keep minimal old/future/corrupt version rejection, current golden framing, hostile lengths/checksums, replay/failure/duplicate and resource invariants |
| KEEP | MQTT 3.1.1/5.0, TCP/UDP device framing, management `/api/v1`, device JSON schema v1 | Names containing v1/v2 are not mechanically removed |
| REWRITE | Current docs, schema/config reference, public exports and CI | State current-only support; remove obsolete gates or migrate their correctness coverage. Preserve historical release/performance records as dated evidence |
| DELETE if unused | Dependencies/features/imports/errors/conversions/wrappers | Cargo tree plus manual call-site review; cargo-machete is not installed, no new tool dependency will be added |

No implementation is deleted before the baseline checks pass. Protocol removal, recovery removal and generic dead-code/doc cleanup use independent compilable commits where practical. Old format inputs reject cleanly without fallback/partial restoration. Important legacy tests are classified by behavior, migrated before their corresponding implementation is removed, and exercised with default/serial test threads repeatedly. The final report lists all retained historical references and their reasons.
