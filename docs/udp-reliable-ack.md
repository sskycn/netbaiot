# UDP v1.1 signed acceptance ACK: implementation and validation

Baseline: `ef9a8837e39aafc7777997be2927a58fd051a071`.
Final source: the `codex/udp-reliable-ack` commit containing this report.
Decision: **KEEP**. No push. Measurement date: 2026-09-23.

## Scope and compatibility review

The existing NBI1 request, shared device listener/classifier, management/business
listeners, HTTP, MQTT 3.1.1 and generic TCP wire protocols are unchanged. NBA1 is
an additive UDP v1.1 response. Clients ignoring responses remain compatible, but
must adopt exact-datagram retries to gain the new reliability guarantee. No UDP
session, command downlink, endpoint registry, encryption, fragmentation, NACK,
ACK scheduler, queue, or persistence is added.

`netbaiot-protocol` schemas and `PROTOCOL_VERSION` remain unchanged. The existing
`AuthCache::verify_signed` API is retained; the new verified-request capability
encapsulates the private decoded key and invalidation epoch. The transport crate's
`ReplayWindow::check/commit` Rust helper API now takes credential version, and
`check` returns `ReplayDecision`; external callers of that low-level helper must
adapt. Wire compatibility is covered by explicit byte-order/HMAC tests.

## Acceptance, retry, and security contract

NBA1 layout is `NBA1[4] | credential_version:u32be | boot_id[16] | sequence:u64be |
HMAC-SHA256[32]`. The HMAC covers the first 32 bytes with the same decoded 32-byte
key as NBI1. Total length is 64 bytes; there is no status or event ID.

New packets authenticate and authorize, pass clock/replay/resource checks, enter
Ingress, cross EventAccepted, commit replay, then sign and try to send. Accepted
duplicates authenticate again locally, skip codec/presence/EventBus/event-ID
creation, and re-ACK. The bitmap key includes authenticated DeviceKey, credential
version, and boot ID. First accepted content wins for an immutable sequence.

Malformed/authentication/version/clock/old-replay/codec/authorization/admission/
EventBus failures and draining are silent. A held lifecycle guard includes bounded
auth work and receipt completion. Invalidation fences synchronous signing and the
nonblocking send under the auth-cache lock. Suppressed/failed receipts never undo
acceptance. There is no second provider lookup and no lock across an await.

NBA1 confirms **EventAccepted only**, equivalent in level to HTTP 202/MQTT QoS1
PUBACK/TCP acceptance. It is not a sink's final ACK, business persistence or command
execution. A codec CommandAck remains an application event.

Retry the exact original NBI1 bytes, including timestamp and HMAC, with bounded
backoff and jitter before timestamp expiry and before the sequence slides out of
64 slots. Unconfirmed beyond those limits means uncertain. Records expire after
120 seconds by default; duplicate ACKs do not extend expiry. Memory-only replay is
rebuilt on both planned and unexpected restart, so previously accepted datagrams
can be ingested again. Stable source_message_id and business idempotency remain
necessary; EventBus replay event_id stability does not make renewed UDP ingestion
exactly-once.

Unauthenticated response count is zero. Minimum structurally valid NBI1 is 76 bytes;
NBA1 payload-byte ratio is 64/76 = 0.8421. Captured authenticated datagrams can still
reflect a smaller receipt to a spoofed source during the valid window. Existing
IP/process rate limits apply before the duplicate path. Payloads are not encrypted.

## Tests actually run

| Command / gate | Result |
|---|---|
| `cargo +1.88.0 fmt --all -- --check` | pass |
| `cargo +1.88.0 check --workspace --all-targets` | pass |
| `cargo +1.88.0 clippy --workspace --all-targets --all-features -- -D warnings` | pass |
| `cargo +1.88.0 test --workspace --all-targets --all-features` | 162 passed, 0 failed, 4 ignored; foundation bench executed |
| `cargo +1.88.0 test --workspace --all-features` | 162 passed, 0 failed, 4 ignored |
| `python3 tests/mqtt_conformance/run.py --release-gate --no-build` | 76/76 pass, 125/125 normative requirements; Mosquitto broker/client reference checks |
| `RUSTUP_TOOLCHAIN=1.88.0 bash scripts/tutorial_smoke.sh` | pass, verifies signed NBA1 |
| `cargo +1.88.0 test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture` | 1 passed in 62.76 s |
| `cargo +nightly fuzz run udp_envelope target/udp-ack/fuzz-corpus -- -max_total_time=60 -timeout=5 -max_len=65540` | 566,406 executions in 61 s, no crash |
| Baseline/final default limits comparison; Python ACK vectors | all 112 defaults identical; valid ACK + 69 rejection vectors pass |

The 13 UDP tests include 10 added tests and three adapted existing tests. Coverage:
all 64 ACK-byte tamper positions; independent HMAC verification; exact wire layout;
real socket ACK loss/retry yielding **two ACKs and one DeviceEvent**; bad HMAC and
other invalid inputs yielding **zero replies**; forbidden command payload; full or
closed EventBus; admission rejection followed by successful same-sequence retry;
WouldBlock after acceptance followed by duplicate re-ACK; stable event_id during
10,000 duplicates with one provider call and one replay entry; first-content wins;
100/102/101 ordering; sequence MAX/64-slot edges; clock edges/overflow; credential
rotation, revoked publication and old in-flight signer fencing; replay count limits
across versions/devices/tenants; source rate limits; draining and no UDP session.
The pending required test sink proves NBA1 does not wait for final business ACK.

The ignored restart soak was run separately. Three unrelated manual benchmarks
(queue depth, 10/50/100 MiB MQTT recovery, MQTT route preflight) were not run. No
new long-duration/WAN/hardware-device campaign or dedicated SIGKILL-loss campaign
was run. Standard workspace restart/corruption/slow-sink tests did execute.
`configs/resource-limits.json` already omits presence_ttl_ms=3600000 at baseline;
all 111 listed values match, and the omission was not changed here.

## Performance

macOS 26.6.2 ARM64 development host, loopback, Rust 1.88 release binaries. Three
alternating baseline/final pairs, one-second warmup, eight-second saturated UDP
send windows, one authenticated device, 251-byte JSON / 337-byte NBI1, local audit
sink. The client continuously drains ACKs on one bounded reader thread. Management
counters measure **accepted**, not attempted, messages; no queue rejection/auth
miss occurred after warmup. Flooded receive sockets do drop many offered requests,
so these numbers are not a loss-free capacity promise. CPU is server process time
sampled with `ps`, excluding client CPU. No heavy validation ran during final pairs.

| Metric (median) | Baseline | Final | Change |
|---|---:|---:|---:|
| UDP accepted/s | 128,819 | 88,537 | -31.3% |
| CPU µs/accepted | 11.12 | 15.42 | +38.7% |
| ACK RTT P50, µs | N/A | 21.54 | new receipt |
| ACK RTT P95, µs | N/A | 25.46 | new receipt |
| ACK RTT P99, µs | N/A | 33.54 | new receipt |

RTT uses 100,000 sequential authenticated confirmations, 0 missing
receipts; the probe caps RTT samples at 100,000. All three final throughput runs
received exactly one ACK per accepted datagram, with zero send failures. Observed
idle/post-run server task counts stayed at five.

The >15% regression was investigated with a temporary **diagnostic, never shipped**
build that signs and fences but replaces the send closure with `Ok(ack.len())`.
Two five-second paired runs measured 119,992 accepted/s and 11.94 µs/accepted for
sign-only, versus 87,954/s and 15.52 µs for actual emission. Most additional cost is
associated with loopback UDP send (~3.58 µs/accepted including kernel/receiver
interaction); signing/fencing/replay identity cost is much smaller. Code review
found no per-ACK allocation, duplicate HMAC verification/provider lookup, new task,
queue, blocking send, or duplicate serialization. Keep the safety fences and accept
the measured cost of a signed network receipt. Re-measure on deployment hardware.

Reproduce with:

```bash
cargo +1.88.0 build --release --locked -p netbaiot-server
cargo +1.88.0 build --release -p netbaiot-loadgen --bin udp_ack
python3 scripts/perf/udp_ack_compare.py --baseline /path/to/ef9a883-server \
  --final target/release/netbaiot-server --output target/udp-ack/comparison.json
```

The baseline binary was built and preserved from the exact baseline before edits.
[Final raw measurements](performance/udp-ack/comparison.json),
[diagnostic cost split](performance/udp-ack/sign-cost.json),
[binary hashes](performance/udp-ack/builds.json), and
[validation summary](performance/udp-ack/validation.txt) are committed with this report.

## Memory and remaining limitations

Replay value stays 24 bytes (max, bitmap, expiry). Measured key+value inline payload:
**88 → 96 bytes per entry**, +8 bytes for credential version plus alignment.
At the default max_replay_entries=1024, extra live-entry payload is **8192 bytes**;
including spare HashMap buckets, estimate about **16 KiB** extra bucket payload at
that default ceiling with this toolchain's growth policy. This is a layout estimate,
not a measured RSS bound; allocator/control-byte overhead remains separate and keys'
existing bounded identifier allocations are unchanged. No per-slot payload/event ID
is stored. Four counters add 32 bytes globally, and one fixed 64-byte ACK buffer is
local to the single receive owner. Version changes share existing count limits.

ACK loss is expected; the server stores no retransmission obligation. Reliability
requires valid same-datagram retries. A sequence can age out due to either clock
skew or newer traffic. Invalidation may conservatively suppress a pending receipt
for an unrelated device too. Restart may cause duplicate ingestion. None of these
limits are business exactly-once, encryption, command delivery, or crash durability.
