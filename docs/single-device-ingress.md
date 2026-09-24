# Single Device Ingress implementation and validation

> Historical report: describes the revision measured when it was written, not the
> current device protocol surface. Device HTTP has since been removed. Current
> behavior, migration, tests and measurements: [removal report](remove-device-http.md).
> Original measurements are retained; old HTTP benchmark tools can be retrieved
> from baseline `945fe5e386d623c32e2c7d2d0568fe0c058107ec`.

Baseline: `bae843e302ce931f859523ae425ed17db0e4b820` (local and remote main matched).
The worktree was clean at the start. Final revision: the commit containing this
report; obtain its SHA with `git log -1 --format=%H -- docs/single-device-ingress.md`.
The final delivery message records that SHA. No push was performed.

Decision: KEEP. Production code, protocol compatibility and the measured connection
path passed the checks below. A separate delivery worktree keeps concurrent release
script/README edits in the original checkout untouched.

## Architecture and configuration

One TCP device listener performs TLS, bounded application-prefix classification,
then dispatches the original HTTP/MQTT/TCP connection handlers. UDP binds the same
resolved SocketAddr, including when port zero is requested. The independent
management listener remains available until recovery and required delivery
completion. Optional business TCP and its loopback/security constraints are unchanged.

The listener part of a development configuration is now:

```json
{
  "device_ingress": "127.0.0.1:8080",
  "management_http": "127.0.0.1:9090",
  "business_tcp": null
}
```

Use the complete `configs/development.json` or `configs/tutorial.json` for credentials,
sinks and other required fields. Production can bind `device_ingress` to
`0.0.0.0:443` with TLS files and a confirmed sink: TCP/443 serves HTTPS, MQTTS and
framed TLS TCP; UDP/443 remains authenticated, unencrypted NBI1.

This pre-1.0 server deployment configuration has no repository contract requiring
the former four addresses to stay readable. `device_http`, `mqtt`, `tcp`, `udp` are
rejected by strict configuration deserialization (`read_config` returns
`Error::Configuration`), even if mixed with the new field. Operators explicitly
choose the shared address and migrate device destinations/firewall rules.
Public protocol v1, management API, Generic TCP and UDP wire formats are unchanged.
Standard MQTT 3.1.1 clients work without an SDK, ALPN, or custom preface.

## Classification and security review

- Fixed 12-byte prefix storage, replayed intact through `PrefixedStream` before
  direct underlying reads. Tests cover fragmented/coalesced streams and writes.
- HTTP recognizes GET/POST/PUT/DELETE/HEAD/OPTIONS/PATCH/CONNECT/TRACE plus SP.
  Extensions and HTTP/2 are not classified. Hyper still validates full requests.
- MQTT uses the existing fixed-header and Remaining Length decoder, validates the
  CONNECT header and `00 04 MQTT` name, and reads the protocol level. Level 4 is
  supported. Other levels go only to the existing parser for standard CONNACK=1
  and close (MQTT-3.1.2-2); silently closing at classification broke CONNECT-002,
  so the existing conformance assertion and response were preserved.
- Generic TCP validates 1..max_tcp_frame_size plus JSON object/whitespace start.
  Validated limits cap frames at 1 MiB, making the length's first byte zero,
  disjoint from HTTP method initials and MQTT 0x10. There is no parser fallback.
- Count, bytes and per-IP admission happen before task creation/TLS. A pending
  lease transitions to protocol accounting without re-acquisition/double release.
  Global/IP rate limits and existing downstream admission/HTTP/MQTT limits remain.
- TLS, detection and first packet/header read share the admission-time connect
  deadline; existing auth and HTTP request/write budgets remain separate. Slow
  prefixes cannot refresh timeouts. TLS errors, EOF, timeout, resource and invalid
  prefixes close safely with debug-level diagnostics; TLS may send its standard alert.
- Device HTTP is forced to `HttpRole::Device`, even if the caller supplies management
  services. Tests deny management paths with both device and admin credentials.
- Shutdown tests verify shared TCP/UDP stop while spool failure keeps management
  reachable/unready; management closes only after storage repair/durable completion.

No new broker/codec/EventBus/command routing behavior, dependencies in the runtime,
UDP encryption or persistence mechanism was introduced. HMAC is reused as a test
dependency to exercise a real signed UDP datagram.

## Validation actually executed

All successful final runs used Rust 1.88.0 unless a command explicitly selects nightly
or invokes the repository's default-toolchain helper. Socket tests needed execution
outside the filesystem/network sandbox. Logs remain under `target/single-ingress/`
in the original checkout; machine-readable measurements are in
[single-device-ingress-results.json](single-device-ingress-results.json).

```bash
cargo +1.88.0 fmt --all -- --check
cargo +1.88.0 check --workspace --all-targets
cargo +1.88.0 clippy --workspace --all-targets --all-features -- -D warnings
cargo +1.88.0 test --workspace --all-targets --all-features
cargo +1.88.0 test --workspace --all-features
python3 tests/mqtt_conformance/run.py --release-gate --no-build
cargo +nightly fuzz run device_classifier -- -max_total_time=30 -max_len=16
cargo +1.88.0 test -p netbaiot-server --test server subprocess_graceful_restart_sixty_second_soak -- --ignored --nocapture
bash scripts/tutorial_smoke.sh
python3 -m unittest discover -s scripts/perf -p 'test_*.py'
```

Results: fmt/check/clippy clean; 152 normal all-targets tests passed, plus doctests.
Four pre-existing manual tests remained opt-in; the 60-second restart soak was run
separately and passed in 62.51 seconds. The other three are EventBus queue-scan,
MQTT recovery-size and route-preflight performance probes, unrelated to the changed
connection setup. The all-targets command also executed the existing foundation
benchmark. No tests were removed, weakened or newly ignored.

The MQTT release gate passed 76/76, with evidence for 125/125 normative requirements,
including raw state machines, Mosquitto CLI, verified TLS, broker differential,
restart and fault tests. Classifier fuzz passed 506,597 executions in 31 seconds.
Tutorial smoke and five Python harness unit tests passed. Existing workspace tests
cover outages, required-sink rollback, slow sink isolation, auth-call counts,
recovery/corruption/spool failure and the documented SIGKILL loss window.

## Performance

Environment: macOS 26.6 / Darwin 25.6, aarch64, Rust 1.88.0, release optimized,
loopback, same host for server and client. Before binary was built from the clean
baseline and copied before edits; both binary SHA256 values are recorded in JSON.
After uses the production source in this commit. Other task workloads were stopped
before the completed measurement. This is a small sequential connection test,
not a production throughput ceiling or long-term soak certification.

```bash
cargo +1.88.0 build --release --locked -p netbaiot-server -p netbaiot-loadgen
python3 scripts/perf/ingress_compare.py --before target/single-ingress/baseline-server --after target/release/netbaiot-server --count 500 --repetitions 5 --output target/single-ingress/performance.json
```

Five alternating before/after repetitions, 50 warmup connections per protocol,
500 measured connections each. Every connection completes one accepted HTTP
request, MQTT CONNECT + QoS1 publish, or TCP auth + message. TLS verifies the test
CA and hostname; standard Mosquitto QoS0/1/2 is also checked. Values are median
completed connection/event cycles per second; larger is better.

| Path | Before | After | Delta |
|---|---:|---:|---:|
| http_plain | 15728.5 | 15713.2 | -0.10% |
| mqtt_plain | 9553.7 | 9493.3 | -0.63% |
| tcp_plain | 10503.4 | 10413.2 | -0.86% |
| http_tls | 1248.3 | 1243.6 | -0.38% |
| mqtt_tls | 1068.9 | 1051.6 | -1.61% |
| tcp_tls | 1143.5 | 1126.8 | -1.46% |

Observed changes range from -0.10% to -1.61%; no material regression in this probe.
Do not interpret noisy individual rounds as an improvement. Initial 1,000-cycle
campaign attempts exhausted macOS ephemeral ports and were discarded. The finished
harness waits for MQTT's server close and uses 500-cycle rounds to stay within host
port capacity; neither server limits nor timeouts were relaxed for the measurement.

Additional existing-harness measurements:

```bash
python3 scripts/perf/event_load.py --rate 1000 --duration 5 --warmup 1 --cooldown 1 --connections 32 --sink-mode webhook --sink-delay-ms 10
python3 scripts/perf/connection_memory.py --transport mqtt --connections 100 --hold-seconds 3
python3 scripts/perf/connection_memory.py --transport mqtt --connections 100 --tls --hold-seconds 3
python3 scripts/perf/connection_memory.py --transport tcp --connections 100 --hold-seconds 3
```

The slow-webhook run accepted 4,999 events with 4,999 PUBACKs, no load errors or
admission rejections, PUBACK p99 0.16 ms, and successful graceful completion. The
pre-shutdown metric sample had 4,152 sink ACKs: this intentionally overloaded the
slow sink, and is not evidence of a sustainable 1,000 events/s confirmed sink rate.

At 100 active connections, measured RSS deltas were 27.68 KiB/connection for MQTT,
39.20 KiB for TLS MQTT, and 21.12 KiB for TCP. Each run added 100 FDs and 100 runtime
tasks. These are process measurements at this sample size, distinct from the
524,288-byte logical reservation; no kernel-memory or production capacity claim.

## Remaining limits

The original global/IP connection pool is shared; downstream bounds isolate work,
but there is no strict per-protocol reserved connection capacity. Management still
shares existing process connection budgets while retaining independent listener
lifecycle. Configuration migration is deliberate and breaking for old deployment
JSON. HTTP/2/custom methods, DTLS and QUIC remain unsupported. MQTT 5 uses the
same ingress listener and is described in [the MQTT profile](mqtt.md). Abrupt failure
can still lose bounded memory traffic. No multi-host saturation campaign, 30-minute
mixed soak, new full parser-fuzz campaign or production TLS deployment was run.
