# Contributing to NetbaIoT

## Setup

Use Rust 1.88 or newer, Cargo, and Python 3.9 or newer. The release checks run on
Rust 1.88.0 and stable. MQTT interoperability checks also need `mosquitto`,
`mosquitto_pub`, `mosquitto_sub`, and OpenSSL. Mosquitto is a test/reference broker,
not a NetbaIoT runtime dependency.

```bash
git clone https://github.com/sskycn/netbaiot.git
cd netbaiot
cargo build --locked
cargo install --locked cargo-audit
```

The [Quick Start](docs/quick-start.md) runs a local gateway and webhook.

## Before opening a PR

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
cargo audit
```

Keep the lockfile and all workspace package versions consistent. Describe the
problem, resulting behavior, compatibility impact, and exactly which checks you
ran. Mark checks you could not run explicitly; do not present configured resource
limits or old benchmark results as measured current capacity.

## MQTT changes

Changes to parsers, the broker, sessions, QoS, recovery, or MQTT 5 require focused
raw state-machine tests and the protocol/interoperability gates:

```bash
cargo build --locked -p netbaiot-server
python3 tests/mqtt_protocol_regressions.py --repo . --output target/mqtt-audit/pr.json
python3 -m unittest discover -s tests/mqtt_conformance -p 'test_*.py' -v
python3 tests/mqtt_conformance/run.py --release-gate
python3 tests/mqtt_conformance/v5_smoke.py
python3 tests/mqtt_conformance/v5_mosquitto.py
cargo build --locked -p netbaiot-device-sdk --example device_mqtt
python3 tests/run_device_profile_mosquitto.py
python3 tests/measure_device_profile.py
```

Run relevant fuzz targets for decoder changes. The **MQTT Device Profile decoder
fuzz smoke** Actions workflow supports manual dispatch; local runs use nightly and
`cargo-fuzz`. A NetbaIoT normative failure must fail the release gate, even when a
reference broker observation is timing-sensitive.

## Security and reliability changes

Add targeted tests for the affected boundary: malformed inputs, count/byte
rollback, cancellation, lifecycle races, invalidation, required sink ACKs, recovery,
or live-session command ownership. Run relevant subprocess restart, outage,
slow-sink, and fuzz checks. Report measurements and untested failure modes honestly.
Follow [SECURITY.md](SECURITY.md) for undisclosed vulnerabilities.

## Scope and architecture

Prefer focused PRs with one logical change. Do not silently change public protocol
semantics; include compatibility review and serialization tests when they change.
The runtime remains database-free, memory-first, and bounded. Business systems own
durable business data and offline commands.

Read [AGENTS.md](AGENTS.md) for the full architecture and correctness constraints.
Use [docs/release-template.md](docs/release-template.md) when preparing a release.
