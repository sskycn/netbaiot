#!/bin/sh
set -eu
export CARGO_NET_OFFLINE=true
export CARGO_PROFILE_RELEASE_DEBUG=1
export PYTHONPYCACHEPREFIX=/tmp/netbaiot-pycache
# Used only when continuing the current serial audit; bounded wait for the last TLS result.
python3 - <<'PY'
import json,pathlib,time
p=pathlib.Path('docs/performance/ramp_tls_cold_1000.json')
for _ in range(240):
    try:
        if json.loads(p.read_text()).get('ended_epoch'):break
    except (FileNotFoundError,json.JSONDecodeError):pass
    time.sleep(5)
else:raise SystemExit('preceding TLS matrix did not finish within 20 minutes')
PY
cargo fmt --all -- --check > /tmp/netbaiot-capacity-final-fmt.log 2>&1
cargo clippy --workspace --all-targets --all-features -- -D warnings > /tmp/netbaiot-capacity-final-clippy.log 2>&1
cargo test --workspace --all-features > /tmp/netbaiot-capacity-final-tests.log 2>&1
cargo build --release -p netbaiot-server -p netbaiot-loadgen --bins > /tmp/netbaiot-capacity-final-release.log 2>&1
for rep in 1 2 3; do
    target/release/admission_auth > "docs/performance/admission_recent_r${rep}.jsonl"
    cargo bench -p netbaiot-transports --bench foundation > "docs/performance/foundation_final_r${rep}.log" 2>&1
done
python3 scripts/perf/sustained.py
for group in commands protocols recovery churn fairness profiles database_growth; do
    python3 scripts/perf/extended.py "$group"
done
python3 scripts/perf/dataset.py
