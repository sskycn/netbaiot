#!/bin/sh
set -eu
export CARGO_NET_OFFLINE=true
export CARGO_PROFILE_RELEASE_DEBUG=1
export PYTHONPYCACHEPREFIX=/tmp/netbaiot-pycache
# Wait for the last serial capacity/dataset experiment, never overlap workloads.
python3 - <<'PY'
import json,pathlib,time
p=pathlib.Path('docs/performance/dataset_1000000.json')
for _ in range(1440):
    try:
        if len(json.loads(p.read_text()).get('queries',{}))>=10:break
    except (FileNotFoundError,json.JSONDecodeError):pass
    time.sleep(5)
else:raise SystemExit('capacity matrix did not complete within two hours')
PY
cargo fmt --all -- --check > docs/performance/validation/retained-fmt.log 2>&1
cargo clippy --workspace --all-targets --all-features -- -D warnings > docs/performance/validation/retained-clippy.log 2>&1
cargo test --workspace --all-features > docs/performance/validation/retained-tests.log 2>&1
python3 scripts/perf/validate.py
python3 scripts/perf/run_case.py scripts/perf/cases/soak.json docs/performance/soak_tls_2h.json
