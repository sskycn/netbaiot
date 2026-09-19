#!/bin/sh
# Explicit serial stages; use after building both release binaries.
set -eu
export PYTHONPYCACHEPREFIX=/tmp/netbaiot-capacity-pycache
python3 scripts/perf/run_case.py scripts/perf/cases/calibration.json docs/performance/generator_calibration.json
python3 scripts/perf/matrix.py uplink
python3 scripts/perf/matrix.py tls_cold
