#!/usr/bin/env python3
"""Run reproducible local V3 send-ahead discovery cases with fresh gateways."""
import argparse
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "tools/netbaiot-loadgen/run_business_rpc_v3_hol.py"
ANALYZER = ROOT / "tools/netbaiot-loadgen/analyze_business_rpc_v3_hol.py"
CASES = [(4096, 8192), (4096, 16384), (8192, 8192), (8192, 16384),
         (8192, 32768), (8192, 65536), (8192, 131072), (8192, 262144),
         (16384, 16384), (16384, 32768), (16384, 65536)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-secs", type=int, default=15)
    parser.add_argument("--bytes-per-second", type=int, default=32768)
    parser.add_argument("--delay-ms", type=float, default=25)
    parser.add_argument("--event-rate", type=int, default=1)
    parser.add_argument("--auth-concurrency", type=int, default=4)
    parser.add_argument("--auth-unique-devices", action="store_true")
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--no-proxy", action="store_true")
    parser.add_argument("--trace-gateway", action="store_true")
    parser.add_argument("--cases", nargs="+", help="frame:stream pairs, e.g. 8192:65536")
    args = parser.parse_args()
    if not 1 <= args.repeat <= 5:
        parser.error("repeat must be 1–5")
    failed = []
    selected = CASES
    if args.cases:
        try:
            selected = [tuple(map(int, case.split(":"))) for case in args.cases]
        except ValueError:
            parser.error("cases must be frame:stream pairs")
        if any(case not in CASES for case in selected):
            parser.error("unknown matrix case")
    for frame, stream in selected:
        for run in range(1, args.repeat + 1):
            target = args.output / f"frame-{frame}-ahead-{stream}-run-{run}"
            mode = f"v3-{frame}"
            command = [sys.executable, str(HARNESS), "--duration-secs", str(args.duration_secs),
                       "--modes", mode, "--send-ahead-stream-bytes", str(stream),
                       "--send-ahead-connection-bytes", str(max(131072, stream)), "--bytes-per-second",
                       str(args.bytes_per_second), "--delay-ms", str(args.delay_ms),
                       "--event-rate", str(args.event_rate), "--auth-concurrency",
                       str(args.auth_concurrency), "--output", str(target)]
            if args.no_proxy:
                command.append("--no-proxy")
            if args.trace_gateway:
                command.append("--trace-gateway")
            if args.auth_unique_devices:
                command.append("--auth-unique-devices")
            environment = os.environ.copy()
            if args.trace_gateway:
                environment["NETBAIOT_HOL_CAPTURE"] = "1"
            print("running", target, flush=True)
            completed = subprocess.run(command, cwd=ROOT, env=environment, check=False)
            if completed.returncode:
                failed.append(str(target))
                continue
            summary = target / "summary.json"
            subprocess.run([sys.executable, str(ANALYZER), str(target), mode,
                            "--output", str(summary)], cwd=ROOT, check=True)
    if failed:
        print("failed cases with preserved raw output:", *failed, sep="\n", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
