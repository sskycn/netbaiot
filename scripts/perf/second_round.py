#!/usr/bin/env python3
"""Serial second-round experiments. Never builds during measurement."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
FIELDS = ["mean_ns", "p50_ns", "p95_ns", "p99_ns", "max_ns", "allocations_per_op", "allocated_bytes_per_op"]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def summarize(paths):
    groups = {}
    for path in paths:
        for line in Path(path).read_text().splitlines():
            # libtest may prepend the test name to the first emitted record.
            marker = line.find("SECOND_ROUND,")
            if marker < 0:
                continue
            _, name, size, run, *values = line[marker:].split(",")
            groups.setdefault(f"{name}/{size}", []).append(dict(zip(FIELDS, map(float, values))))
    return {key: {field: (max(row[field] for row in rows) if field == "max_ns" else statistics.median(row[field] for row in rows)) for field in FIELDS}
            for key, rows in sorted(groups.items())}


def micro(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["NETBAIOT_SECOND_ROUND"] = "1"
    manifest = {"transports_sha256": digest(args.transports), "runtime_sha256": digest(args.runtime), "commands": []}
    cases = args.cases.split(",") if args.cases else ["topic", "preflight", "session", "route_ack", "expiry", "simultaneous_due", "metadata", "retained", "order"]
    paths = []
    for case in cases:
        binary = args.runtime if case == "simultaneous_due" else args.transports
        command = [str(Path(binary).resolve()), "second_round_" + case, "--ignored", "--nocapture", "--test-threads=1"]
        manifest["commands"].append(command)
        print("MEASURE", case, flush=True)
        path = output / f"{case}.log"
        with path.open("w") as log:
            subprocess.run(command, env=env, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, check=True)
        paths.append(path)
    (output / "summary.json").write_text(json.dumps(summarize(paths), indent=2))
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2))


def network(args):
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    cases = {
        "q1": ["--connections", "256", "--qos", "1", "--payload-bytes", "1024"],
        "large": ["--connections", "64", "--qos", "1", "--payload-bytes", "16384"],
        "q2": ["--connections", "64", "--qos", "2", "--payload-bytes", "1024"],
        "metadata": ["--connections", "100", "--qos", "1", "--payload-bytes", "1024", "--mqtt-v5", "--mqtt-metadata"],
        "slow": ["--connections", "64", "--qos", "1", "--payload-bytes", "1024", "--sink-mode", "webhook", "--sink-delay-ms", "10"],
    }
    plan = {"before_sha256": digest(args.before), "after_sha256": digest(args.after), "loadgen_sha256": digest(args.loadgen), "duration": args.duration, "warmup": args.warmup, "rate": args.rate, "order": args.order, "cases": args.cases.split(",")}
    (output / "plan.json").write_text(json.dumps(plan, indent=2))
    for case in plan["cases"]:
        for index, label in enumerate(args.order):
            binary = args.before if label == "A" else args.after
            command = [sys.executable, str(ROOT / "scripts/perf/event_load.py"), "--server-bin", binary, "--loadgen-bin", args.loadgen, "--rate", str(args.rate), "--duration", str(args.duration), "--warmup", str(args.warmup), "--sink-mode", "none", "--subscribe-uplink", "--audit-open-loop"] + cases[case]
            print("NETWORK", case, index, label, flush=True)
            with (output / f"{case}-{index}-{label}.json").open("w") as result:
                subprocess.run(command, cwd=ROOT, stdout=result, check=True, timeout=args.duration + args.warmup + 90)
            result = json.loads((output / f"{case}-{index}-{label}.json").read_text())
            if result["load_exit"] or not result["load"] or result["load"]["stats"]["error_samples"]:
                raise RuntimeError(f"failed workload retained in {case}-{index}-{label}.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="mode", required=True)
    p = sub.add_parser("micro")
    p.add_argument("--transports", required=True)
    p.add_argument("--runtime", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--cases")
    p.set_defaults(run=micro)
    p = sub.add_parser("network")
    p.add_argument("--before", required=True)
    p.add_argument("--after", required=True)
    p.add_argument("--loadgen", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--cases", default="q1")
    p.add_argument("--order", default="ABBA")
    p.add_argument("--duration", type=float, default=10)
    p.add_argument("--warmup", type=float, default=3)
    p.add_argument("--rate", type=float, default=20000)
    p.set_defaults(run=network)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
