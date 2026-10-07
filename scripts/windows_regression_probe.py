#!/usr/bin/env python3
"""Measure the two recovery regressions without changing their deadlines."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import time


TESTS = (
    "one_socket_authentication_progresses_while_event_ack_waits",
    "v1_spooled_required_event_replays_to_v2_with_stable_event_id",
)


def annotation(title, value):
    data = json.dumps(value, separators=(",", ":"))
    data = data.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    print(f"::notice title={title}::{data}", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--commit", required=True)
    parser.add_argument("--repetitions", type=int, default=10)
    parser.add_argument("--failure-repetitions", type=int, default=40)
    parser.add_argument("--skip-suites", action="store_true")
    args = parser.parse_args()
    root = Path("target/evidence/windows-regression")
    root.mkdir(parents=True, exist_ok=True)
    results = []

    def run(name, command, iteration):
        started = time.monotonic()
        with (root / f"{name}-{iteration}.log").open("w", encoding="utf-8") as output:
            result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT)
        duration = time.monotonic() - started
        print(f"{args.commit} {name} {iteration}: exit={result.returncode} {duration:.3f}s", flush=True)
        if result.returncode:
            tail = (root / f"{name}-{iteration}.log").read_text(encoding="utf-8", errors="replace")[-2800:]
            print(tail, flush=True)
        return {"pass": result.returncode == 0, "duration_s": round(duration, 3)}

    base = ["cargo", "test", "--locked", "-p", "netbaiot-server", "--test", "business_rpc_v2"]
    build = run("build", base + ["--no-run"], 1)
    if not build["pass"]:
        annotation("regression-build", {"commit": args.commit, **build})
        return 1
    for test in TESTS:
        samples = []
        target = args.repetitions
        while len(samples) < target:
            sample = run(test, base + [test, "--", "--exact", "--nocapture"], len(samples) + 1)
            samples.append(sample)
            if not sample["pass"]:
                target = max(target, args.failure_repetitions)
        passed = sum(sample["pass"] for sample in samples)
        summary = {
            "commit": args.commit,
            "test": test,
            "pass": passed,
            "fail": len(samples) - passed,
            "failure_rate": (len(samples) - passed) / len(samples),
            "duration_s": round(sum(sample["duration_s"] for sample in samples), 3),
            "samples": samples,
        }
        results.append(summary)
        annotation("regression-repetitions", {key: value for key, value in summary.items() if key != "samples"})
    if not args.skip_suites:
        for name, command in (
            ("suite-serial", base + ["--", "--test-threads=1"]),
            ("suite-parallel", base),
            ("workspace-parallel", ["cargo", "test", "--locked", "--workspace", "--all-features"]),
        ):
            sample = run(name, command, 1)
            summary = {"commit": args.commit, "test": name, **sample}
            results.append(summary)
            annotation("regression-suite", summary)
    (root / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write("```json\n" + json.dumps(results, indent=2) + "\n```\n")
    return int(any(result.get("fail", 0) or result.get("pass") is False for result in results))


if __name__ == "__main__":
    raise SystemExit(main())
