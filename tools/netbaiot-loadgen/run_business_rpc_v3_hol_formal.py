#!/usr/bin/env python3
"""Repeat V2 and V3 HOL candidates with fresh gateways and preserve every raw result."""
import argparse
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
HARNESS = ROOT / "tools/netbaiot-loadgen/run_business_rpc_v3_hol.py"
ANALYZER = ROOT / "tools/netbaiot-loadgen/analyze_business_rpc_v3_hol.py"
VARIANTS = ("multiplexed", "dual", "v3-baseline", "v3-ahead-8192", "v3-ahead-16384")


def stats(values):
    values = [value for value in values if value is not None]
    return {"count": len(values), "median": statistics.median(values) if values else None,
            "min": min(values) if values else None, "max": max(values) if values else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration-secs", type=int, default=60)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--bytes-per-second", type=int, default=32768)
    parser.add_argument("--delay-ms", type=float, default=25)
    parser.add_argument("--event-rate", type=int, default=1)
    parser.add_argument("--auth-concurrency", type=int, default=4)
    parser.add_argument("--no-proxy", action="store_true")
    parser.add_argument("--no-trace-v3", action="store_true")
    parser.add_argument("--variants", nargs="+", choices=VARIANTS, default=VARIANTS)
    args = parser.parse_args()
    if not 1 <= args.repeat <= 5 or not 10 <= args.duration_secs <= 120:
        parser.error("invalid repeat or duration")
    args.output.mkdir(parents=True, exist_ok=True)
    results = {}
    failures = []
    for run in range(1, args.repeat + 1):
        for variant in args.variants:
            target = args.output / f"run-{run}-{variant}"
            mode = "v3-8192" if variant.startswith("v3-") else variant
            command = [sys.executable, str(HARNESS), "--duration-secs", str(args.duration_secs),
                       "--auth-unique-devices", "--auth-concurrency", str(args.auth_concurrency),
                       "--event-rate", str(args.event_rate), "--bytes-per-second",
                       str(args.bytes_per_second), "--delay-ms", str(args.delay_ms),
                       "--modes", mode, "--output", str(target)]
            environment = os.environ.copy()
            if variant.startswith("v3-"):
                if not args.no_trace_v3:
                    command.append("--trace-gateway")
                    environment["NETBAIOT_HOL_CAPTURE"] = "1"
            if args.no_proxy:
                command.append("--no-proxy")
            if variant.startswith("v3-ahead-"):
                stream = int(variant.rsplit("-", 1)[1])
                command.extend(["--send-ahead-stream-bytes", str(stream),
                                "--send-ahead-connection-bytes", "131072"])
            print("running", target, flush=True)
            completed = subprocess.run(command, cwd=ROOT, env=environment, check=False)
            if completed.returncode:
                failures.append(str(target))
                continue
            raw = json.loads((target / f"{mode}.json").read_text())
            if variant.startswith("v3-"):
                summary = target / "summary.json"
                subprocess.run([sys.executable, str(ANALYZER), str(target), mode,
                                "--output", str(summary)], cwd=ROOT, check=True)
                data = json.loads(summary.read_text())
            else:
                counts = raw["counts"]
                data = {
                    "sample_count": counts["requests"],
                    "auth_latency_ms": counts["latency_ms"],
                    "auth_per_second": raw["requests_per_second"],
                    "event_acks": counts["event_acks"],
                    "event_throughput_payload_bytes_per_second": counts["event_acks"] * 16384 / args.duration_secs,
                    "cpu_percent": raw["peaks"]["gateway_cpu"]["average_workload_percent"],
                    "rss_peak_kb": raw["peaks"]["rss_peak_kb"],
                }
            data["auth_success"] = raw["counts"]["success"]
            data["auth_rejected"] = raw["counts"]["rejected"]
            data["auth_timeout"] = raw["counts"]["timeout"]
            data["provider_calls"] = raw["auth_provider_calls"]
            results.setdefault(variant, []).append(data)
    summary = {"duration_secs": args.duration_secs, "repeat": args.repeat,
               "bytes_per_second": args.bytes_per_second,
               "delay_ms_per_read_per_direction": args.delay_ms,
               "event_rate": args.event_rate, "auth_concurrency": args.auth_concurrency,
               "fresh_identities": True, "no_proxy": args.no_proxy,
               "trace_v3": not args.no_trace_v3,
               "failures": failures, "variants": {}}
    for variant, runs in results.items():
        summary["variants"][variant] = {
            "runs": len(runs),
            "sample_counts": [run["sample_count"] for run in runs],
            "auth_success": [run["auth_success"] for run in runs],
            "auth_rejected": [run["auth_rejected"] for run in runs],
            "auth_timeout": [run["auth_timeout"] for run in runs],
            "provider_calls": [run["provider_calls"] for run in runs],
            "p50_ms": stats([run["auth_latency_ms"]["p50"] for run in runs]),
            "p95_ms": stats([run["auth_latency_ms"]["p95"] for run in runs]),
            "p99_ms": stats([run["auth_latency_ms"]["p99"] for run in runs]),
            "auth_per_second": stats([run["auth_per_second"] for run in runs]),
            "event_payload_bytes_per_second": stats([run["event_throughput_payload_bytes_per_second"] for run in runs]),
            "cpu_percent": stats([run["cpu_percent"] for run in runs]),
            "rss_peak_kb": stats([run["rss_peak_kb"] for run in runs]),
        }
        if variant.startswith("v3-"):
            summary["variants"][variant].update({
                "event_completion_p50_ms": stats([run["event_completion_ms"]["p50"] for run in runs]),
                "event_completion_p95_ms": stats([run["event_completion_ms"]["p95"] for run in runs]),
                "competing_event_streams": [run["competing_event_streams"] for run in runs],
                "interleaved_event_streams": [run["interleaved_event_streams"] for run in runs],
                "preemption_p95_ms": stats([run["preemption_ms"]["p95"] for run in runs]),
            })
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if failures:
        print("failed cases with preserved raw output:", *failures, sep="\n", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
