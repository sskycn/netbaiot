#!/usr/bin/env python3
"""Serial second-round experiments. Never builds during measurement."""
import argparse
import hashlib
import glob
import itertools
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
        binary = args.runtime if case in ["simultaneous_due", "due_workers"] else args.transports
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
        "plain": ["--connections", "256", "--qos", "1", "--payload-bytes", "1024"],
        "q1": ["--connections", "256", "--qos", "1", "--payload-bytes", "1024"],
        "large": ["--connections", "64", "--qos", "1", "--payload-bytes", "16384"],
        "q2": ["--connections", "64", "--qos", "2", "--payload-bytes", "1024"],
        "metadata": ["--connections", "100", "--qos", "1", "--payload-bytes", "1024", "--mqtt-v5", "--mqtt-metadata"],
        "outage": ["--connections", "64", "--qos", "1", "--payload-bytes", "1024", "--sink-mode", "webhook", "--sink-outage-seconds", "5"],
        "slow": ["--connections", "64", "--qos", "1", "--payload-bytes", "1024", "--sink-mode", "webhook", "--sink-delay-ms", "10"],
    }
    plan = {"before_sha256": digest(args.before), "after_sha256": digest(args.after), "loadgen_sha256": digest(args.loadgen), "duration": args.duration, "warmup": args.warmup, "rate": args.rate, "order": args.order, "cases": args.cases.split(",")}
    (output / "plan.json").write_text(json.dumps(plan, indent=2))
    for case in plan["cases"]:
        for index, label in enumerate(args.order):
            binary = args.before if label == "A" else args.after
            command = [sys.executable, str(ROOT / "scripts/perf/event_load.py"), "--server-bin", binary, "--loadgen-bin", args.loadgen, "--rate", str(args.rate), "--duration", str(args.duration), "--warmup", str(args.warmup), "--sink-mode", "none", "--subscribe-uplink", "--audit-open-loop"] + cases[case]
            if not args.lock_metrics:
                command.append("--no-lock-metrics")
            if case == "plain":
                command.remove("--subscribe-uplink")
            print("NETWORK", case, index, label, flush=True)
            with (output / f"{case}-{index}-{label}.json").open("w") as result:
                subprocess.run(command, cwd=ROOT, stdout=result, check=True, timeout=args.duration + args.warmup + 90)
            result = json.loads((output / f"{case}-{index}-{label}.json").read_text())
            if result["load_exit"] or not result["load"] or result["load"]["stats"]["error_samples"]:
                raise RuntimeError(f"failed workload retained in {case}-{index}-{label}.json")


def network_summary(paths):
    groups = {}
    for path in paths:
        path = Path(path)
        data = json.loads(path.read_text())
        stats = data["load"]["stats"]
        counters = stats["counters"]
        ack = "pubcomp" if data["qos"] == 2 else "puback"
        histogram = stats["latencies"][ack]
        metrics = {}
        for line in data["metrics"].splitlines():
            if line.startswith("#") or "{" in line:
                continue
            try:
                name, value = line.rsplit(" ", 1)
                metrics[name] = float(value)
            except ValueError:
                continue
        row = {"file": path.name, "cohort": path.parent.name, "samples": histogram["count"],
               "throughput_ops_s": counters.get("measurement_" + ack + "s", 0) / data["duration_seconds"],
               "rss_peak_kib": max(item["rss_kib"] for item in data["samples"]),
               "cpu_mean_percent": statistics.mean(item["cpu_percent"] for item in data["samples"]),
               "errors": len(stats["error_samples"]), "unexpected_disconnects": counters.get("client_errors", 0),
               "pending_at_disconnect": counters.get("pending_at_disconnect", 0),
               "queue_peak_count": max(item["event_count"] for item in data["samples"]),
               "queue_peak_bytes": max(item["event_bytes"] for item in data["samples"]),
               "overload": metrics.get("netbaiot_queue_rejects_total", 0) + metrics.get("netbaiot_ingress_rejected_total", 0)}
        row.update({key: histogram[key] for key in ["p50_ms", "p95_ms", "p99_ms", "p999_ms"]})
        for lock in ["broker_lock", "event_bus_state"]:
            for kind in ["wait", "hold"]:
                stem = f"netbaiot_{lock}_{kind}_us"
                count = metrics.get(stem + "_count", 0)
                row[lock + "_" + kind + "_mean_us"] = metrics.get(stem + "_sum", 0) / count if count else None
        case, _, label = path.stem.rsplit("-", 2)
        groups.setdefault(case, {}).setdefault(label, []).append(row)
    summaries = {}
    for case, variants in groups.items():
        output = {}
        for label, rows in variants.items():
            output[label] = {"runs": rows, "median": {key: statistics.median(row[key] for row in rows if row[key] is not None)
                           if any(row[key] is not None for row in rows) else None
                           for key in rows[0] if key not in ["file", "cohort"]}}
        if set(output) == {"A", "B"}:
            a, b = output["A"]["median"], output["B"]["median"]
            output["delta_percent"] = {key: 100 * (b[key] / a[key] - 1) if a[key] not in [None, 0] and b[key] is not None else None for key in a}
            x = [row["p99_ms"] for row in variants["A"]]
            y = [row["p99_ms"] for row in variants["B"]]
            if len(x) + len(y) <= 16:
                values = x + y
                difference = abs(statistics.median(x) - statistics.median(y))
                extreme = total = 0
                for split in itertools.combinations(range(len(values)), len(x)):
                    selected = set(split)
                    observed = abs(statistics.median(values[i] for i in selected) - statistics.median(values[i] for i in range(len(values)) if i not in selected))
                    extreme += observed >= difference - 1e-12
                    total += 1
                output["p99_two_sided_exact_permutation_p"] = extreme / total
        summaries[case] = output
    return summaries


def summary(args):
    data = {}
    if args.network:
        data["network"] = network_summary([path for pattern in args.network for path in glob.glob(pattern)])
    if args.micro_before and args.micro_after:
        before = summarize([path for pattern in args.micro_before for path in glob.glob(pattern)])
        after = summarize([path for pattern in args.micro_after for path in glob.glob(pattern)])
        if before.keys() != after.keys():
            raise ValueError("micro case sets differ")
        data["micro"] = {key: {"before": before[key], "after": after[key]} for key in before}
    Path(args.output).write_text(json.dumps(data, indent=2, allow_nan=False))


def auxiliary_records(paths):
 groups={};workers={};section=None;lines=[]
 for p in paths:
  for line in p.read_text().splitlines():
   if 'SECOND_LOCK,' in line:
    _,name,size,*v=line[line.index('SECOND_LOCK,'):].split(',');keys=['wait_mean_ns','wait_p50_ns','wait_p95_ns','wait_p99_ns','wait_max_ns','hold_mean_ns','hold_p50_ns','hold_p95_ns','hold_p99_ns','hold_max_ns'];groups.setdefault(name+'/'+size,[]).append(dict(zip(keys,map(float,v))))
   elif 'SECOND_WORKER,' in line:
    _,name,size,run,count,elapsed,rate=line[line.index('SECOND_WORKER,'):].split(',');workers.setdefault(name+'/'+size,[]).append({'elapsed_seconds':float(elapsed),'throughput_ops_s':float(rate)})
   elif 'WORKER_METRICS_BEGIN,' in line:
    _,name,size,run=line[line.index('WORKER_METRICS_BEGIN,'):].split(',');section=name+'/'+size;lines=[]
   elif line=='WORKER_METRICS_END':
    from eventbus_summary import histogram, metrics
    v=metrics('\n'.join(lines));workers[section][-1]['take_ready_hold']=histogram(v,'event_bus_site_take_ready_hold_ns');section=None
   elif section:lines.append(line)
 def median_tree(rows):
  if isinstance(rows[0],dict):return {k:median_tree([r[k] for r in rows]) for k in rows[0]}
  return statistics.median(rows)
 return {'locks':{k:{f:(max(r[f] for r in rows) if f.endswith('_max_ns') else statistics.median(r[f] for r in rows)) for f in rows[0]} for k,rows in groups.items()},'workers':{k:median_tree(rows) for k,rows in workers.items()}}

def auxiliary(args):
    paths = [Path(path) for pattern in args.logs for path in glob.glob(pattern)]
    Path(args.output).write_text(json.dumps(auxiliary_records(paths), indent=2))

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
    p.add_argument("--lock-metrics", action=argparse.BooleanOptionalAction, default=True)
    p.set_defaults(run=network)
    p = sub.add_parser("summary")
    p.add_argument("--network", nargs="+")
    p.add_argument("--micro-before", nargs="+")
    p.add_argument("--micro-after", nargs="+")
    p.add_argument("--output", required=True)
    p.set_defaults(run=summary)
    p = sub.add_parser("auxiliary")
    p.add_argument("--logs", nargs="+", required=True)
    p.add_argument("--output", required=True)
    p.set_defaults(run=auxiliary)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
