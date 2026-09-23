#!/usr/bin/env python3
"""Summarize retained campaign JSON, preserving unavailable values as null."""
import json
import math
import pathlib
import statistics
import sys


def metrics(text):
    return {k: float(v) for k, v in (line.split() for line in text.splitlines())}


def histogram(values, name):
    prefix = "netbaiot_" + name
    count = values.get(prefix + "_count", 0)
    if not count:
        return None
    result = {"count": count, "mean": values[prefix + "_sum"] / count,
              "sum": values[prefix + "_sum"]}
    buckets = sorted((float(k.split('le="')[1].split('"')[0]), v)
                     for k, v in values.items() if k.startswith(prefix + "_bucket{"))
    for percentile in (50, 95, 99):
        bound = next(b for b, v in buckets if v >= math.ceil(count * percentile / 100))
        result[f"p{percentile}_upper"] = bound if math.isfinite(bound) else None
    return result


def probes(values, events):
    raw = {k.split("probe_")[1].removesuffix("_total"): v / events
           for k, v in values.items() if "event_bus_probe_" in k}
    raw["locks"] = sum(raw.get(k, 0) for k in ("publish", "take_ready", "complete", "next_delay"))
    raw["worker_wakes"] = sum(raw.get(k, 0) for k in ("wake_notify", "wake_timer", "wake_join"))
    return raw


def network(path):
    d = json.loads(path.read_text())
    m = metrics(d["metrics"])
    n = m["netbaiot_events_accepted_total"]
    latency = "puback" if d["qos"] == 1 else "pubcomp"
    return {"accepted_s": n / d["duration_seconds"], "sink_acks_s": m["netbaiot_sink_acks_total"] / d["duration_seconds"],
            "pubacks_s": d["load"]["stats"]["counters"].get("pubacks", 0) / d["duration_seconds"] if d["qos"] == 1 else None,
            "cpu_seconds": d.get("server_cpu_seconds"),
            "cpu_us_per_event": d["server_cpu_seconds"] * 1_000_000 / n if d.get("server_cpu_seconds") is not None else None,
            "sink_failures": m["netbaiot_sink_failures_total"],
            "latency": d["load"]["stats"]["latencies"].get(latency),
            "cpu_peak": max(s["cpu_percent"] for s in d["samples"]),
            "rss_peak_kib": max(s["rss_kib"] for s in d["samples"]),
            "pending_max": max(s["pending_required"] for s in d["samples"]),
            "bytes_max": max(s["event_bytes"] for s in d["samples"]),
            "retries": m["netbaiot_sink_retries_total"],
            "rejects": {k:v for k,v in m.items() if "reject" in k},
            "probes": probes(m, n),
            "locks": {name: histogram(m, name) for name in (
                "event_bus_lock_wait_us", "event_bus_lock_hold_us", "event_bus_state_wait_us", "event_bus_state_hold_us", "event_bus_route_wait_us")}}


def main():
    root = pathlib.Path(sys.argv[1])
    result = {}
    for path in sorted(root.glob("*.json")):
        if not path.stat().st_size or path.name == "summary.json":
            continue
        value = json.loads(path.read_text())
        if isinstance(value, dict) and "load" in value and "rate_requested" in value:
            result[path.stem] = network(path)
    if (root/"micro.jsonl").exists():
        result["micro"] = []
        for line in (root/"micro.jsonl").read_text().splitlines():
            d = json.loads(line)
            before = metrics(d.pop("before_metrics"))
            after = metrics(d.pop("metrics"))
            delta = {k:v-before.get(k,0) for k,v in after.items()}
            d["probes"] = probes(delta,d["events"])
            d["locks"] = {name:histogram(delta,name) for name in ("event_bus_state_wait_us","event_bus_state_hold_us")}
            result["micro"].append(d)
    primaries = [result[f"q1-20k-{i}"] for i in (1,2,3) if f"q1-20k-{i}" in result]
    def median_tree(values):
        if isinstance(values[0],dict):
            return {k:median_tree([v[k] for v in values]) for k in values[0]}
        if values[0] is None: return None
        return statistics.median(values)
    if primaries: result["primary_median"] = median_tree(primaries)
    for side in ("before", "after"):
        paired = [result[f"{side}-q1-20k-{i}"] for i in (1, 2, 3)
                  if f"{side}-q1-20k-{i}" in result]
        if len(paired) == 3:
            result[f"{side}_primary_median"] = median_tree(paired)
    print(json.dumps(result,indent=2,sort_keys=True))

if __name__ == "__main__": main()
