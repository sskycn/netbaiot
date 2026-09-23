#!/usr/bin/env python3
"""Summarize opt-in EventBus sites and dequeue distributions, retaining units."""
import json
import math
import pathlib
import statistics
import sys

from eventbus_summary import histogram, metrics, network

SITES = ("publish", "take_ready", "next_ready_delay", "complete", "control_restore_spool")


def accounting(values, events):
    sites = {}
    for site in SITES:
        wait = histogram(values, f"event_bus_site_{site}_wait_ns")
        hold = histogram(values, f"event_bus_site_{site}_hold_ns")
        count = wait["count"] if wait else 0
        sites[site] = {"acquisitions_per_event": count / events, "count": count,
                       "wait_ns": wait, "hold_ns": hold}
    dequeue = histogram(values, "event_bus_dequeue_records")
    prefix = "netbaiot_event_bus_dequeue_records"
    empty = values.get(prefix + '_bucket{le="0"}', 0)
    nonempty = dequeue["count"] - empty if dequeue else 0
    buckets = sorted((float(k.split('le="')[1].split('"')[0]), v)
                     for k, v in values.items() if k.startswith(prefix + "_bucket{"))
    batch = {"calls": dequeue["count"] if dequeue else 0, "empty_calls": empty,
             "batches": nonempty, "records": dequeue["sum"] if dequeue else 0,
             "mean_records_per_call": dequeue["mean"] if dequeue else 0,
             "mean_records_per_batch": dequeue["sum"] / nonempty if nonempty else None,
             "histogram_including_empty": dict(buckets)}
    for p in (50, 95, 99):
        bound = next((b for b, v in buckets if nonempty and v - empty >= math.ceil(nonempty * p / 100)), None)
        batch[f"nonempty_p{p}_upper"] = bound if bound is not None and math.isfinite(bound) else None
    return {"sites": sites, "state_acquisitions_per_event": sum(s["count"] for s in sites.values()) / events,
            "state_wait_sum_ns": sum(s["wait_ns"]["sum"] for s in sites.values() if s["wait_ns"]),
            "state_hold_sum_ns": sum(s["hold_ns"]["sum"] for s in sites.values() if s["hold_ns"]),
            "batch": batch, "queue_len": histogram(values, "event_bus_dequeue_queue_len"),
            "selection_ns": histogram(values, "event_bus_dequeue_selection_ns")}


def median_tree(values):
    if isinstance(values[0], dict):
        return {k: median_tree([v[k] for v in values]) for k in values[0]}
    if values[0] is None:
        return None
    return statistics.median(values)


def main():
    root = pathlib.Path(sys.argv[1])
    result = {}
    for path in sorted(root.glob("*.json")):
        if not path.stat().st_size or path.name.endswith("summary.json"):
            continue
        d = json.loads(path.read_text())
        if isinstance(d, dict) and "rate_requested" in d:
            values = metrics(d["metrics"])
            events = values["netbaiot_events_accepted_total"]
            result[path.stem] = {"network": network(path), **accounting(values, events)}
    for path in sorted(root.glob("*-micro*.jsonl")):
        rows = []
        for line in path.read_text().splitlines():
            d = json.loads(line)
            before = metrics(d.pop("before_metrics"))
            after = metrics(d.pop("metrics"))
            delta = {k: v - before.get(k, 0) for k, v in after.items()}
            rows.append({**d, **accounting(delta, d["events"])})
        result[path.stem] = rows
    for side in ("before", "after"):
        runs = [result[f"{side}-q1-20k-{i}"] for i in range(1, 4) if f"{side}-q1-20k-{i}" in result]
        if len(runs) == 3:
            result[f"{side}_median"] = median_tree(runs)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
