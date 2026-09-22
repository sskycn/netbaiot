#!/usr/bin/env python3
"""Reproduce EventId campaign summaries from raw local evidence."""
import json
import pathlib
import re
import statistics
import sys
from eventbus_summary import network


def median_tree(values):
    if isinstance(values[0], dict):
        return {k: median_tree([v[k] for v in values]) for k in values[0]}
    if values[0] is None:
        return None
    return statistics.median(values)


def profile(path, direct=False):
    source = path.read_text()
    total = sum(map(int, re.findall(r"^    (\d+) Thread_", source, re.M)))
    section = source.split("Sort by top of stack, same collapsed (when >= 5):")[1].split("Binary Images:")[0]
    rows = {m.group(1): int(m.group(2)) for m in
            re.finditer(r"^\s*(.*?)  \(in .*\)\s+(\d+)$", section, re.M)}
    parks = ("__ulock_wait",) if direct else ("__psynch_cvwait", "kevent")
    denominator = total - sum(rows.get(k, 0) for k in parks)
    categories = {
        "entropy": lambda s: "getentropy" in s,
        "mutex_wait": lambda s: "mutexwait" in s or "mutex_firstfit_lock_wait" in s,
        "send_recv": lambda s: "sendto" in s or "recvfrom" in s,
        "timekeeping": lambda s: "mach_absolute_time" in s or "clock_gettime" in s,
        "malloc_free_copy": lambda s: any(k in s for k in ("malloc", "free", "memcpy", "memmove", "memset")),
        "json_codec": lambda s: "serde_json" in s or "JsonV1" in s,
        "tokio": lambda s: "tokio" in s,
        "generator_rng": lambda s: "event_id" in s or "chacha" in s or "getpid" in s,
    }
    counts = {k: sum(n for s, n in rows.items() if predicate(s)) for k, predicate in categories.items()}
    return {"total": total, "nonpark": denominator, "counts": counts,
            "shares_percent": {k: 100 * n / denominator for k, n in counts.items()},
            "top": sorted(((s, n) for s, n in rows.items() if s not in parks), key=lambda row: -row[1])[:15]}


def main():
    root = pathlib.Path(sys.argv[1])
    result = {"network": {}}
    for path in sorted(root.glob("*.json")):
        if not path.stat().st_size or path.name in ("summary.json", "eventid-summary.json"):
            continue
        raw = json.loads(path.read_text())
        if isinstance(raw, dict) and "load" in raw and "rate_requested" in raw:
            row = network(path)
            row["cpu_seconds"] = raw["server_cpu_seconds"]
            result["network"][path.stem] = row
    primary = [result["network"][f"q1-20k-{i}"] for i in (1, 2, 3)]
    result["primary_median"] = median_tree(primary)
    raw_micro = json.loads((root / "micro.json").read_text())
    result["micro"] = {}
    for threads in (1, 2, 4, 8, 10):
        rows = [r for r in raw_micro if r["threads"] == threads]
        result["micro"][threads] = {
            key: statistics.median(r[key] for r in rows) for key in
            ("ids_per_second", "wall_ns_per_id", "cpu_seconds", "voluntary_context_switches", "involuntary_context_switches")}
        result["micro"][threads]["cold_first_id_ns"] = statistics.median(n for r in rows for n in r["cold_first_id_ns"])
    for key, name, direct in (("network_profile", "profile.sample.txt", False), ("direct_profile", "direct.sample.txt", True)):
        if (root / name).exists():
            result[key] = profile(root / name, direct)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
