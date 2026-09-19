#!/usr/bin/env python3
"""Derive bounded resource/phase/trend evidence from completed raw experiments.

Sampled maxima are lower bounds on instantaneous peaks. Interval latency means
come from cumulative sums/counts; cumulative percentiles cannot be subtracted.
This script never contacts the database or a running service.
"""
import json
import math
import pathlib
import re
import statistics

from summarize import final, summary

OUT = pathlib.Path(__file__).resolve().parents[2] / 'docs/performance'


def describe(values):
    values = sorted(v for v in values if isinstance(v, (int, float)))
    if not values:
        return None
    return dict(count=len(values), min=values[0], median=statistics.median(values),
                p95=values[math.ceil(len(values) * .95) - 1],
                p99=values[math.ceil(len(values) * .99) - 1], max=values[-1])


def vmmap(path):
    if not path.exists():
        return None
    raw = path.read_text(); zones = []

    def size(value):
        match = re.fullmatch(r'([0-9.]+)([KMGT]?)', value)
        if not match:
            raise ValueError(value)
        return float(match[1]) * 1024 ** (' KMGT'.index(match[2]) if match[2] else 0)

    for line in raw.splitlines():
        fields = line.split()
        if len(fields) == 10 and 'MallocZone' in fields[0] and fields[8].endswith('%'):
            zones.append(dict(name=fields[0], resident_bytes=size(fields[2]), dirty_bytes=size(fields[3]),
                              allocations=int(fields[5]), allocated_bytes=size(fields[6]),
                              fragmentation_bytes=size(fields[7]), fragmentation_percent=float(fields[8][:-1])))
    footprint = re.search(r'^Physical footprint:\s+(\S+)', raw, re.MULTILINE)
    return dict(source=path.name, zones=zones,
                physical_footprint_bytes=size(footprint[1]) if footprint else None,
                limitation='Rounded native allocator/footprint snapshot, not ps RSS, allocation stacks or per-request allocation counts')


def window(rows):
    if not rows:
        return {}
    result = dict(start_s=rows[0]['elapsed_s'], end_s=rows[-1]['elapsed_s'], samples=len(rows))
    for label, keys in {
        'server': ['rss_kib', 'fds', 'network_sockets', 'threads'],
        'postgres': ['rows', 'commands', 'command_attempts', 'outbox_pending',
                     'oldest_outbox_ms', 'connections', 'active', 'lock_waiters',
                     'dead_rows', 'db_bytes', 'ingress_bytes', 'outbox_bytes'],
        'metrics': ['netbaiot_queue_bytes', 'netbaiot_queue_depth',
                    'netbaiot_ingress_inflight', 'netbaiot_ingress_inflight_bytes',
                    'netbaiot_protocol_inflight', 'netbaiot_protocol_inflight_bytes',
                    'netbaiot_runtime_alive_tasks', 'netbaiot_registered_sessions',
                    'netbaiot_session_tenant_entries', 'netbaiot_presence_entries',
                    'netbaiot_subscription_entries'],
    }.items():
        result[label] = {k: describe(r.get(label, {}).get(k) for r in rows) for k in keys}
    elapsed = rows[-1]['elapsed_s'] - rows[0]['elapsed_s']
    if elapsed > 0:
        store_ms = 0
        accepts = 0
        for a, b in zip(rows, rows[1:]):
            before, after = a.get('metrics', {}), b.get('metrics', {})
            latency = 'netbaiot_database_latency_ms_total'
            count = 'netbaiot_ingress_accepted_total'
            if (all(k in before and k in after for k in (latency, count))
                    and a.get('server', {}).get('pid') == b.get('server', {}).get('pid')
                    and after[latency] >= before[latency] and after[count] >= before[count]):
                store_ms += after[latency] - before[latency]
                accepts += after[count] - before[count]
        if accepts:
            # Store timing includes acquisition/transaction work and is floored
            # per successful invocation; denominator includes command ACK ingress.
            result['successful_store_ms_floor_per_ingress_accept'] = store_ms / accepts
        for label in ['server', 'sink'] + ['generator' if i == 0 else f'generator_{i}' for i in range(8)]:
            good = [r for r in rows if 'cpu_seconds' in r.get(label, {})]
            if len(good) > 1:
                seconds = sum(max(0, b[label]['cpu_seconds'] - a[label]['cpu_seconds'])
                              for a, b in zip(good, good[1:]) if a[label].get('pid') == b[label].get('pid'))
                result[label + '_cpu_cores'] = seconds / (good[-1]['elapsed_s'] - good[0]['elapsed_s'])
        backend_seconds = 0
        for a, b in zip(rows, rows[1:]):
            before = a.get('postgres', {}).get('backend_processes', {})
            after = b.get('postgres', {}).get('backend_processes', {})
            for pid in before.keys() & after.keys():
                if 'cpu_seconds' in before[pid] and 'cpu_seconds' in after[pid]:
                    backend_seconds += max(0, after[pid]['cpu_seconds'] - before[pid]['cpu_seconds'])
        # Excludes checkpointer/autovacuum and processes that disappear between samples.
        result['postgres_observed_backend_cpu_cores'] = backend_seconds / elapsed
    return result


def analyze(d):
    result = summary(d)
    rows = d.get('samples', [])
    result['all_samples'] = window(rows)
    result['connection_close_reasons'] = d.get('connection_close_reasons')
    result['trace_summary'] = {k: describe(v) for k, v in d.get('trace_measurements', {}).items()}
    result['storage_state'] = d.get('final_storage_state')
    result['shutdowns'] = d.get('shutdowns')
    result['vmmap'] = {label: vmmap(OUT / (d['name'] + suffix)) for label, suffix in
                       [('during_profile', '.peak-vmmap.txt'), ('after_disconnect', '.vmmap.txt')]}
    result['events'] = d.get('events')
    result['server_died_at'] = d.get('server_died_at')
    result['snapshots'] = {k: d.get(k) for k in ['baseline', 'after_disconnect', 'cooldown']}
    result['metric_observation_failures'] = sum('unavailable' in r.get('metrics', {}) or 'http_status' in r.get('metrics', {}) for r in rows)
    result['postgres_observation_failures'] = sum('error' in r.get('postgres', {}) for r in rows)
    result['generator_groups'] = {}
    for i, load in enumerate([d.get('spec', {}).get('load', {})] + d.get('spec', {}).get('extra_loads', [])):
        label = 'generator' if i == 0 else f'generator_{i}'
        f = final(d, label)
        origin = load.get('connections', 100) / load.get('ramp_per_sec', 100) + load.get('warmup_secs', 3)
        phases = []
        for p in load.get('phases', []):
            phases.append(window([r for r in rows if origin <= r['elapsed_s'] < origin + p['seconds']]))
            origin += p['seconds']
        result['generator_groups'][label] = dict(load=load, final=f, phases=phases)
    if d.get('name', '').startswith('soak'):
        result['five_minute_windows'] = [window([r for r in rows if begin <= r['elapsed_s'] < begin + 300])
                                         for begin in range(0, int(max((r['elapsed_s'] for r in rows), default=0)) + 1, 300)]
        intervals = []
        previous = None
        for event in d.get('generator', []):
            if event.get('event') not in ('sample', 'final'):
                continue
            if previous:
                span = event['elapsed_s'] - previous['elapsed_s']
                if span > 0:
                    a = previous['stats']; b = event['stats']
                    row = dict(end_s=event['elapsed_s'], duration_s=span,
                               counter_deltas={k: v - a.get('counters', {}).get(k, 0) for k, v in b.get('counters', {}).items()})
                    row['latency_interval_mean_ms'] = {}
                    for k, v in b.get('latencies', {}).items():
                        old = a.get('latencies', {}).get(k, {})
                        count = v['count'] - old.get('count', 0)
                        if count:
                            row['latency_interval_mean_ms'][k] = (v['mean_ms'] * v['count'] - old.get('mean_ms', 0) * old.get('count', 0)) / count
                    intervals.append(row)
            previous = event
        result['generator_intervals'] = intervals
    return result


if __name__ == '__main__':
    results = {}
    peaks = []
    interrupted_or_failed = []
    for path in sorted(OUT.glob('*.json')):
        d = json.loads(path.read_text())
        if isinstance(d, dict) and 'samples' in d and d.get('ended_epoch'):
            results[path.stem] = analyze(d)
            snapshots = [(k, d.get(k, {})) for k in ['baseline', 'after_disconnect', 'cooldown']]
            snapshots += [(f"sample@{r['elapsed_s']}", r) for r in d['samples']]
            for phase, row in snapshots:
                rss = row.get('server', {}).get('rss_kib')
                if rss is not None:
                    peaks.append(dict(case=path.stem, phase=phase, server_rss_kib=rss))
            if d.get('error'):
                interrupted_or_failed.append(dict(case=path.stem, error=d['error']))
    (OUT / 'resource-evidence.json').write_text(json.dumps(results, indent=2) + '\n')
    overview = dict(case_records=len(results),
                    largest_server_rss=max(peaks, key=lambda r: r['server_rss_kib']) if peaks else None,
                    interrupted_or_failed=interrupted_or_failed,
                    scope='All ended raw case files, including failed/cancelled experiments; sampled server RSS including baseline/disconnect/cooldown, excluding generator/PG RSS')
    (OUT / 'audit-overview.json').write_text(json.dumps(overview, indent=2) + '\n')
    print(f'Wrote resource evidence for {len(results)} completed cases')
