#!/usr/bin/env python3
"""Verify frozen artifacts, cleanup and three-protocol ownership-removal measurements."""
import hashlib
import json
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / 'docs/performance/remove-device-config'


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fd_count(text):
    return sum(line.startswith('f') and line[1:].isdigit() for line in text.splitlines())


def summarize():
    builds = {side: json.loads((RAW / f'{side}-build.json').read_text()) for side in ('before', 'after')}
    servers = {side: build['artifacts'][f'target/remove-device-config/{side}-server']['sha256'] for side, build in builds.items()}
    driver = builds['before']['artifacts']['target/remove-device-config/loadgen']['sha256']
    rows, excluded = [], []
    for path in sorted(RAW.glob('config-*.json')):
        raw = json.loads(path.read_text())
        if 'plan' not in raw:
            continue  # Environment records.
        side = 'before' if raw['label'].startswith('config-before') else 'after'
        assert raw['server_sha256'] == servers[side], path
        assert raw['loadgen_sha256'] == driver, path
        assert raw['loadgen_tokio_workers'] == 4 and raw['server_tokio_workers'] == 10, path
        if 'result' not in raw:
            assert not raw['samples'] and raw['load_stop'] is None, path
            excluded.append(dict(raw=str(path.relative_to(ROOT)), reason='server failed before readiness/load', server_log=raw['server_log_tail']))
            continue
        assert raw['loadgen_exit'] == 0 and raw['load_stop'] == {'exit_code': 0, 'forced': False}, path
        assert raw['server_stop'] == {'exit_code': 0, 'forced': False}, path
        assert raw['plan']['seconds'] == raw['result']['duration_secs'] == 30, path
        assert 30 <= raw['result']['measurement_secs'] <= 35, path
        assert len(raw['result']['groups']) == 1, path
        protocol, group = next(iter(raw['result']['groups'].items()))
        counts, latency = group['counters'], group['latencies']['acceptance']
        assert latency['count'] == counts['accepted'] and 0 < counts['accepted'] <= counts['attempted'], path
        assert not counts.get('protocol_error', 0) and not counts.get('udp_invalid_ack', 0), path
        idle, cool = raw['idle']['status'], raw['cooldown']['status']
        assert all(cool[key] == 0 for key in ('event_count', 'event_bytes', 'pending_required')), path
        assert cool['active_connections'] == {'mqtt': 0, 'tcp': 0, 'udp': 0}, path
        assert cool['runtime_tasks'] == idle['runtime_tasks'], path
        assert fd_count(raw['idle_fds']) == fd_count(raw['cooldown_fds']), path
        assert all(item['name'] == 'spool/mqtt-runtime.state' for item in raw['spool_files']), path
        if side == 'after':
            assert 'config_cache_entries' not in cool and 'config_cache_bytes' not in cool, path
        samples = [s for s in raw['samples'] if 0 <= s['t'] < 30 and s.get('server')]
        assert len(samples) >= 20, path
        cpu = (samples[-1]['server']['cpu_seconds'] - samples[0]['server']['cpu_seconds']) / (samples[-1]['t'] - samples[0]['t'])
        rows.append(dict(side=side, protocol=protocol, repeat=raw['repeat'], raw=str(path.relative_to(ROOT)), raw_sha256=digest(path),
            accepted_per_second=counts['accepted']/30, accepted_attempted_pct=100*counts['accepted']/counts['attempted'],
            accepted_offered_pct=100*counts['accepted']/30/raw['plan']['groups'][0]['rate'],
            receipt_p95_ms=latency['p95_ms'], receipt_p99_ms=latency['p99_ms'], server_cpu_cores=cpu,
            idle_rss_kib=raw['idle_process']['rss_kib'], peak_rss_kib=max(s['server']['rss_kib'] for s in samples),
            counters=counts, full_run_server_counters={k:v-raw['idle']['counters'].get(k,0) for k,v in raw['cooldown']['counters'].items()},
            cleanup=dict(events=0, event_bytes=0, pending_required=0, device_connections=0, runtime_tasks=cool['runtime_tasks'], fds=fd_count(raw['cooldown_fds']))))
    comparisons = []
    for protocol in ('mqtt', 'tcp', 'udp'):
        medians = {}
        for side in ('before', 'after'):
            selected = [r for r in rows if r['side'] == side and r['protocol'] == protocol]
            assert len(selected) == 3, (side, protocol)
            keys = ('accepted_per_second', 'accepted_attempted_pct', 'accepted_offered_pct', 'receipt_p95_ms', 'receipt_p99_ms', 'server_cpu_cores', 'idle_rss_kib', 'peak_rss_kib')
            medians[side] = {key: statistics.median(r[key] for r in selected) for key in keys}
            for key in ('accepted_per_second', 'receipt_p99_ms'):
                medians[side][key+'_range'] = [min(r[key] for r in selected), max(r[key] for r in selected)]
        comparisons.append(dict(protocol=protocol, **medians, accepted_rate_delta_pct=100*(medians['after']['accepted_per_second']/medians['before']['accepted_per_second']-1)))
    result = dict(schema_version=1, baseline=builds['before']['revision'], implementation=builds['after']['revision'],
        methodology=dict(repeats=3, measurement_seconds=30, warmup_seconds=5, ramp_seconds=1, payload_bytes=256,
            environment='Apple M4 macOS, same-host loopback; server 10 workers, frozen loadgen 4 workers',
            offered_rates={'mqtt':45000,'tcp':45000,'udp':80000}, sink='Immediate required AuditSink',
            ordering='Serialized before, then after; no concurrent compilation/tests; not randomized',
            latency='Successful EventAccepted receipts only; excludes unconfirmed/rejected and unsent work',
            memory='1-second process RSS samples; idle fixture has credentials/product profiles and zero business configs even before removal'),
        builds=builds, comparisons=comparisons, runs=rows, excluded=excluded, cleanup_checks=f'{len(rows)}/{len(rows)} passed',
        limitations=['30-second N=3 loopback windows, not production capacity or loss-free throughput.',
            'Stream modes hit admission limits; UDP client window may shed scheduled sends. See accepted/offered and raw counters.',
            'Peak and idle RSS are sampled process values, not exact object accounting or proof of a memory reduction.',
            'No separate-host, multi-hour soak, power-loss or syscall profiling. TLS correctness is tested but TLS throughput is not rerun here.'])
    (ROOT/'docs/remove-device-config-results.json').write_text(json.dumps(result, indent=2)+'\n')
    for row in comparisons:
        print(f"{row['protocol']}: {row['before']['accepted_per_second']:.1f} -> {row['after']['accepted_per_second']:.1f} ({row['accepted_rate_delta_pct']:+.2f}%), p95 {row['before']['receipt_p95_ms']} -> {row['after']['receipt_p95_ms']}, p99 {row['before']['receipt_p99_ms']} -> {row['after']['receipt_p99_ms']}")
    print(result['cleanup_checks'])


if __name__ == '__main__':
    summarize()
