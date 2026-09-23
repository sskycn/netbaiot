#!/usr/bin/env python3
"""Verify and summarize the serialized device protocol removal experiment."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics

KINDS = ('mqtt', 'tcp', 'udp', 'mqtts', 'tls-tcp')

def median(values):
    return statistics.median(values)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def summarize(root):
    builds = {side: json.loads((root / f'{side}-build.json').read_text()) for side in ('before', 'after')}
    expected_servers = {side: next(value['sha256'] for path, value in build['artifacts'].items() if path.endswith(f'{side}-server')) for side, build in builds.items()}
    driver = builds['before']['artifacts']['target/remove-device-http/loadgen']['sha256']
    rows = []
    for side in ('before', 'after'):
        for kind in KINDS:
            for repeat in range(3):
                path = root / f'{side}-remove-http-{kind}-{repeat}.json'
                raw = json.loads(path.read_text())
                assert raw['server_sha256'] == expected_servers[side], path
                assert raw['loadgen_sha256'] == driver, path
                assert raw['loadgen_exit'] == 0 and raw['load_stop'] == {'exit_code': 0, 'forced': False}, path
                assert raw['server_stop'] == {'exit_code': 0, 'forced': False}, path
                seconds = raw['result']['duration_secs']
                assert seconds == raw['plan']['seconds'] == 30, path
                assert 30 <= raw['result']['measurement_secs'] <= 35, path
                assert len(raw['result']['groups']) == 1, path
                group = next(iter(raw['result']['groups'].values()))
                counts = group['counters']
                latency = group['latencies']['acceptance']
                assert latency['count'] == counts['accepted'] and 0 < counts['accepted'] <= counts['attempted'], path
                assert not counts.get('protocol_error', 0) and not counts.get('udp_invalid_ack', 0), path
                cooldown = raw['cooldown']['status']
                assert all(cooldown[key] == 0 for key in ('event_count', 'event_bytes', 'pending_required')), path
                assert all(cooldown['active_connections'][key] == 0 for key in ('mqtt', 'tcp', 'udp')), path
                if side == 'after':
                    assert 'http' not in cooldown['active_connections'], path
                assert cooldown['runtime_tasks'] == raw['idle']['status']['runtime_tasks'], path
                fd_count = lambda text: sum(line.startswith('f') and line[1:].isdigit() for line in text.splitlines())
                assert fd_count(raw['cooldown_fds']) == fd_count(raw['idle_fds']), path
                assert all(item['name'] == 'spool/mqtt-runtime.state' for item in raw['spool_files']), path
                samples = [item for item in raw['samples'] if 0 <= item['t'] < seconds and item.get('server')]
                assert len(samples) >= 20, path
                cpu = (samples[-1]['server']['cpu_seconds'] - samples[0]['server']['cpu_seconds']) / (samples[-1]['t'] - samples[0]['t'])
                counter_delta = {key: value - raw['idle']['counters'].get(key, 0) for key, value in raw['cooldown']['counters'].items()}
                rows.append(dict(side=side, protocol=kind, repeat=repeat, raw=str(path), raw_sha256=digest(path), accepted_per_second=counts['accepted']/seconds,
                    accepted_attempted_pct=100*counts['accepted']/counts['attempted'], offered_per_second=raw['plan']['groups'][0]['rate'],
                    accepted_offered_pct=100*counts['accepted']/seconds/raw['plan']['groups'][0]['rate'],
                    receipt_p99_ms=latency['p99_ms'], server_cpu_cores=cpu,
                    server_peak_rss_kib=max(item['server']['rss_kib'] for item in samples),
                    loadgen_peak_rss_kib=max(item['loadgen']['rss_kib'] for item in samples if item.get('loadgen')),
                    counters=counts, full_run_server_counters=counter_delta,
                    cleanup=dict(events=0,event_bytes=0,pending_required=0,device_connections=0,runtime_tasks=cooldown['runtime_tasks'],fds=fd_count(raw['cooldown_fds'])),
                    actual_loadgen_workers=4))
    comparisons = []
    for kind in KINDS:
        medians = {}
        for side in ('before', 'after'):
            relevant = [row for row in rows if row['side'] == side and row['protocol'] == kind]
            keys = ('accepted_per_second','accepted_attempted_pct','accepted_offered_pct','receipt_p99_ms','server_cpu_cores','server_peak_rss_kib','loadgen_peak_rss_kib')
            medians[side] = {key: median([row[key] for row in relevant]) for key in keys}
            medians[side]['accepted_per_second_range'] = [min(row['accepted_per_second'] for row in relevant),max(row['accepted_per_second'] for row in relevant)]
            medians[side]['receipt_p99_ms_range'] = [min(row['receipt_p99_ms'] for row in relevant),max(row['receipt_p99_ms'] for row in relevant)]
        comparisons.append(dict(protocol=kind,**medians,accepted_rate_delta_pct=100*(medians['after']['accepted_per_second']/medians['before']['accepted_per_second']-1)))
    return dict(schema_version=1,baseline=builds['before']['revision'],final_implementation=builds['after']['implementation_revision'],
        environment='Same-host Apple M4 macOS loopback; server 10 Tokio workers; frozen loadgen 4 workers.',
        methodology=dict(repeats=3,measurement_seconds=30,warmup_seconds=5,ramp_seconds=1,rate_formula='real accepted receipts / fixed 30-second measurement window',
            payload_bytes=256,sink='Immediate required AuditSink; real webhook/confirmed stream covered by correctness tests, not this throughput experiment.',
            ordering='All before trials then all after trials; serialized, without compilation/tests. No randomized or interleaved treatment.',
            latency='Successful receipt histogram; excludes unconfirmed/rejected attempts. Not a loss-free capacity claim.',
            worker_correction='Before raw loadgen_tokio_workers=2 is requested environment only; executable explicitly used 4 in both treatments. See metadata-corrections.json.'),
        comparisons=comparisons,runs=rows,cleanup_checks='30/30 passed',builds=builds,
        validation=json.loads((root/'validation/index.json').read_text()),
        limitations=['30-second loopback windows, N=3; no confidence interval or production capacity certification.',
            'Stream modes deliberately approach admission limits; report accepted/offered and disconnects alongside rate.',
            'UDP is sessionless and unencrypted; recent in-memory state can be lost on abrupt crash.',
            'No replacement automatic device config-pull protocol. Management configuration and application commands remain.',
            'No separate-host, multi-hour soak, power-loss or syscall profiling in this task.'])

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=Path('docs/performance/remove-device-http'))
    parser.add_argument('--output', type=Path, default=Path('docs/remove-device-http-results.json'))
    args = parser.parse_args()
    result = summarize(args.input)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
    for row in result['comparisons']:
        print(f"{row['protocol']}: {row['before']['accepted_per_second']:.1f} -> {row['after']['accepted_per_second']:.1f} ({row['accepted_rate_delta_pct']:+.2f}%)")
    print(result['cleanup_checks'])

if __name__ == '__main__':
    main()
