#!/usr/bin/env python3
"""Summarize device_protocol_benchmark UDP records without conflating windows.

CPU/event uses process CPU and gateway EventAccepted deltas from the same pair
of samples. Receipt rate/P99 and client counters use the load generator's window.
Kernel drop counters are host-wide and include warmup/cooldown.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re


def kernel_drops(text):
    match = re.search(r'(\d+) dropped due to full socket buffers', text)
    return int(match[1]) if match else None


def summarize(path):
    data = json.loads(path.read_text())
    if 'result' not in data or 'udp' not in data['result']['groups']:
        return None
    duration = data['plan']['seconds']
    samples = [s for s in data['samples'] if 0 <= s['t'] < duration
               and s.get('server') and 'counters' in s]
    if len(samples) < 2:
        raise ValueError(f'insufficient samples: {path.name}')
    first, last = samples[0], samples[-1]
    accepted = last['counters']['events_accepted'] - first['counters']['events_accepted']
    cpu = last['server']['cpu_seconds'] - first['server']['cpu_seconds']
    elapsed = last['t'] - first['t']
    if elapsed <= 0 or accepted < 0 or cpu < 0:
        raise ValueError(f'non-monotonic measurement samples: {path.name}')
    group = data['result']['groups']['udp']
    client = group['counters']
    counters = data.get('cooldown', {}).get('counters', {})
    before, after = [kernel_drops(data.get(key, '')) for key in ['network_before', 'network_after']]
    keys = ['udp_datagrams', 'udp_accepted', 'udp_accepted_duplicates', 'udp_acks_sent',
            'udp_ack_send_failures', 'udp_overload_drops', 'events_accepted', 'events_rejected',
            'ingress_admission_rejects', 'sink_acks', 'sink_retries', 'sink_failures', 'sink_drops',
            'auth_cache_hits', 'auth_cache_misses']
    return dict(
        name=data['name'], input=path.name,
        input_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        server_sha256=data['server_sha256'], loadgen_sha256=data['loadgen_sha256'],
        plan=data['plan'], receipt_per_second=client.get('accepted', 0) / duration,
        receipt_percent=100 * client.get('accepted', 0) / max(1, client.get('attempted', 0)),
        cpu_us_per_event=cpu * 1e6 / accepted if accepted else None,
        cpu_percent=100 * cpu / elapsed, cpu_sample_seconds=elapsed,
        cpu_sample_events=accepted,
        gateway_events_per_second=accepted / elapsed,
        latency=group['latencies'].get('acceptance', {}), client=client,
        rss_peak_kib=max(s['server']['rss_kib'] for s in samples),
        rss_first_kib=first['server']['rss_kib'], rss_last_kib=last['server']['rss_kib'],
        pending_peak=max(s['status']['pending_required'] for s in samples),
        tasks_peak=max(s['status']['runtime_tasks'] for s in samples),
        cooldown=data.get('cooldown', {}).get('status'),
        full_run_counters={key: counters.get(key) for key in keys},
        kernel_full_socket_drops=after - before if before is not None and after is not None else None,
        stop=data['server_stop'],
        diagnostic_means_us={key: counters.get(key + '_sum', 0) / counters[key + '_count']
                             for key in ['authentication_us', 'codec_latency_us',
                                         'admission_wait_us', 'eventbus_queue_latency_us',
                                         'event_bus_lock_wait_us', 'event_bus_lock_hold_us']
                             if counters.get(key + '_count', 0)},
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--match', default='*.json', help='Input filename glob (for compact evidence groups)')
    args = parser.parse_args()
    rows, incomplete = [], []
    for path in sorted(args.input.glob(args.match)):
        row = summarize(path)
        if row is not None:
            rows.append(row)
        else:
            data = json.loads(path.read_text())
            if 'name' in data:
                incomplete.append(dict(
                    name=data['name'], input=path.name,
                    input_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                    plan=data['plan'], server_sha256=data['server_sha256'],
                    reason='no completed load generator result',
                    sample_times=[s['t'] for s in data.get('samples', [])],
                    server_stop=data.get('server_stop'), load_stop=data.get('load_stop'),
                ))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(schema_version=1, rows=rows, incomplete_runs=incomplete), separators=(',', ':')) + '\n')
    print(f'{len(rows)} runs summarized; {len(incomplete)} incomplete runs retained')


if __name__ == '__main__':
    main()
