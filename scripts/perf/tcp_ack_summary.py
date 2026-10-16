#!/usr/bin/env python3
"""Summarize paired TCP ACK runs using matched CPU / EventAccepted windows."""
import argparse
import hashlib
import json
from pathlib import Path
import re


def retransmits(text):
    match = re.search(r'(\d+) data packets? .* retransmitted', text)
    return int(match[1]) if match else None


def summarize(path):
    raw = json.loads(path.read_text())
    if 'result' not in raw or 'tcp' not in raw['result']['groups']:
        return None
    assert raw['loadgen_exit'] == 0, path
    assert raw['server_stop'] == {'exit_code': 0, 'forced': False}, path
    seconds = raw['plan']['seconds']
    group = raw['result']['groups']['tcp']
    client = group['counters']
    samples = [s for s in raw['samples'] if 0 <= s['t'] < seconds and s.get('server') and 'counters' in s]
    assert len(samples) >= 2, path
    first, last = samples[0], samples[-1]
    events = last['counters']['events_accepted'] - first['counters']['events_accepted']
    cpu = last['server']['cpu_seconds'] - first['server']['cpu_seconds']
    elapsed = last['t'] - first['t']
    assert events > 0 and cpu >= 0 and elapsed > 0, path
    status = raw['cooldown']['status']
    assert all(status[key] == 0 for key in ['event_count', 'event_bytes', 'pending_required']), path
    assert all(count == 0 for count in status['active_connections'].values()), path
    assert status['runtime_tasks'] == raw['idle']['status']['runtime_tasks'], path
    before, after = [retransmits(raw.get(key, '')) for key in ['tcp_before', 'tcp_after']]
    packets = re.search(r'(\d+) packets? sent', raw.get('tcp_after', ''))
    kernel_usable = bool(packets and int(packets[1]) > 0)
    offered = next(g['rate'] for g in raw['plan']['groups'] if g['label'] == 'tcp')
    return dict(name=raw['name'], input_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                server_sha256=raw['server_sha256'], loadgen_sha256=raw['loadgen_sha256'], plan=raw['plan'],
                receipt_per_second=client.get('accepted', 0) / seconds,
                receipt_percent=100 * client.get('accepted', 0) / max(1, client.get('attempted', 0)),
                offered_percent=100 * client.get('accepted', 0) / (offered * seconds),
                cpu_us_per_event=cpu * 1e6 / events, cpu_percent=100 * cpu / elapsed,
                cpu_sample_events=events, cpu_sample_seconds=elapsed,
                rss_peak_kib=max(s['server']['rss_kib'] for s in samples),
                latency=group['latencies'].get('acceptance', {}), client=client,
                host_tcp_retransmits=after - before if kernel_usable and before is not None and after is not None else None,
                tcp_kernel_counters_usable=kernel_usable,
                pending_peak=max(s['status']['pending_required'] for s in samples),
                rss_first_kib=first['server']['rss_kib'], rss_last_kib=last['server']['rss_kib'],
                cooldown=status,
                counters={key: value - raw['idle']['counters'].get(key, 0)
                          for key, value in raw['cooldown']['counters'].items()
                          if key in ['events_accepted', 'events_rejected', 'sink_acks', 'sink_drops', 'sink_failures', 'ingress_admission_rejects']})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    rows = [row for path in sorted(args.input.glob('*.json')) if (row := summarize(path)) is not None]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(schema_version=1, rows=rows), indent=2) + '\n')
    for row in rows:
        print(f"{row['name']}: {row['receipt_per_second']:.1f}/s {row['cpu_us_per_event']:.2f} CPU-us/event P99={row['latency'].get('p99_ms')}ms accepted={row['receipt_percent']:.3f}%")


if __name__ == '__main__':
    main()
