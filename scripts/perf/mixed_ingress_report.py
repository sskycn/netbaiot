#!/usr/bin/env python3
"""Summarize preserved JSON evidence without mixing offered, sent, and accepted work."""
import argparse
import json
from pathlib import Path
import statistics

ERRORS = ('connect_refused', 'connect_timeout', 'tls_failure', 'auth_failure',
          'http_overloaded', 'http_status_error', 'write_timeout', 'read_timeout',
          'remote_close', 'disconnects', 'udp_no_ack', 'udp_invalid_ack', 'udp_send_error',
          'protocol_error', 'unexpected_receipt', 'unconfirmed', 'udp_late_ack')


def summarize(row):
    seconds = row['plan']['seconds']
    samples = [s for s in row['samples'] if 0 <= s['t'] < seconds]
    usage = [s for s in samples if s.get('server') and s.get('loadgen')]
    cpu = {}
    if len(usage) >= 2:
        a, b = usage[0], usage[-1]
        cpu = {kind: (b[kind]['cpu_seconds']-a[kind]['cpu_seconds']) / (b['t']-a['t']) * 100
               for kind in ('server', 'loadgen')}
    server_samples = [s for s in samples if 'counters' in s]
    delta = {}
    if len(server_samples) >= 2:
        a, b = server_samples[0], server_samples[-1]
        delta = {key: value-a['counters'].get(key, 0) for key, value in b['counters'].items()}
    groups = {}
    for name, stats in row['result']['groups'].items():
        config = next(g for g in row['plan']['groups'] if g['label'] == name)
        counters = stats['counters']; accepted = counters.get('accepted', 0); sent = counters.get('attempted', 0)
        offered = config['rate'] * seconds if config.get('mode', 'normal') == 'normal' else 0
        groups[name] = dict(protocol=config['protocol'], offered_per_second=config['rate'],
                           accepted_per_second=accepted/seconds, sent_per_second=sent/seconds,
                           success_pct=accepted/sent*100 if sent else None,
                           offered_coverage_pct=accepted/offered*100 if offered else None,
                           initial_connect_success_pct=counters.get('connected_total', 0)/counters['connect_attempts_total']*100 if counters.get('connect_attempts_total') else None,
                           connect_success_pct=counters.get('connected', 0)/counters['connect_attempts']*100 if counters.get('connect_attempts') else None,
                           latency=stats['latencies'].get('acceptance', {}), connect_latency=stats['latencies'].get('connect', {}),
                           errors={key: counters.get(key, 0) for key in ERRORS},
                           schedule_missed=counters.get('schedule_missed', 0), client_window_full=counters.get('client_window_full', 0),
                           authenticated_total=counters.get('authenticated_total', 0),
                           authenticated=counters.get('authenticated', 0),
                           app_connect_success_pct=counters.get('authenticated', 0)/counters['connect_attempts']*100 if counters.get('connect_attempts') and config['protocol'] in ('mqtt','tcp') else None)
    return dict(name=row['name'], label=row['label'], scenario=row['plan']['name'], seconds=seconds,
                production_revision=row.get('production_revision', row['baseline']),
                server_sha256=row['server_sha256'], loadgen_sha256=row['loadgen_sha256'],
                groups=groups, cpu_pct=cpu,
                rss_kib=dict(first=usage[0]['server']['rss_kib'], last=usage[-1]['server']['rss_kib'],
                             minimum=min(s['server']['rss_kib'] for s in usage), peak=max(s['server']['rss_kib'] for s in usage)) if usage else {},
                management_failures=sum('management_error' in s for s in samples),
                peak_pending_required=max((s['status']['pending_required'] for s in server_samples), default=None),
                peak_event_bytes=max((s['status']['event_bytes'] for s in server_samples), default=None),
                peak_runtime_tasks=max((s['status']['runtime_tasks'] for s in server_samples), default=None),
                counter_delta=delta, stop=row['server_stop'], cooldown=row.get('cooldown'),
                exclusion=('Unpaced raw TLS-pending reconnect loop; retained as diagnostics, replaced by v3 bounded retry pairs.'
                           if row['plan']['name'] == 'tls_pending-256' else None))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', type=Path, default=Path(__file__).resolve().parents[2] / 'docs/performance/mixed-ingress')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    rows = []
    for path in sorted(args.directory.glob('*.json')):
        row = json.loads(path.read_text())
        if 'result' in row:
            summary = summarize(row); summary['raw_file'] = path.name; rows.append(summary)
    if args.output:
        args.output.write_text(json.dumps(dict(schema_version=1, notes=[
            'CPU 100% = one core. Histograms cover acknowledged operations only.',
            'Initial connection totals include warmup. Measurement connect counts exclude warmup.',
            'Server counter deltas span first-to-last successful sample, not exactly the client measurement interval.',
            'Error counters can overlap; never sum disconnects and their causal errors.',
            'A zero attempted count under connection failure means success is undefined; offered coverage is still zero.'
        ], rows=rows), indent=2) + '\n')
    for r in rows:
        protocols = ' '.join(f'{name}:{g["accepted_per_second"]:.0f}/s p99={g["latency"].get("p99_ms", "NA")}ms coverage={g["offered_coverage_pct"]}' for name, g in r['groups'].items() if name in ('http', 'mqtt', 'tcp', 'udp'))
        print(r['name'], protocols, f'CPU={r["cpu_pct"].get("server",0):.1f}% RSS={r["rss_kib"].get("peak")}KiB')


if __name__ == '__main__':
    main()
