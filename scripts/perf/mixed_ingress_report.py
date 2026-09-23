#!/usr/bin/env python3
"""Summarize preserved JSON evidence without mixing offered, sent, and accepted work."""
import argparse
import json
from pathlib import Path
import statistics
import re
from datetime import datetime, timezone

ERRORS = ('connect_refused', 'connect_timeout', 'connect_address_unavailable', 'tls_failure', 'auth_failure',
          'http_overloaded', 'http_status_error', 'write_timeout', 'read_timeout',
          'remote_close', 'disconnects', 'udp_no_ack', 'udp_invalid_ack', 'udp_send_error',
          'protocol_error', 'unexpected_receipt', 'unconfirmed', 'udp_late_ack')


def phase_summary(row):
    """Counter deltas over actual sample intervals; no invented interval percentiles."""
    seconds = row['plan']['seconds']
    intervals = [('first_third', 0, seconds/3), ('last_third', seconds*2/3, seconds)]
    if row.get('sink_restored_at'):
        restored = row['sink_restored_at']
        intervals += [('sink_slow', 0, restored), ('sink_recovered', min(restored+5, seconds-1), seconds)]
    if row.get('shutdown_at'):
        intervals += [('before_shutdown', 0, row['shutdown_at'])]
    phases = {}
    for name, start, end in intervals:
        client = [s for s in row.get('client_samples', []) if start <= s['measurement_secs'] <= end]
        server = [s for s in row['samples'] if start <= s['t'] <= end and s.get('server')]
        if len(client) < 2:
            continue
        first, last = client[0], client[-1]
        elapsed = last['measurement_secs']-first['measurement_secs']
        groups = {}
        for group, value in last['groups'].items():
            a = first['groups'][group]['counters']; b = value['counters']
            accepted = b.get('accepted', 0)-a.get('accepted', 0)
            attempted = b.get('attempted', 0)-a.get('attempted', 0)
            groups[group] = dict(accepted_per_second=accepted/elapsed,
                attempt_per_second=attempted/elapsed,
                receipt_to_attempt_delta_pct=100*accepted/attempted if attempted else None)
        phases[name] = dict(sample_start=first['measurement_secs'], sample_end=last['measurement_secs'], groups=groups,
            rss_kib=dict(minimum=min(s['server']['rss_kib'] for s in server), maximum=max(s['server']['rss_kib'] for s in server)) if server else None,
            peak_event_count=max((s['status']['event_count'] for s in server if 'status' in s), default=None),
            peak_pending_required=max((s['status']['pending_required'] for s in server if 'status' in s), default=None))
    return phases


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
    idle_matches = re.findall(r'CPU usage:.*?([0-9.]+)% idle', row.get('host_cpu_sample', ''))
    def descriptor_count(value):
        return len(re.findall(r'^f[0-9]+$', value, re.MULTILINE))
    def kernel_value(text, description):
        match = re.search(r'^\s*(\d+) ' + re.escape(description) + r'$' , text, re.MULTILINE)
        return int(match[1]) if match else None
    udp_before = kernel_value(row.get('network_before', ''), 'dropped due to full socket buffers')
    udp_after = kernel_value(row.get('network_after', ''), 'dropped due to full socket buffers')
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
                           connect_success_pct=counters.get('connected_total', 0)/counters['connect_attempts_total']*100 if counters.get('connect_attempts_total') else None,
                           latency=stats['latencies'].get('acceptance', {}), connect_latency=stats['latencies'].get('connect', {}),
                           errors={key: (None if key=='connect_address_unavailable' and row.get('loadgen_schema_version',1)<2 else counters.get(key, 0)) for key in ERRORS},
                           schedule_missed=counters.get('schedule_missed', 0), client_window_full=counters.get('client_window_full', 0),
                           authenticated_total=counters.get('authenticated_total', 0),
                           authenticated=counters.get('authenticated', 0),
                           app_connect_success_pct=counters.get('authenticated_total', 0)/counters['connect_attempts_total']*100 if counters.get('connect_attempts_total') and config['protocol'] in ('mqtt','tcp') else None)
    return dict(name=row['name'], label=row['label'], scenario=row['plan']['name'], seconds=seconds,
                timestamp=row['timestamp'],
                production_revision=row.get('production_revision', row['baseline']),
                server_sha256=row['server_sha256'], loadgen_sha256=row['loadgen_sha256'],
                groups=groups, cpu_pct=cpu, phases=phase_summary(row),
                loadgen_tokio_workers=row.get('loadgen_tokio_workers',2),
                server_tokio_workers=row.get('server_tokio_workers',10),
                loadgen_schema_version=row.get('loadgen_schema_version',1),
                host_idle_pct=float(idle_matches[-1]) if idle_matches else None,
                host_udp_full_socket_drops=udp_after-udp_before if udp_before is not None and udp_after is not None else None,
                descriptor_counts=dict(idle=descriptor_count(row.get('idle_fds','')), cooldown=descriptor_count(row.get('cooldown_fds',''))),
                peak_connections={p:max((s['status']['active_connections'][p] for s in server_samples),default=None) for p in ['http','mqtt','tcp','udp']},
                peak_event_count=max((s['status']['event_count'] for s in server_samples),default=None),
                rss_kib=dict(first=usage[0]['server']['rss_kib'], last=usage[-1]['server']['rss_kib'],
                             minimum=min(s['server']['rss_kib'] for s in usage), peak=max(s['server']['rss_kib'] for s in usage)) if usage else {},
                management_failures=sum('management_error' in s and 'CannotSendRequest' not in s['management_error'] for s in samples),
                observer_state_errors=sum('CannotSendRequest' in s.get('management_error','') for s in samples),
                observer_recovery_error=row.get('cooldown_error')=='Request-sent',
                harness_version=row.get('harness_version',1),
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
        if 'plan' in row and isinstance(row.get('result'), dict) and 'groups' in row['result']:
            summary = summarize(row); summary['raw_file'] = path.name; rows.append(summary)
    aggregate = {}
    for row in rows:
        if row['exclusion']:
            continue
        scenario = re.sub(r'-(?:pair|repeat)\d+$', '', row['scenario'])
        key = row['label'] + '/' + scenario
        aggregate.setdefault(key, []).append(row)
    def distribution(values):
        values = [v for v in values if v is not None]
        return dict(n=len(values), median=statistics.median(values), minimum=min(values), maximum=max(values)) if values else None
    tables = {}
    for key, trials in aggregate.items():
        protocols = {}
        for name in trials[0]['groups']:
            entries = [r['groups'][name] for r in trials]
            protocols[name] = {field: distribution([g[field] for g in entries]) for field in
                               ['accepted_per_second','sent_per_second','success_pct','offered_coverage_pct','connect_success_pct','app_connect_success_pct']}
            protocols[name]['latency'] = {field: distribution([g['latency'].get(field) for g in entries]) for field in ['p50_ms','p95_ms','p99_ms','mean_ms']}
            protocols[name]['errors'] = {field: distribution([g['errors'][field] for g in entries]) for field in ERRORS}
        tables[key] = dict(runs=[r['name'] for r in trials], seconds=[r['seconds'] for r in trials],
                          groups=protocols, total_accepted_per_second=distribution([sum(g['accepted_per_second'] for g in r['groups'].values()) for r in trials]),
                          cpu_pct={kind:distribution([r['cpu_pct'].get(kind) for r in trials]) for kind in ['server','loadgen']},
                          peak_rss_kib=distribution([r['rss_kib'].get('peak') for r in trials]))
    if args.output:
        args.output.write_text(json.dumps(dict(schema_version=2, raw_directory='performance/mixed-ingress',
            environment=json.loads((args.directory/'baseline-environment.json').read_text()),
            conclusion=json.loads((args.directory/'conclusion.json').read_text()) if (args.directory/'conclusion.json').exists() else None,
            plan_file='performance/mixed-ingress/plan.json',
            validation_file='performance/mixed-ingress/validation.json',
            supplemental_plan_file='performance/mixed-ingress/supplement-plan.json',
            verification_files=['performance/mixed-ingress/matrix-verification.json','performance/mixed-ingress/supplement-verification.json'],
            generated_at=datetime.now(timezone.utc).isoformat(), aggregate=tables, notes=[
            'CPU 100% = one core. Histograms cover acknowledged operations only.',
            'Phase rates are counter deltas across their recorded sample intervals. ACKs can cross interval boundaries; phase receipt/attempt deltas are not matched-cohort success percentages.',
            'Connection success percentages use all owned connection attempts, including warmup, so an authentication crossing the measurement start cannot produce a percentage above 100. Raw measurement-only counters remain available.',
            'Server counter deltas span first-to-last successful sample, not exactly the client measurement interval.',
            'Error counters can overlap; never sum disconnects and their causal errors.',
            'connect_address_unavailable is separately instrumented only in diagnostic loadgen schema version 2; earlier rows preserve null for this category, whose failures were folded into remote_close.',
            'Failure counters are observed during the measurement window; timed-out warmup operations can contribute at the start boundary. Use accepted/attempted for the measured-send cohort.',
            'sent_per_second and success_pct use transmission attempts, including failed writes; the raw attempted count is authoritative.',
            'unconfirmed is a diagnostic counter, not exhaustive lost work; failed writes can abandon other pending receipts. Use accepted/attempted for receipt failure.',
            'A zero attempted count under connection failure means success is undefined; offered coverage is still zero.'
        ], rows=rows), indent=2) + '\n')
    for r in rows:
        protocols = ' '.join(f'{name}:{g["accepted_per_second"]:.0f}/s p99={g["latency"].get("p99_ms", "NA")}ms coverage={g["offered_coverage_pct"]}' for name, g in r['groups'].items() if name in ('http', 'mqtt', 'tcp', 'udp'))
        print(r['name'], protocols, f'CPU={r["cpu_pct"].get("server",0):.1f}% RSS={r["rss_kib"].get("peak")}KiB')


if __name__ == '__main__':
    main()
