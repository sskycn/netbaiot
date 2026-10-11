#!/usr/bin/env python3
"""Summarize local codec audit evidence without committing samples or logs."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import statistics


def digest(path):
    return dict(size_bytes=path.stat().st_size,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def distribution(values):
    return dict(median=statistics.median(values), minimum=min(values), maximum=max(values))


def rounded(value):
    if isinstance(value, float):
        return round(value, 6)
    if isinstance(value, dict):
        return {k:rounded(v) for k,v in value.items()}
    if isinstance(value, list):
        return [rounded(v) for v in value]
    return value


def case_key(row):
    return tuple(row[k] for k in ('codec', 'protocol', 'mqtt_version', 'qos', 'workload'))


def queues_empty(row):
    status = row['status']
    return (not any(status[k] for k in ('event_count', 'event_bytes', 'pending_required'))
            and not any(status['active_connections'].values())
            and not any(s[k] for s in status['sink_diagnostics']
                        for k in ('queue_count', 'queue_bytes', 'inflight')))


def aggregate(rows):
    groups = {}
    for row in rows:
        assert not row['errors'], 'failed runs cannot be summarized as successful evidence'
        groups.setdefault(case_key(row), []).append(row)
    result = {}
    for key, group in groups.items():
        rate = 'commands_per_s' if key[-1] == 'command' else 'events_per_s'
        item = dict(repeats=len(group), events=sum(r['events'] for r in group),
                    commands=sum(r['command_count'] for r in group),
                    rate_per_s=distribution([r[rate] for r in group]),
                    cpu_pct=distribution([r['cpu_pct'] for r in group]),
                    idle_rss_kib=statistics.median(r['idle_rss_kib'] for r in group),
                    median_rss_kib=statistics.median(r['median_rss_kib'] for r in group),
                    peak_sampled_rss_kib=max(r['peak_sampled_rss_kib'] for r in group),
                    sampled_growth_kib_per_connection=statistics.median(
                        r['sampled_growth_kib_per_connection'] for r in group),
                    errors=0, final_queues_and_connections_empty=all(queues_empty(r) for r in group))
        for metric in ('receipt', 'webhook_response', 'command_received'):
            values = [r[metric] for r in group if r[metric] is not None]
            item[metric] = None if not values else dict(
                count=sum(v['count'] for v in values),
                **{k:statistics.median(v[k] for v in values) for k in ('p50_ms', 'p95_ms', 'p99_ms')},
                p99_run_min_ms=min(v['p99_ms'] for v in values),
                p99_run_max_ms=max(v['p99_ms'] for v in values))
        result[key] = item
    return result


def normalized_config(value):
    return {**value, 'device_ingress':'loopback:ephemeral',
            'management_http':'loopback:ephemeral', 'spool_directory':'<output>/spool',
            'delivery_url':'http://loopback:ephemeral/events'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('target/performance/codec-audit'))
    parser.add_argument('--output', type=Path, default=Path('docs/performance/codec-audit'))
    parser.add_argument('--network-only', action='store_true')
    parser.add_argument('--print-summary', action='store_true')
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    files, environments, campaigns = {}, {}, {}

    def remember(path):
        files[str(path)] = digest(path)

    for phase in ('matrix', 'commands-bounded', 'slow32'):
        roots = {v:args.root / v / phase for v in ('before', 'after')}
        present = [p.exists() for p in roots.values()]
        if not any(present):
            continue
        if not all(present):
            raise ValueError(f'incomplete before/after phase: {phase}')
        raw, env, config = {}, {}, {}
        for version, directory in roots.items():
            raw[version] = json.loads((directory / 'summary.json').read_text())
            env[version] = json.loads((directory / 'environment.json').read_text())
            config[version] = json.loads((directory / 'server.json').read_text())
            for path in directory.iterdir():
                if path.name in ('summary.json', 'environment.json', 'server.json', 'server.log'):
                    remember(path)
        assert env['before']['harness_sha256'] == env['after']['harness_sha256']
        assert normalized_config(config['before']) == normalized_config(config['after'])
        options = {v:{k:x for k,x in e['arguments'].items()
                      if k not in ('server', 'output', 'revision')} for v,e in env.items()}
        assert options['before'] == options['after']
        grouped = {v:aggregate(rows) for v,rows in raw.items()}
        assert grouped['before'].keys() == grouped['after'].keys()
        cases = []
        for key, baseline in grouped['before'].items():
            candidate = grouped['after'][key]
            assert baseline['repeats'] == candidate['repeats'] == options['before']['repeats']
            cases.append(dict(zip(('codec','protocol','mqtt_version','qos','workload'), key),
                              before=baseline, after=candidate,
                              rate_change_pct=(candidate['rate_per_s']['median'] /
                                               baseline['rate_per_s']['median']-1)*100))
        campaigns[phase] = dict(
            options=options['before'], cases=cases,
            runs_per_version={v:len(rows) for v,rows in raw.items()},
            totals={v:dict(events=sum(r['events'] for r in rows),
                           commands=sum(r['command_count'] for r in rows), errors=0,
                           all_final_queues_and_connections_empty=all(queues_empty(r) for r in rows),
                           peak_sampled_rss_kib=max(r['peak_sampled_rss_kib'] for r in rows))
                    for v,rows in raw.items()},
            rate_change_pct=distribution([r['rate_change_pct'] for r in cases]))
        environments[phase] = dict(versions=env,
                                  configuration=dict(development=config['before']['development'],
                                      limits=config['before']['limits'],
                                      credential_count=len(config['before']['credentials']),
                                      credential_scope='public synthetic fixtures only',
                                      network='plaintext loopback, independent required HTTP 2xx ACK sink'))
    micro = {'before':{}, 'after':{}}
    for version in ('before', 'after'):
        if args.network_only:
            break
        path = args.root / version / 'resources.csv'
        remember(path)
        groups = {}
        for row in csv.DictReader(path.open()):
            groups.setdefault((row['codec'], int(row['fields']), row['operation']), []).append(row)
        micro[version] = {key:dict(repeats=len(rows), wire_bytes=int(rows[0]['wire_bytes']),
            **{column:distribution([float(r[column]) for r in rows])
               for column in ('mean_ns','allocations_per_op','allocated_bytes_per_op')})
            for key,rows in groups.items()}
    assert micro['before'].keys() == micro['after'].keys()
    benchmarks = [dict(codec=key[0], fields=key[1], operation=key[2],
                       before=micro['before'][key], after=micro['after'][key])
                  for key in micro['before']]
    measured = {}
    allocation_log = args.root / 'allocation-matrix.log'
    for line in ([] if args.network_only else allocation_log.read_text().splitlines()):
        values = line.split(',')
        if len(values) == 7 and values[0] in ('cbor','msgpack','protobuf'):
            measured.setdefault(values[0], []).append(values)
    allocations = {codec:dict(cases=len(rows),
        successful_cases=sum(r[3] == 'true' for r in rows),
        success_max_cumulative_bytes=max(int(r[-1]) for r in rows if r[3] == 'true'),
        rejection_max_cumulative_bytes=max(int(r[-1]) for r in rows if r[3] == 'false'))
        for codec,rows in measured.items()}
    for path in args.root.iterdir():
        if path.is_file() and path.suffix == '.log':
            remember(path)
    for version in ('before', 'after'):
        if not args.network_only:
            remember(args.root / version / 'server')
        for path in (args.root / version).glob('*.log'):
            remember(path)
    for path in ('scripts/perf/codec_campaign.py', 'scripts/perf/codec_audit_summary.py',
                 'crates/netbaiot-codecs/benches/codecs.rs',
                 'crates/netbaiot-codecs/tests/fixtures/generate.py', 'Cargo.lock'):
        remember(Path(path))
    summary = rounded(dict(version=1,
        aggregation='median of per-run percentiles; no pooled percentile or significance claim',
        campaigns=campaigns, microbenchmarks=benchmarks, allocation_matrix=allocations))
    for name, value in (('summary.json',summary), ('environment.json',environments),
                        ('manifest.json',dict(version=1, availability='raw local files in ignored target; SHA-256 validation and reproducible source inputs, no samples committed', files=files))):
        path = args.output / name
        # Case tables are already aggregates; compact them below the ordinary
        # 256 KiB evidence budget rather than adding a policy exception.
        encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':')) if name == 'summary.json' else json.dumps(value, indent=2, ensure_ascii=False)
        path.write_text(encoded+'\n')
        print(f'{path}: {path.stat().st_size} bytes')
    if args.print_summary:
        print('CODEC_AUDIT_ENVIRONMENT='+json.dumps(environments, separators=(',', ':')))
        print('CODEC_AUDIT_SUMMARY='+json.dumps(summary, separators=(',', ':')))


if __name__ == '__main__':
    main()
