#!/usr/bin/env python3
"""Read an existing case snapshot and optional generator log; no service probes."""
import argparse
import json
import pathlib
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('result', type=pathlib.Path)
parser.add_argument('--generator-log', type=pathlib.Path)
args = parser.parse_args()
for attempt in range(3):
    try:
        data = json.loads(args.result.read_text())
        break
    except json.JSONDecodeError:
        if attempt == 2:
            raise
        time.sleep(.1)  # The runner may be replacing its progress snapshot.
rows = data.get('samples', [])
last = rows[-1] if rows else {}
previous = next((r for r in reversed(rows) if r['elapsed_s'] <= last['elapsed_s'] - 60), None)
latest = next((e for e in reversed(data.get('generator', [])) if 'stats' in e), {})
if args.generator_log:
    with args.generator_log.open('rb') as stream:
        stream.seek(0, 2)
        stream.seek(max(0, stream.tell() - 65536))
        lines = stream.read(65536).decode(errors='replace').splitlines()
    for line in reversed(lines):
        try:
            event = json.loads(line)
            if 'stats' in event:
                latest = event
                break
        except json.JSONDecodeError:
            continue
server = last.get('server', {})
metrics = last.get('metrics', {})
database = last.get('postgres', {})
cpu = None
if (previous and 'cpu_seconds' in previous.get('server', {}) and 'cpu_seconds' in server
        and previous['server'].get('pid') == server.get('pid')):
    cpu = (server['cpu_seconds'] - previous['server']['cpu_seconds']) / (last['elapsed_s'] - previous['elapsed_s'])
counters = latest.get('stats', {}).get('counters', {})
print(json.dumps(dict(
    name=data.get('name'), finished=bool(data.get('ended_epoch')), error=data.get('error'),
    elapsed_s=last.get('elapsed_s'), generator_elapsed_s=latest.get('elapsed_s'),
    rss_mib=server.get('rss_kib', 0) / 1024 if 'rss_kib' in server else None,
    recent_cpu_cores=cpu, fds=server.get('fds'),
    resources={k: metrics.get('netbaiot_' + k) for k in ['registered_sessions', 'runtime_alive_tasks', 'queue_depth', 'queue_bytes', 'ingress_inflight', 'timeouts_total']},
    postgres={k: database.get(k) for k in ['rows', 'db_bytes', 'connections', 'lock_waiters', 'outbox_pending', 'oldest_outbox_ms']},
    counters={k: (counters.get(k, 0) if latest else None) for k in ['accepted', 'published', 'client_errors', 'ack_timeouts', 'connected', 'command_acks', 'commands_queued']},
    cumulative_ack=latest.get('stats', {}).get('latencies', {}).get('application_ack'),
), separators=(',', ':')))
