#!/usr/bin/env python3
"""Offline A/B/A plan probe on existing disposable cap_dataset_* fixtures.

Run only after live workloads finish. No production migration is created. The
temporary index is removed in finally; plans and cleanup evidence are retained.
This measures an empty expired-command poll, not end-to-end command throughput
or the write amplification of maintaining another index.
"""
import json
import statistics
import sys
import time

from run_case import ROOT, pg

OUT = ROOT / 'docs/performance'


def measure(database, query):
    runs = []
    for repetition in range(4):
        raw = pg(database, "BEGIN; SET LOCAL statement_timeout='30s'; "
                 "SET LOCAL lock_timeout='2s'; EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "
                 + query + '; ROLLBACK;', timeout=35)
        plan = json.loads(raw[raw.index('['):raw.rindex(']') + 1])[0]
        if repetition:
            runs.append(plan)
    return dict(warmup_runs=1, measured_runs=3, plans=runs,
                execution_ms=[r['Execution Time'] for r in runs],
                median_ms=statistics.median(r['Execution Time'] for r in runs))


def probe(rows):
    if rows not in (10000, 100000, 1000000):
        raise ValueError('Only the three explicitly owned fixture sizes are allowed')
    database = f'cap_dataset_{rows}'
    source = OUT / f'dataset_{rows}.json'
    fixture = json.loads(source.read_text())
    if int(pg(database, 'SELECT count(*) FROM commands', timeout=30)) != rows:
        raise ValueError('Fixture row count changed; refusing an unmatched comparison')
    # Unique name prevents cleanup from touching an index belonging to a prior run.
    index = f'audit_expired_commands_{time.time_ns()}'
    if pg(database, f"SELECT to_regclass('public.{index}') IS NULL") != 't':
        raise ValueError('Probe index already exists')
    query = fixture['queries']['expired_commands']['sql']
    result = dict(database=database, rows_per_table=rows, source=source.name,
                  query_as_of_ms=fixture['query_as_of_ms'], sql=query,
                  index_sql=f'CREATE INDEX {index} ON commands(expires_at) WHERE NOT terminal',
                  scope='offline warm A/B/A; empty expired poll; no live service or production migration',
                  started_epoch=time.time())
    attempted = False
    path = OUT / f'index_probe_{rows}.json'
    try:
        result['before'] = measure(database, query)
        attempted = True
        start = time.monotonic()
        pg(database, "SET statement_timeout='120s'; SET lock_timeout='2s'; "
           + result['index_sql'], timeout=125)
        result['index_build_seconds'] = time.monotonic() - start
        result['index_bytes'] = int(pg(database, f"SELECT pg_relation_size('{index}')"))
        result['after'] = measure(database, query)
    except Exception as error:
        result['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        try:
            if attempted:
                pg(database, "SET statement_timeout='30s'; SET lock_timeout='2s'; "
                   f'DROP INDEX IF EXISTS {index}', timeout=35)
            result['probe_index_absent'] = pg(database, f"SELECT to_regclass('public.{index}') IS NULL") == 't'
        except Exception as error:
            result['cleanup_error'] = f'{type(error).__name__}: {error}'
            raise
        finally:
            result['ended_epoch'] = time.time()
            path.write_text(json.dumps(result, indent=2) + '\n')
    if not result['probe_index_absent']:
        raise RuntimeError('Temporary probe index was not removed')
    result['reverted'] = measure(database, query)
    result['ended_epoch'] = time.time()
    path.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(rows=rows, before_ms=result['before']['median_ms'],
                          after_ms=result['after']['median_ms'],
                          reverted_ms=result['reverted']['median_ms'],
                          index_bytes=result['index_bytes'],
                          probe_index_absent=result['probe_index_absent'])), flush=True)


if __name__ == '__main__':
    sizes = [int(sys.argv[1])] if len(sys.argv) > 1 else [10000, 100000, 1000000]
    for size in sizes:
        probe(size)
