#!/usr/bin/env python3
"""Summarize owned log slices without retaining SQL parameters or giant traces."""
import collections,json,pathlib,re,statistics,sys
ROOT=pathlib.Path(__file__).resolve().parents[2]
for name in sys.argv[1:]:
    path=ROOT/'docs/performance'/f'{name}.json';d=json.loads(path.read_text())
    groups=collections.defaultdict(list);transactions=collections.defaultdict(list);completed=[]
    with open('/tmp/netbaiot-capacity-postgres.log','rb') as log:
        log.seek(d['sql_log_start']);raw=log.read(d['sql_log_end']-d['sql_log_start']).decode()
    for line in raw.splitlines():
        m=re.search(r'\[(\d+)\] LOG:  duration: ([\d.]+) ms  (statement|execute \S+|bind \S+|parse \S+): (.*)',line)
        if not m:continue
        pid,ms,kind,sql=m.groups();ms=float(ms)
        if kind.startswith(('bind','parse')):continue
        sql=sql.strip()
        if sql.startswith('SELECT json_build_object'):continue
        groups[sql].append(ms)
        if sql=='BEGIN':transactions[pid]=[(sql,ms)]
        elif pid in transactions:
            transactions[pid].append((sql,ms))
            if sql in ('COMMIT','ROLLBACK'):completed.append(transactions.pop(pid))
    rows=[]
    for sql,v in groups.items():
        rows.append(dict(sql=sql,count=len(v),sum_ms=sum(v),mean_ms=statistics.mean(v),p50_ms=statistics.median(v),p95_ms=sorted(v)[int(.95*(len(v)-1))],max_ms=max(v)))
    categories=collections.defaultdict(list)
    for tx in completed:
        text=' '.join(q for q,_ in tx)
        label=('command_insert' if 'INSERT INTO commands VALUES' in text else 'command_claim_nonempty' if 'INSERT INTO command_attempts' in text else 'command_state' if 'UPDATE command_attempts' in text else 'command_ack_ingress' if 'INSERT INTO ingress_messages' in text and 'SELECT record FROM commands' in text else 'telemetry_ingress' if 'INSERT INTO ingress_messages' in text else 'command_claim_empty' if 'jsonb_to_recordset' in text else 'outbox_claim' if 'WITH selected AS' in text else 'outbox_finish' if 'UPDATE delivery_jobs SET done=$1' in text else 'maintenance' if 'DELETE FROM commands WHERE command_id IN' in text else 'other')
        categories[label].append(dict(statement_count=len(tx),server_execution_ms=sum(t for _,t in tx),statements=[q for q,_ in tx]))
    result=dict(name=name,scope='execute/simple-query counts; parse/bind excluded; BEGIN/COMMIT included; observer SELECT excluded; server duration excludes network/client scheduling; SQLx health-check Sync/Ready exchanges are not SQL statements; not physical wire RTT counts; no per-command tracing ID in PostgreSQL',queries=sorted(rows,key=lambda r:-r['sum_ms']),transactions={k:dict(count=len(v),statement_counts=dict(collections.Counter(x['statement_count'] for x in v)),server_ms=sum(x['server_execution_ms'] for x in v),example=v[0]['statements']) for k,v in categories.items()})
    out=path.with_name(name+'_sql.json');out.write_text(json.dumps(result,indent=2)+'\n');print(name,json.dumps(result['transactions']))
