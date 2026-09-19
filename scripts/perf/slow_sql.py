#!/usr/bin/env python3
"""Capture slow statement evidence from each result's owned log offsets."""
import collections,json,pathlib,re
ROOT=pathlib.Path(__file__).resolve().parents[2];OUT=ROOT/'docs/performance'
results={}
with open('/tmp/netbaiot-capacity-postgres.log','rb') as log:
 for path in sorted(OUT.glob('*.json')):
    d=json.loads(path.read_text())
    if not isinstance(d,dict) or 'sql_log_end' not in d:continue
    log.seek(d['sql_log_start']);raw=log.read(d['sql_log_end']-d['sql_log_start']).decode()
    groups=collections.defaultdict(list);checkpoints=[]
    for line in raw.splitlines():
        if 'checkpoint ' in line:checkpoints.append(line)
        m=re.search(r'duration: ([\d.]+) ms  (?:statement|execute \S+): (.*)',line)
        if not m or float(m[1])<100:continue
        sql=m[2]
        if sql.startswith('SELECT json_build_object'):continue
        groups[sql].append(float(m[1]))
    results[path.stem]=dict(scope='only statements >=100ms; cluster log slice may include background work, not total DB latency distribution',slow_queries=[dict(sql=k,count=len(v),max_ms=max(v),total_ms=sum(v)) for k,v in sorted(groups.items(),key=lambda kv:-sum(kv[1]))],checkpoints=checkpoints)
(OUT/'slow-sql.json').write_text(json.dumps(results,indent=2)+'\n')
