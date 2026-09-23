#!/usr/bin/env python3
"""Read bounded live driver output; does not send traffic or affect the server."""
import json
from pathlib import Path

root=Path(__file__).resolve().parents[2]
files=list((root/'target/mixed-audit').glob('*/loadgen.jsonl'))
if not files: raise SystemExit('No audit output yet')
latest=max(files,key=lambda p:p.stat().st_mtime)
with latest.open('rb') as source:
    source.seek(max(0,latest.stat().st_size-32768))
    lines=source.read(32768).splitlines()
for line in reversed(lines):
    try:
        value=json.loads(line)
        if value.get('event') not in ('sample','final'): continue
        seconds=value.get('measurement_secs',value.get('duration_secs',0))
        print(json.dumps(dict(run=latest.parent.name,event=value['event'],seconds=seconds,
              groups={name:dict(accepted_per_second=round(stats['counters'].get('accepted',0)/seconds,1) if seconds else None,
                               p99_ms=stats['latencies'].get('acceptance',{}).get('p99_ms'),
                               counters=stats['counters']) for name,stats in value['groups'].items()})))
        break
    except (ValueError,KeyError):
        continue
