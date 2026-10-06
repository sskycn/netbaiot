#!/usr/bin/env python3
"""Summarize serial hotspot CSV records; retain raw records separately."""
import argparse
import json
import re
import statistics
from pathlib import Path

FIELDS=['throughput_ops_s','p50_ns','p95_ns','p99_ns','allocations_per_op','allocated_bytes_per_op','mutex_wait_ns','mutex_hold_ns']

def records(path):
    groups={}
    for line in Path(path).read_text().splitlines():
        match=re.search(r'(RUNTIME_HOTSPOT|MQTT_ALLOC),([^,]+),(\d+),(\d+),(.+)$',line)
        if not match: continue
        values=[float(v) for v in match[5].split(',')]
        key=f'{match[2]}/{match[3]}'
        groups.setdefault(key,[]).append(dict(zip(FIELDS,values)))
    summaries={key:{field:statistics.median(row[field] for row in rows) for field in rows[0]} for key,rows in groups.items()}
    for line in Path(path).read_text().splitlines():
        if line.startswith('EVENT_QUEUE_LOCK,'):
            _,name,size,count,wait,hold=line.split(',')
            summaries[f'{name}/{size}'].update(mutex_wait_ns=float(wait),mutex_hold_ns=float(hold))
    return summaries

def delta(before,after):
    return {key:None if before[key]==0 else 100*(after[key]/before[key]-1) for key in before.keys()&after.keys()}

def e2e(paths):
    rows=[]
    for path in paths:
        d=json.loads(Path(path).read_text());stats=d['load']['stats']
        row={'file':str(path),'throughput_ops_s':stats['counters'].get('pubacks',0)/d['duration_seconds']}
        row.update({f'{k}_ms':stats['latencies']['puback'][f'{k}_ms'] for k in ['p50','p95','p99']})
        row['rss_peak_kib']=max(v['rss_kib'] for v in d['samples'])
        row['pending_at_disconnect']=stats['counters'].get('pending_at_disconnect',0)
        row['errors']=stats['error_samples'];row['server_exit']=d['server_exit']
        metrics={}
        for line in d['metrics'].splitlines():
            name,_,value=line.partition(' ')
            if '{' not in name:
                try:metrics[name]=float(value)
                except ValueError:pass
        for lock in ['admission_lock','event_bus_lock','event_bus_state','broker_lock']:
            for kind in ['wait','hold']:
                name=f'netbaiot_{lock}_{kind}_us'
                count=metrics.get(name+'_count',0)
                row[f'{lock}_{kind}_mean_us']=metrics[name+'_sum']/count if count else None
        rows.append(row)
    summary={}
    for key in rows[0]:
        if isinstance(rows[0][key],(int,float)) or rows[0][key] is None:
            values=[row[key] for row in rows if row[key] is not None]
            summary[key]=statistics.median(values) if values else None
    return {'median':summary,'runs':rows}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before',required=True);parser.add_argument('--after',required=True)
    parser.add_argument('--e2e',action='store_true');args=parser.parse_args()
    if args.e2e:
        import glob
        before=e2e(sorted(glob.glob(args.before)));after=e2e(sorted(glob.glob(args.after)))
        keys=before['median'].keys() & after['median'].keys()
        changes={k:None if before['median'][k] in (None,0) or after['median'][k] is None else 100*(after['median'][k]/before['median'][k]-1) for k in keys}
        output={'before':before,'after':after,'delta_percent':changes}
    else:
        before=records(args.before);after=records(args.after)
        if before.keys()!=after.keys():raise ValueError('benchmark case sets differ')
        output={key:{'before':before[key],'after':after[key],'delta_percent':delta(before[key],after[key])} for key in sorted(before)}
    print(json.dumps(output,ensure_ascii=False,indent=2,allow_nan=False))

if __name__=='__main__':main()
