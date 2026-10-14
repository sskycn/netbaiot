#!/usr/bin/env python3
"""Summarize validated 10K run ranges; keep failed TCP evidence separate."""
import argparse
import csv
from datetime import datetime,timezone
import json
from pathlib import Path
parser=argparse.ArgumentParser()
parser.add_argument('--original',type=Path,required=True);parser.add_argument('--tcp-retry',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args();args.output.mkdir(exist_ok=False)
rows=[]
for directory,archive,skip_tcp in [(args.original,'density-original',True),(args.tcp_retry,'density-tcp-retry',False)]:
    records=json.loads((directory/'outcomes.json').read_text())
    for record in records:
        if skip_tcp and '-tcp-idle-' in record['case']:continue
        data=json.loads((directory/record['json']).read_text());side=json.loads((directory/record['sidecar']).read_text());c=data['load_final']['counters'];assert data['measurement_valid'] and data['connections_active']==10000 and c['connected']==10000 and c['disconnected']==10000
        assert not data['validation_errors'] and not data['load_final']['error_samples'] and not c.get('client_errors',0) and data['load_returncode']==0
        variant,rest=record['case'].split('-',1);case=rest.rsplit('-10000-',1)[0];samples=side['samples'];lat=data['load_final'].get('latencies',{})
        row={'case':case,'variant':variant,'repeat':int(record['case'].rsplit('-',1)[1]),'json':archive+'/'+record['json'],'sidecar':archive+'/'+record['sidecar'],'online':data['connections_active'],'connected':c['connected'],'disconnected':c['disconnected'],'client_errors':c.get('client_errors',0),'rss_delta_bytes_per_connection':data['rss_delta_bytes_per_connection'],'requested_heap_delta_bytes_per_connection':data['requested_heap_delta_bytes_per_connection'],'task_delta_per_connection':data['tasks_delta_per_connection'],'process_cpu_percent_ramp':side.get('process_cpu_percent_ramp'),'process_cpu_percent_stable':side.get('process_cpu_percent_stable'),'cpu_ramp_snapshot_seconds':samples[1]['monotonic_seconds']-samples[0]['monotonic_seconds'],'cpu_stable_snapshot_seconds':samples[-1]['monotonic_seconds']-samples[1]['monotonic_seconds'],'connect_p99_ms':lat.get('connect',{}).get('p99_ms'),'heartbeat_p99_ms':lat.get('ping',{}).get('p99_ms'),'pings_sent':c.get('pings_sent',0),'ping_responses':c.get('ping_responses',0),'swap_all_snapshots_used_zero':all('used = 0.00M' in x.get('host_swapusage','') for x in samples),'linux_cgroup_kernel_socket':None,'global_connection_byte_budget_balance':None,'recorded_at_utc':datetime.fromtimestamp((directory/record['json']).stat().st_mtime,timezone.utc).isoformat()}
        rows.append(row)
assert len(rows)==24
summaries=[]
def value_range(values):
    values=[v for v in values if v is not None]
    return [min(values),max(values)] if values else None
for case in dict.fromkeys(r['case'] for r in rows):
    by={variant:sorted([r for r in rows if r['case']==case and r['variant']==variant],key=lambda r:r['repeat']) for variant in ['before','after']};assert all(len(v)==2 for v in by.values())
    summary={'case':case,'variants':{}}
    for variant,runs in by.items():
        summary['variants'][variant]={'online_per_run':[r['online'] for r in runs],'completed_disconnects_per_run':[r['disconnected'] for r in runs],'errors_per_run':[r['client_errors'] for r in runs],'runs':[r['json'] for r in runs]}
        for key in ['rss_delta_bytes_per_connection','requested_heap_delta_bytes_per_connection','task_delta_per_connection','process_cpu_percent_ramp','process_cpu_percent_stable','cpu_ramp_snapshot_seconds','cpu_stable_snapshot_seconds','connect_p99_ms','heartbeat_p99_ms']:
            summary['variants'][variant][key+'_range']=value_range([r[key] for r in runs])
    summary['paired_requested_heap_delta_bytes_per_connection']=[b['requested_heap_delta_bytes_per_connection']-a['requested_heap_delta_bytes_per_connection'] for a,b in zip(by['before'],by['after'])]
    summaries.append(summary)
result={'baseline_source':'fe4770b240e628418102b9c8964ac81d3273b730','candidate_runtime_source':'b4db4b7be6f3207aac34a9610eb0ce96d4f23e96','connection_target':10000,'valid_matched_runs':len(rows),'repeats_per_variant':2,'all_runs_actual_online_and_disconnects_10000':True,'all_host_swap_snapshots_used_zero':all(r['swap_all_snapshots_used_zero'] for r in rows),'cpu_ramp_snapshot_seconds_range':value_range([r['cpu_ramp_snapshot_seconds'] for r in rows]),'cpu_stable_snapshot_seconds_range':value_range([r['cpu_stable_snapshot_seconds'] for r in rows]),'excluded_original_tcp_runs':['density-original/before-tcp-idle-10000-1.json','density-original/after-tcp-idle-10000-1.json'],'notes':['Invalid original candidate established only 6332 and is never normalized as 10K.','TCP comparison is entirely the four runs using identical 35-second natural cooldown.','CPU percentages derive from ps cumulative process CPU across RSS snapshot intervals, not full 20-second hold.','Host-wide swap is not per-process swap; ps CPU has finite time resolution.','Linux cgroup/kernel socket memory and private quota balance remain null.'],'summaries':summaries,'runs':rows}
(args.output/'density-summary.json').write_text(json.dumps(result,indent=2)+'\n')
with (args.output/'density-runs.csv').open('w',newline='') as stream:
    writer=csv.DictWriter(stream,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows)
print(json.dumps({'valid_matched_runs':len(rows),'summary_cases':len(summaries),'cpu_ramp_snapshot_seconds_range':result['cpu_ramp_snapshot_seconds_range'],'cpu_stable_snapshot_seconds_range':result['cpu_stable_snapshot_seconds_range']}))
