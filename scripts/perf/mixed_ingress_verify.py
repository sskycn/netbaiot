#!/usr/bin/env python3
"""Check raw matrix completeness, bounded cleanup and invalid receipt invariants."""
import argparse
import json
import re
from pathlib import Path


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--plan',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    failures=[]; checked=[]; missing=[]; binary_sets={}; durations=0; clock_offsets=[]
    for plan in json.loads(args.plan.read_text()):
        for repeat in range(plan.get('repeats',1)):
            name=f'{plan["variant"]}-{plan["name"]}-{repeat}'
            path=args.directory/(name+'.json')
            if not path.exists(): missing.append(name); continue
            row=json.loads(path.read_text()); checked.append(name); durations+=plan['seconds']
            binary_sets.setdefault(plan['variant'],set()).add(row['server_sha256'])
            if row['plan']!=plan or row.get('production_revision')!=plan['production_revision']:
                failures.append([name,'configuration/revision mismatch'])
            if row.get('loadgen_exit')!=0 or row.get('server_stop')!={'exit_code':0,'forced':False}:
                failures.append([name,'nonzero/forced child exit'])
            if 'result' not in row: failures.append([name,'missing final statistics']); continue
            client_samples=row.get('client_samples',[])
            if not client_samples:
                failures.append([name,'missing monotonic measurement samples'])
                continue
            last=client_samples[-1]
            # Wall time can step under NTP while Tokio Instant remains monotonic.
            # Align the nearby final timestamp with the final monotonic sample.
            offset=last['epoch_ms']-row['workload']['start_ms']-last['measurement_secs']*1000
            overrun=row['result']['epoch_ms']-row['workload']['start_ms']-plan['seconds']*1000-offset
            if 'measurement_secs' in row['result']:
                overrun=(row['result']['measurement_secs']-plan['seconds'])*1000
            if abs(offset)>5:
                clock_offsets.append(dict(name=name,wall_minus_monotonic_ms=offset))
            if not -5<=overrun<=row['workload']['timeout_ms']+4000 or last['measurement_secs']<plan['seconds']-1.1:
                failures.append([name,'driver monotonic measurement/drain deadline',overrun,last['measurement_secs']])
            for group,stats in row['result']['groups'].items():
                for error in ['udp_invalid_ack','protocol_error','unexpected_receipt']:
                    if stats['counters'].get(error,0): failures.append([name,group,error,stats['counters'][error]])
            if not plan.get('shutdown_at'):
                state=row.get('cooldown',{}).get('status',{})
                if state.get('active_connections')!={'http':1,'mqtt':0,'tcp':0,'udp':0}:
                    failures.append([name,'connections did not return to the single management request',state])
                if state.get('event_count')!=0 or state.get('pending_required')!=0 or state.get('event_bytes')!=0:
                    failures.append([name,'required work did not drain after cooldown',state])
                baseline_tasks=row.get('idle',{}).get('status',{}).get('runtime_tasks',5)
                tasks=state.get('runtime_tasks')
                if plan.get('sink'):
                    # HttpSink owns a reqwest pool: <= concurrency idle connection
                    # tasks plus one pool timer. This owner remains alive until
                    # server shutdown; it is absent from the immediate AuditSink.
                    concurrency=row['server_config']['limits'].get('sink_delivery_concurrency',8)
                    idle_fds=len(re.findall(r'^f[0-9]+$',row.get('idle_fds',''),re.MULTILINE))
                    cooldown_fds=len(re.findall(r'^f[0-9]+$',row.get('cooldown_fds',''),re.MULTILINE))
                    if tasks is None or not baseline_tasks<=tasks<=baseline_tasks+concurrency+1:
                        failures.append([name,'HTTP sink tasks exceed bounded pool ownership',tasks])
                    if not idle_fds or not cooldown_fds or cooldown_fds>idle_fds+concurrency:
                        failures.append([name,'HTTP sink descriptors exceed bounded idle pool',idle_fds,cooldown_fds])
                elif tasks!=baseline_tasks:
                    failures.append([name,'runtime tasks did not return to baseline',tasks])
            elif not any(p['name'].endswith('.spool') for p in row.get('spool_files',[])):
                failures.append([name,'mixed shutdown did not leave required recovery spool'])
    for variant,hashes in binary_sets.items():
        if len(hashes)!=1: failures.append([variant,'multiple server binaries within matrix',sorted(hashes)])
    result=dict(checked=len(checked),measurement_seconds=durations,missing=missing,failures=failures,clock_offsets=clock_offsets,
                server_hashes={key:sorted(values) for key,values in binary_sets.items()},
                cleanup_policy='Device connections and required work return to zero. Immediate sink tasks return to idle baseline; HTTP sink may retain <= sink concurrency idle connections plus one owned pool timer. Child exits must be normal.',
                passed=not missing and not failures)
    args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
    if not result['passed']:raise SystemExit(1)


if __name__=='__main__':main()
