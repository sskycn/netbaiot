#!/usr/bin/env python3
"""Check raw matrix completeness, bounded cleanup and invalid receipt invariants."""
import argparse
import json
from pathlib import Path


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--directory',type=Path,required=True)
    parser.add_argument('--plan',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    failures=[]; checked=[]; missing=[]; binary_sets={}; durations=0
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
            overrun=row['result']['epoch_ms']-row['workload']['start_ms']-plan['seconds']*1000
            if not -5<=overrun<=row['workload']['timeout_ms']+4000:
                failures.append([name,'driver measurement/drain deadline',overrun])
            for group,stats in row['result']['groups'].items():
                for error in ['udp_invalid_ack','protocol_error','unexpected_receipt']:
                    if stats['counters'].get(error,0): failures.append([name,group,error,stats['counters'][error]])
            if not plan.get('shutdown_at'):
                state=row.get('cooldown',{}).get('status',{})
                if state.get('active_connections')!={'http':1,'mqtt':0,'tcp':0,'udp':0}:
                    failures.append([name,'connections did not return to the single management request',state])
                if state.get('event_count')!=0 or state.get('pending_required')!=0 or state.get('event_bytes')!=0:
                    failures.append([name,'required work did not drain after cooldown',state])
                if state.get('runtime_tasks')!=5:
                    failures.append([name,'runtime tasks did not return to baseline',state.get('runtime_tasks')])
            elif not any(p['name'].endswith('.spool') for p in row.get('spool_files',[])):
                failures.append([name,'mixed shutdown did not leave required recovery spool'])
    for variant,hashes in binary_sets.items():
        if len(hashes)!=1: failures.append([variant,'multiple server binaries within matrix',sorted(hashes)])
    result=dict(checked=len(checked),measurement_seconds=durations,missing=missing,failures=failures,
                server_hashes={key:sorted(values) for key,values in binary_sets.items()},passed=not missing and not failures)
    args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
    if not result['passed']:raise SystemExit(1)


if __name__=='__main__':main()
