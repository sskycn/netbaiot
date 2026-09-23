#!/usr/bin/env python3
"""Generate the reproducible serialized audit matrix; no traffic is sent here."""
import argparse
import json
from pathlib import Path
from mixed_ingress_audit import BASELINE, group


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--final-revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plans = []
    def add(name, groups, seconds=30, repeats=1, variant='baseline', **options):
        plans.append(dict(name=name, groups=groups, seconds=seconds, repeats=repeats, variant=variant,
                          binary='candidate' if variant == 'final' else 'baseline',
                          production_revision=args.final_revision if variant == 'final' else BASELINE, **options))
    def mix(total, dominant=None):
        return [group(p, total * (.25 if dominant is None else .7 if p == dominant else .1)) for p in ['http','mqtt','tcp','udp']]
    def occupancy(workers):
        return [dict(label='occupier', protocol='mqtt', workers=workers, offset=512, rate=0, mode='idle')] + [group(p,100,4,delay_secs=3) for p in ['http','mqtt','tcp','udp']]
    add('idle-final-256', occupancy(256), repeats=3)
    add('idle-final-256', occupancy(256), repeats=3, variant='final')
    for variant in ['baseline', 'final']:
        add('default-ip-occupancy-32', occupancy(32), repeats=3, variant=variant, limits={'max_connections_per_ip':32})
    for repeat in range(3):
        for p, rate in [('http',8000),('mqtt',45000),('tcp',45000),('udp',80000)]:
            for variant in (['baseline','final'] if repeat%2==0 else ['final','baseline']) if p != 'udp' else ['baseline']:
                add(f'solo-{p}-pair{repeat}', [group(p,rate)], variant=variant)
    # Key mixed profiles have full five-minute measurement windows, three repeats.
    for repeat in range(3):
        for variant in ['baseline','final'] if repeat%2==0 else ['final','baseline']:
            add(f'balanced-pair{repeat}', mix(24000), seconds=300, variant=variant)
        for dominant,total in [('mqtt',50000),('http',10000),('tcp',50000),('udp',70000)]:
            add(f'{dominant}-heavy-repeat{repeat}', mix(total,dominant), seconds=300)
    for mode,protocol,rate in [('slow_http','http',0),('slow_tcp','tcp',0),('unclassified','mqtt',0),('tls_storm','http',8000)]:
        probes=[group('mqtt',1000,8),group('tcp',1000,8),group('udp',1000,4),group('http',100,4,delay_secs=4),
                dict(label='mqtt_new', protocol='mqtt', workers=4, offset=200, rate=100, reuse=False, delay_secs=4, window=1)]
        attacker=dict(label='attacker',protocol=protocol,workers=256 if rate==0 else 128,offset=512,rate=rate,mode=mode,delay_secs=2)
        for variant in ['baseline','final'] if mode in ('slow_http','slow_tcp') else ['baseline']:
            add(mode+'-established-and-new',probes+[attacker],repeats=3,variant=variant)
    add('tls-pending-final', [dict(label='occupier',protocol='mqtt',workers=256,offset=512,rate=0,mode='tls_pending')]+[group(p,100,4,delay_secs=3) for p in ['http','mqtt','tcp','udp']],repeats=3,variant='final')
    for percent in [20,40,60,80,100,120]:
        add(f'http-cliff-{percent}',[group('http',8000*percent/100),group('mqtt',1000,8),group('tcp',1000,8),group('udp',1000,4),
                                  dict(label='mqtt_new',protocol='mqtt',workers=4,offset=200,rate=100,reuse=False,window=1)])
        add(f'udp-cliff-{percent}',[group('udp',80000*percent/100)]+[group(p,1000,8) for p in ['http','mqtt','tcp']])
        for variant in ['baseline','final']:
            add(f'occupancy-cliff-{percent}',occupancy(round(256*percent/100)),variant=variant)
    for variant in ['baseline','final']:
        add('slow-required-sink', [group(p,100,4) for p in ['http','mqtt','tcp','udp']],seconds=60,repeats=3,variant=variant,sink=True)
    add('mixed-shutdown', [group(p,100,4) for p in ['http','mqtt','tcp','udp']],seconds=30,repeats=3,variant='final',sink=True,shutdown_at=15,sink_restore_at=28)
    add('plaintext-decomposition',mix(24000),repeats=3,tls=False)
    add('http-explicit-new-connection', [group('http',8000,reuse=False)],repeats=3)
    add('stable-mixed-soak',mix(16000),seconds=900,variant='final')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(plans,indent=2)+'\n')
    print(json.dumps(dict(plan_count=len(plans),runs=sum(p['repeats'] for p in plans),measurement_minutes=sum(p['seconds']*p['repeats'] for p in plans)/60)))


if __name__ == '__main__':
    main()
