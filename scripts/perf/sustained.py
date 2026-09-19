#!/usr/bin/env python3
"""Three repetitions below the first failure; finite observation windows, not a TTL-long claim."""
import json
from extended import case
for qos in [0,1]:
    for rate in [100,50,25,10]:
        passed=True
        for rep in range(1,4):
            r=case(f'stable_observer_isolated_q{qos}_{rate}_r{rep}',duration=120,rate=rate,load=dict(qos=qos),quiesce_checkpoint=True,rust_log='warn,netbaiot_transports::common=debug')
            f=next((v.get('stats',{}) for v in reversed(r.get('generator',[])) if v['event']=='final'),{})
            c=f.get('counters',{})
            ok=not r.get('error') and not c.get('client_errors',0) and not c.get('ack_timeouts',0) and c.get('accepted',0)==c.get('published',-1) and c.get('published',0)>=rate*120*.99 and r.get('cooldown',{}).get('postgres',{}).get('outbox_pending')==0
            print(json.dumps(dict(case=r['name'],finite_window_pass=ok)),flush=True)
            if not ok:passed=False;break
        if passed:break
