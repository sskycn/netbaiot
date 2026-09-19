#!/usr/bin/env python3
"""Serial experiments: never run two capacity generators against this host concurrently."""
import json, pathlib, sys
from run_case import run
ROOT=pathlib.Path(__file__).resolve().parents[2]
OUT=ROOT/'docs/performance'

def execute(spec):
    path=OUT/(spec['name']+'.json')
    if path.exists():
        result=json.loads(path.read_text())
        if result.get('ended_epoch'):
            print(json.dumps({'skip_existing':spec['name']}),flush=True);return result
    return run(spec,path)

def idle():
    for tls in [False,True]:
        for n in [100,1000,2000]:
            for rep in range(1,4):
                result=execute(dict(name=f'idle_{"tls" if tls else "plain"}_{n}_r{rep}',tls=tls,load=dict(connections=n,subscribe=False,publish_rate=0,ramp_per_sec=500,warmup_secs=5,duration_secs=15,cooldown_secs=0),sample_secs=2,cooldown_secs=5))
                if result.get('error'):raise RuntimeError(result['error'])
    for rate in [100,1000]:
        for tls in [False,True]:
            execute(dict(name=f'ramp_{"tls" if tls else "plain"}_{rate}',tls=tls,load=dict(connections=2000,subscribe=False,publish_rate=0,ramp_per_sec=rate,warmup_secs=5,duration_secs=15,cooldown_secs=0),sample_secs=1,cooldown_secs=5))
    execute(dict(name='reservation_ceiling',credentials=2100,load=dict(connections=2100,subscribe=False,publish_rate=0,ramp_per_sec=500,warmup_secs=3,duration_secs=15,cooldown_secs=0),sample_secs=2,cooldown_secs=3))
    execute(dict(name='config_ceiling_5000',credentials=5000,load=dict(connections=5000,subscribe=False,duration_secs=5)))

def tls_cold():
    for n in [100,1000,2000]:
        for rep in range(1,4):
            execute(dict(name=f'idle_tls_cold_{n}_r{rep}',tls=True,load=dict(connections=n,subscribe=False,publish_rate=0,ramp_per_sec=500,warmup_secs=5,duration_secs=15,cooldown_secs=0),sample_secs=2,cooldown_secs=5))
    for tls in [False,True]:
        for rep in range(1,4):
            execute(dict(name=f'idle_8k_{"tls" if tls else "plain"}_3400_r{rep}',tls=tls,limits=dict(max_mqtt_packet_size=8192,max_http_body_size=8192,max_tcp_frame_size=8192,max_command_bytes=4096,connection_memory_reservation=65536),load=dict(connections=3400,subscribe=False,publish_rate=0,ramp_per_sec=500,warmup_secs=5,duration_secs=15,cooldown_secs=0),sample_secs=2,cooldown_secs=5))
    for rate in [100,1000]:
        execute(dict(name=f'ramp_tls_cold_{rate}',tls=True,load=dict(connections=2000,subscribe=False,publish_rate=0,ramp_per_sec=rate,warmup_secs=5,duration_secs=15,cooldown_secs=0),sample_secs=1,cooldown_secs=5))

def uplink():
    for qos in [0,1]:
        for rate in [25,100,250,500,1000]:
            r=execute(dict(name=f'uplink_isolated_q{qos}_{rate}',load=dict(connections=100,publish_rate=rate,qos=qos,duration_secs=45,warmup_secs=5,ramp_per_sec=500),sample_secs=2,cooldown_secs=10))
            final=next((v for v in reversed(r.get('generator',[])) if v['event']=='final'),{})
            c=final.get('stats',{}).get('counters',{})
            # Stop above the first rejection/timeout/window/backlog knee; never chase a headline.
            if c.get('client_errors',0) or c.get('accepted',0)<c.get('published',1)*.99 or c.get('client_window_full',0)>c.get('published',1)*.01 or r.get('cooldown',{}).get('postgres',{}).get('outbox_pending',0)>0:
                print(json.dumps({'knee_candidate':r['name'],'counters':c}),flush=True);break
    for qos in [0,1]:
        for size in [128,1024,4096,65000]:
            execute(dict(name=f'payload_isolated_q{qos}_{size}',load=dict(connections=100,publish_rate=25,qos=qos,payload_bytes=size,duration_secs=30,warmup_secs=3,ramp_per_sec=500),sample_secs=2,cooldown_secs=5))
    for n in [1000,2000]:
        execute(dict(name=f'active_isolated_{n}',load=dict(connections=n,publish_rate=100,qos=1,duration_secs=45,warmup_secs=5,ramp_per_sec=500),sample_secs=2,cooldown_secs=10))

if __name__=='__main__':
    {'idle':idle,'tls_cold':tls_cold,'uplink':uplink}[sys.argv[1]]()
