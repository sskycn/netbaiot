#!/usr/bin/env python3
"""Bounded serial wire-payload and partial-disconnect evidence, using event_load."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

parser=argparse.ArgumentParser()
for name in ('repo','baseline','candidate','loadgen','output'):parser.add_argument('--'+name,type=Path,required=True)
args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=False)
wrapper=Path(__file__).with_name('event_with_tcp_override.py')
environment=dict(os.environ)
for key in ('HTTPS_PROXY','HTTP_PROXY','ALL_PROXY','https_proxy','http_proxy','all_proxy'):environment.pop(key,None)
child=None;records=[]
def stop_owned(signum,frame):
    if child is not None and child.poll() is None:
        child.terminate()
        try:child.wait(timeout=15)
        except subprocess.TimeoutExpired:child.kill();child.wait(timeout=5)
    raise SystemExit(128+signum)
signal.signal(signal.SIGTERM,stop_owned);signal.signal(signal.SIGINT,stop_owned)
metadata={'source_commit':'b4db4b7be6f3207aac34a9610eb0ce96d4f23e96','connections':32,'payload_wire_bytes':65000,'rate_per_second':100,'measured_seconds':20,'warmup_seconds':2,'cooldown_seconds':2,'server_workers':10,'placement':'same-host loopback, serial','binary_sha256':{k:hashlib.sha256(v.read_bytes()).hexdigest() for k,v in [('before',args.baseline),('after',args.candidate),('frozen_loadgen',args.loadgen)]},'observer_sha256':hashlib.sha256(wrapper.read_bytes()).hexdigest(),'natural_cooldown_seconds':35,'tcp_msl_milliseconds':15000}
(args.output/'campaign.json').write_text(json.dumps(metadata,indent=2)+'\n')
def run_case(name,variant,server,transport,options,partial=False):
    global child
    stem=variant+'-'+name;out=args.output/(stem+'.json');err=args.output/(stem+'-stderr.txt');sidecar=args.output/(stem+'-sidecar.json')
    command=[sys.executable,str(wrapper),'--repo',str(args.repo),'--sidecar',str(sidecar),'--transport',transport]
    if partial:command+=['--partial-frame']
    command+=['--','--connections','16' if partial else '32','--rate','0' if partial else '100','--duration','5' if partial else '20','--warmup','1' if partial else '2','--cooldown','1' if partial else '2','--payload-bytes','65000','--qos','1','--server-workers','10','--server-bin',str(server),'--loadgen-bin',str(args.loadgen),*options]
    print(json.dumps({'state':'natural_cooldown','seconds':35,'next_case':stem}),flush=True)
    time.sleep(35)
    snapshot_script=Path(__file__).with_name('tcp_timewait_snapshot.py')
    pre_snapshot=args.output/(stem+'-timewait-before.json')
    subprocess.run([sys.executable,str(snapshot_script),'--output',str(pre_snapshot)],check=True,stdout=subprocess.DEVNULL)
    started=time.monotonic()
    with out.open('x') as stdout,err.open('x') as stderr:
        child=subprocess.Popen(command,cwd=args.repo,env=environment,stdout=stdout,stderr=stderr)
        try:code=child.wait(timeout=100)
        except subprocess.TimeoutExpired:
            child.terminate()
            try:code=child.wait(timeout=15)
            except subprocess.TimeoutExpired:child.kill();code=child.wait(timeout=5)
            records.append({'case':stem,'exit_code':code,'owned_deadline_exceeded':True});(args.output/'outcomes.json').write_text(json.dumps(records,indent=2)+'\n');raise SystemExit('owned event case deadline exceeded')
    child=None
    post_snapshot=args.output/(stem+'-timewait-after.json')
    subprocess.run([sys.executable,str(snapshot_script),'--output',str(post_snapshot)],check=True,stdout=subprocess.DEVNULL)
    result=json.loads(out.read_text());side=json.loads(sidecar.read_text())
    stats=(result.get('load') or {}).get('stats',{});counters=stats.get('counters',{});metrics=dict(re.findall(r'^(netbaiot_[a-z_]+_total) (\d+)$',result.get('metrics',''),re.M))
    online=side.get('cleanup_samples',[])[-1] if side.get('cleanup_samples') else {}
    validation=[]
    for key in ('client_errors','ack_timeouts','forced_tasks','panicked_tasks','connect_rejected'):
        if counters.get(key,0):validation.append(key+'='+str(counters[key]))
    if stats.get('error_samples'):validation.append('client error samples')
    if code!=0 or result.get('load_exit')!=0 or result.get('server_exit')!=0:validation.append('nonzero process exit')
    expected=16 if partial else 32
    if counters.get('connected')!=expected or counters.get('disconnected')!=expected:validation.append('connection population/owned disconnect mismatch')
    if online.get('online')!=0 or online.get('status',{}).get('pending_required')!=0:validation.append('online/pending cleanup incomplete')
    if online.get('status',{}).get('runtime_tasks')!=side.get('base_status',{}).get('runtime_tasks'):validation.append('task cleanup incomplete')
    published=counters.get('published',0)
    if partial:
        if published:validation.append('intentional partial frame published unexpectedly')
    else:
        if published<=0:validation.append('no publishes')
        if counters.get('pending_at_disconnect',0):validation.append('pending acknowledgements at disconnect')
        if transport=='mqtt':
            if counters.get('pubacks')!=published or counters.get('subscriber_publishes')!=published:validation.append('PUBACK/subscriber publish mismatch')
        elif counters.get('accepted')!=published:validation.append('TCP EventAccepted receipt mismatch')
        if (result.get('sink') or {}).get('received')!=published or int(metrics.get('netbaiot_sink_acks_total','-1'))!=published:validation.append('confirmed sink ACK mismatch')
    record={'case':stem,'exit_code':code,'elapsed_seconds':time.monotonic()-started,'json':out.name,'sidecar':sidecar.name,'stderr':err.name,'timewait_before':pre_snapshot.name,'timewait_after':post_snapshot.name,'intentional_partial_frame_close':partial,'valid':not validation,'validation_errors':validation,'counters':counters,'final_online':online.get('online'),'final_pending_required':online.get('status',{}).get('pending_required'),'final_runtime_tasks':online.get('status',{}).get('runtime_tasks'),'base_runtime_tasks':side.get('base_status',{}).get('runtime_tasks'),'sink_received':(result.get('sink') or {}).get('received'),'sink_acks':int(metrics.get('netbaiot_sink_acks_total','-1'))}
    records.append(record);(args.output/'outcomes.json').write_text(json.dumps(records,indent=2)+'\n');print(json.dumps(record),flush=True)
    if variant=='after' and validation:raise SystemExit('candidate event validation failed; preserve diagnosis')
for name,transport,options in [('mqtt311-large-subscription','mqtt',['--subscribe-uplink']),('mqtt5-large-subscription','mqtt',['--subscribe-uplink','--mqtt-v5']),('tcp-large-ingress','tcp',[])]:
    for variant,server in [('before',args.baseline),('after',args.candidate)]:run_case(name,variant,server,transport,options)
run_case('tcp-partial-frame-disconnect','after',args.candidate,'tcp',[],partial=True)
