#!/usr/bin/env python3
"""Observe event_load cleanup; select transport only in its owned load configuration."""
import argparse
from contextlib import redirect_stdout
import hashlib
import io
import json
from pathlib import Path
import signal
import subprocess
import sys
import time

parser=argparse.ArgumentParser()
parser.add_argument('--repo',type=Path,required=True)
parser.add_argument('--sidecar',type=Path,required=True)
parser.add_argument('--transport',choices=('mqtt','tcp'),default='tcp')
parser.add_argument('--partial-frame',action='store_true')
args,harness_args=parser.parse_known_args()
if harness_args and harness_args[0]=='--': harness_args=harness_args[1:]
if args.sidecar.exists(): raise SystemExit('sidecar exists; preserve previous evidence')
if args.partial_frame and args.transport!='tcp': raise SystemExit('partial frame requires TCP')
sys.path.insert(0,str(args.repo/'scripts/perf'))
import event_load as harness
from connection_memory import active_connection_count
original_popen=subprocess.Popen
original_management_get=harness.management_get
original_status=harness.status
base_status=None
actual_load_config=None
cleanup_samples=[]

def popen_with_transport(command,*positional,**keywords):
    global actual_load_config
    if isinstance(command,(list,tuple)) and len(command)==2 and Path(command[0]).name=='netbaiot-loadgen':
        config_path=Path(command[1]);actual_load_config=json.loads(config_path.read_text())
        if args.transport=='tcp': actual_load_config.update(transport='tcp',qos=0,subscribe=False,subscribe_uplink=False)
        if args.partial_frame:actual_load_config.update(partial_frame=True,publish_rate=0)
        config_path.write_text(json.dumps(actual_load_config))
    return original_popen(command,*positional,**keywords)

def record_status(port,tls):
    global base_status
    state=original_status(port,tls)
    if base_status is None:base_status=state
    return state

def observe_final_metrics(port,path,tls=False):
    if path=='/api/v1/metrics':
        deadline=time.monotonic()+5
        while True:
            state=harness.status(port,tls)
            cleanup_samples.append({'monotonic_seconds':time.monotonic(),'online':active_connection_count(state),'status':state})
            if (active_connection_count(state)==0 and state.get('pending_required')==0
                    and state.get('runtime_tasks')==base_status.get('runtime_tasks')):break
            if time.monotonic()>=deadline:break
            time.sleep(.05)
    return original_management_get(port,path,tls)

def interrupted(signum,frame): raise SystemExit(128+signum)
signal.signal(signal.SIGTERM,interrupted)
subprocess.Popen=popen_with_transport
harness.management_get=observe_final_metrics
harness.status=record_status
sys.argv=[str(args.repo/'scripts/perf/event_load.py'),*harness_args]
output=io.StringIO()
try:
    with redirect_stdout(output): harness.main()
finally:
    subprocess.Popen=original_popen;harness.management_get=original_management_get;harness.status=original_status
    raw=output.getvalue()
    if len(raw.encode())>1024*1024: raise SystemExit('bounded event harness output exceeds 1 MiB')
    print(raw,end='')
    result={'measurement':'Unchanged event_load server/webhook lifecycle; owned transport selection and post-load diagnostic reads',
            'transport':args.transport,'intentional_partial_frame_close':args.partial_frame,
            'actual_loadgen_configuration':actual_load_config,'base_status':base_status,'cleanup_samples':cleanup_samples,
            'harness_sha256':hashlib.sha256((args.repo/'scripts/perf/event_load.py').read_bytes()).hexdigest(),
            'notes':['Only the owned temporary loadgen configuration selects TCP and intentional partial frames.',
                     'Server resource limits and webhook ACK policy are unchanged.',
                     'Post-load status polling is bounded to five seconds and occurs before shutdown, outside measured publish duration.',
                     'TCP accepted/application_ack counters are EventAccepted receipts, not business sink execution.',
                     'Intentional partial-frame disconnects are not successful publishes.']}
    args.sidecar.write_text(json.dumps(result,indent=2)+'\n')
