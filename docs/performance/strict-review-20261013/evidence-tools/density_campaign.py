#!/usr/bin/env python3
"""Run owned, bounded, serial A/B cases with the unchanged density harness."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--repo', type=Path, required=True)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--loadgen', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--connections', type=int, default=10000)
parser.add_argument('--repeats', type=int, default=2)
args = parser.parse_args()
if not 1 <= args.connections <= 10000 or not 1 <= args.repeats <= 2:
    raise SystemExit('campaign bound is 10000 connections and two repeats')
args.output.mkdir(parents=True, exist_ok=False)
script = Path(__file__).with_name('memory_with_sidecar.py')
environment = dict(os.environ, TOKIO_WORKER_THREADS='10')
for key in ('HTTPS_PROXY','HTTP_PROXY','ALL_PROXY','https_proxy','http_proxy','all_proxy'):
    environment.pop(key, None)
cases = [
    ('mqtt311-idle', ['--transport','mqtt','--mqtt-version','4']),
    ('mqtt5-idle', ['--transport','mqtt','--mqtt-version','5']),
    ('mqtt311-tls', ['--transport','mqtt','--mqtt-version','4','--tls']),
    ('mqtt311-heartbeat', ['--transport','mqtt','--mqtt-version','4','--ping-seconds','2']),
    ('mqtt5-persistent-subscribed', ['--transport','mqtt','--mqtt-version','5','--persistent','--subscribe']),
    ('tcp-idle', ['--transport','tcp']),
]
records = []
child = None

def stop_owned(signum, frame):
    if child is not None and child.poll() is None:
        child.terminate()
        try: child.wait(timeout=15)
        except subprocess.TimeoutExpired:
            child.kill(); child.wait(timeout=5)
    raise SystemExit(128 + signum)

signal.signal(signal.SIGTERM, stop_owned)
signal.signal(signal.SIGINT, stop_owned)
meta = {'placement':'same-host loopback', 'connections':args.connections,
        'hold_seconds':20, 'ramp_per_second':500, 'server_worker_threads':10,
        'repeat_order':'before/after for even repeats; after/before for odd repeats',
        'measurement':'unchanged repository connection_memory.py plus separate macOS CPU/swap sidecar',
        'binary_sha256':{label:hashlib.sha256(path.read_bytes()).hexdigest()
                         for label,path in [('before',args.baseline),('after',args.candidate),('frozen_loadgen',args.loadgen)]}}
(args.output/'campaign.json').write_text(json.dumps(meta,indent=2)+'\n')
for name, options in cases:
    for repeat in range(args.repeats):
        order = [('before',args.baseline),('after',args.candidate)]
        if repeat % 2: order.reverse()
        for variant, server in order:
            stem = f'{variant}-{name}-{args.connections}-{repeat+1}'
            output = args.output/(stem+'.json')
            error = args.output/(stem+'-stderr.txt')
            sidecar = args.output/(stem+'-sidecar.json')
            command = [sys.executable,str(script),'--repo',str(args.repo),'--sidecar',str(sidecar),'--',
                       *options,'--connections',str(args.connections),'--density','--allocation-profile',
                       '--require-density-limits','--hold-seconds','20','--ramp-per-second','500',
                       '--server',str(server),'--loadgen',str(args.loadgen)]
            started=time.monotonic()
            with output.open('x') as stdout, error.open('x') as stderr:
                child=subprocess.Popen(command,cwd=args.repo,env=environment,stdout=stdout,stderr=stderr)
                try:
                    code=child.wait(timeout=args.connections/500+90)
                except subprocess.TimeoutExpired:
                    child.terminate()
                    try: code=child.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        child.kill();code=child.wait(timeout=5)
                    records.append({'case':stem,'exit_code':code,'owned_deadline_exceeded':True})
                    (args.output/'outcomes.json').write_text(json.dumps(records,indent=2)+'\n')
                    raise SystemExit('owned case deadline exceeded; preserve failure')
            child=None
            record={'case':stem,'exit_code':code,'elapsed_seconds':time.monotonic()-started,
                    'json':output.name,'stderr':error.name,'sidecar':sidecar.name}
            records.append(record)
            (args.output/'outcomes.json').write_text(json.dumps(records,indent=2)+'\n')
            print(json.dumps(record),flush=True)
            if code:
                raise SystemExit('density case failed; preserve failure and stop ramp escalation')
