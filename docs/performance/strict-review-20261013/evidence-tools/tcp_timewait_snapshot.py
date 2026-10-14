#!/usr/bin/env python3
"""Read only host loopback state counts; do not retain socket endpoints."""
import argparse
from collections import Counter
from datetime import datetime,timezone
import json
from pathlib import Path
import subprocess
parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
if args.output.exists():raise SystemExit('preserve earlier snapshot')
output=subprocess.check_output(['netstat','-an','-p','tcp'],text=True)
counts=Counter();ephemeral=set()
for line in output.splitlines():
    parts=line.split()
    if len(parts)<6 or not parts[0].startswith('tcp'):continue
    local,peer,state=parts[3],parts[4],parts[-1]
    if local.startswith('127.0.0.1.') and peer.startswith('127.0.0.1.'):
        counts[state]+=1
        if state=='TIME_WAIT':
            for address in (local,peer):
                try:port=int(address.rsplit('.',1)[1])
                except ValueError:continue
                if 49152<=port<=65535:ephemeral.add(port)
result={'recorded_at_utc':datetime.now(timezone.utc).isoformat(),'measurement':'read-only host loopback TCP state aggregate; no remote endpoints retained','tcp_msl_milliseconds':int(subprocess.check_output(['sysctl','-n','net.inet.tcp.msl'],text=True)),'loopback_tcp_states':dict(counts),'unique_ephemeral_ports_in_time_wait':len(ephemeral),'ephemeral_range_size':65535-49152+1,'does_not_attribute_background_loopback_sockets_to_server':True}
args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
