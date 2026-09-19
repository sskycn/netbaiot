#!/usr/bin/env python3
"""Final isolated PostgreSQL contracts and bounded ASan fuzz smokes (no load in parallel)."""
import argparse,json,pathlib,subprocess,sys,time
from run_case import PG,ENV,ROOT
out=ROOT/'docs/performance/validation';out.mkdir(exist_ok=True)
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--fuzz-only',action='store_true',help='Keep already-passing contracts and rerun only fuzz smoke tests')
args=parser.parse_args()
records=[];run_id=str(int(time.time()))
contracts=[
 ('contract','netbaiot-storage','semantics','postgres_transaction_and_command_contract'),
 ('pressure','netbaiot-storage','semantics','audit_postgres_concurrency_leases_pool_pressure_and_cleanup'),
 ('history','netbaiot-storage','semantics','audit_postgres_attempt_history_cannot_regress'),
 ('outbox','netbaiot-storage','semantics','audit_outbox_process_crash_recovery'),
 ('mqtt','netbaiot-transports','end_to_end','audit_postgres_process_crash_boundaries')]
for label,package,target,test in ([] if args.fuzz_only else contracts):
    database='cap_validation_'+label+'_'+run_id
    subprocess.run([PG+'/createdb',database],env=ENV,check=True,capture_output=True,timeout=30)
    command=['cargo','test','--offline','-p',package,'--test',target,test,'--','--ignored','--exact']
    started=time.time()
    with (out/(label+'_'+run_id+'.log')).open('w') as log:
        result=subprocess.run(command,cwd=ROOT,env=dict(ENV,NETBAIOT_TEST_DATABASE_URL=f"postgres://{ENV['PGUSER']}@{ENV['PGHOST']}:{ENV['PGPORT']}/{database}"),stdout=log,stderr=log,timeout=300)
    records.append(dict(name=label,command=command,database=database,exit_code=result.returncode,seconds=time.time()-started))
    print(json.dumps(records[-1]),flush=True)
subprocess.run([sys.executable,str(ROOT/'fuzz/seed_corpus.py')],cwd=ROOT,check=True)
for target,length,runs in [('mqtt_fixed_header',65540,20000),('mqtt_remaining_length',8,20000),('mqtt_packet',65540,100000),('tcp_frame',65540,20000),('udp_envelope',1201,20000),('json_codec',65537,20000)]:
    command=['cargo','+nightly','fuzz','run',target,'--',f'-runs={runs}',f'-max_len={length}']
    started=time.time()
    with (out/(target+'_'+run_id+'.log')).open('w') as log:
        result=subprocess.run(command,cwd=ROOT,env=dict(ENV,CARGO_NET_OFFLINE='true'),stdout=log,stderr=log,timeout=600)
    records.append(dict(name=target,command=command,exit_code=result.returncode,seconds=time.time()-started))
    print(json.dumps(records[-1]),flush=True)
(out/('results_'+run_id+'.json')).write_text(json.dumps(records,indent=2)+'\n')
raise SystemExit(any(r['exit_code']!=0 for r in records))
