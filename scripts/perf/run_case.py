#!/usr/bin/env python3
"""Run a bounded release-server experiment. Only creates cap_* disposable DBs.
Usage: python3 scripts/perf/run_case.py CASE.json OUTPUT.json
Environment: PGHOST/PGPORT/PGUSER (defaults 127.0.0.1/55432/sam).
No machine limits are changed. All owned processes/sockets have a shutdown path.
"""
import copy, hashlib, http.client, json, os, pathlib, signal, socket, ssl, subprocess, sys, tempfile, time
ROOT = pathlib.Path(__file__).resolve().parents[2]
PG = '/opt/local/lib/pgsql/bin'
ENV = dict(os.environ, DYLD_LIBRARY_PATH='/opt/local/lib/icu/lib:/opt/local/lib/pgsql/lib', PGHOST=os.environ.get('PGHOST','127.0.0.1'), PGPORT=os.environ.get('PGPORT','55432'), PGUSER=os.environ.get('PGUSER','sam'), NO_PROXY='localhost,127.0.0.1', no_proxy='localhost,127.0.0.1')
SECRET = '000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f'
ADMIN = 'ab'*32

def pg(db, sql, timeout=10):
    return subprocess.check_output([PG+'/psql','-X','-v','ON_ERROR_STOP=1','-At',db,'-c',sql],env=ENV,text=True,stderr=subprocess.PIPE,timeout=timeout).strip()

def stop(proc, seconds=8):
    started=time.monotonic();sent=False;forced=False
    if proc and proc.poll() is None:
        proc.send_signal(signal.SIGTERM);sent=True
        try: proc.wait(timeout=seconds)
        except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3);forced=True
    return dict(pid=proc.pid if proc else None,label=getattr(proc,'audit_label',None),sent_sigterm=sent,forced_kill=forced,elapsed_seconds=time.monotonic()-started,exit_code=proc.returncode if proc else None)

def cpu_time(value):
    days, _, clock=value.rpartition('-')
    parts=[float(v) for v in clock.split(':')]
    return (int(days)*86400 if days else 0)+sum(v*60**i for i,v in enumerate(reversed(parts)))

def process(pid, deep=False):
    data=subprocess.check_output(['ps','-o','rss=,vsz=,time=,pcpu=','-p',str(pid)],text=True,timeout=3).split()
    if len(data)<4: return {'gone':True}
    row=dict(pid=pid,rss_kib=int(data[0]),vsz_kib=int(data[1]),cpu_seconds=cpu_time(data[2]),ps_cpu_percent=float(data[3]))
    if deep:
        row['threads']=max(0,len(subprocess.check_output(['ps','-M','-p',str(pid)],text=True,timeout=3).splitlines())-1)
        out=subprocess.run(['lsof','-nP','-a','-p',str(pid),'-Fft'],capture_output=True,text=True,timeout=5).stdout
        row['fds']=sum(1 for l in out.splitlines() if l.startswith('f') and l[1:2].isdigit())
        row['network_sockets']=sum(1 for l in out.splitlines() if l in ('tIPv4','tIPv6'))
    return row

def metrics(port, tls_ca=None, credential_id="a0"):
    context=ssl.create_default_context(cafile=tls_ca) if tls_ca else None
    c=(http.client.HTTPSConnection('127.0.0.1',port,timeout=2,context=context) if context else http.client.HTTPConnection('127.0.0.1',port,timeout=2))
    try:
        c.request('GET','/metrics',headers={'Authorization':'Bearer '+credential_id+':'+SECRET})
        r=c.getresponse();body=r.read()
        if r.status!=200: return {'http_status':r.status}
        return {line.rsplit(' ',1)[0]:int(line.rsplit(' ',1)[1]) for line in body.decode().splitlines()}
    except (OSError, http.client.HTTPException) as e: return {'unavailable':type(e).__name__}
    finally: c.close()

DB_STATS="""SELECT json_build_object(
'rows',(SELECT count(*) FROM ingress_messages),
'outbox_pending',(SELECT count(*) FROM delivery_jobs WHERE NOT done),
'oldest_outbox_ms',(SELECT coalesce(extract(epoch from clock_timestamp())*1000-min(m.accepted_at),0)::bigint FROM delivery_jobs j JOIN ingress_messages m USING(message_id) WHERE NOT j.done),
'outbox_done',(SELECT count(*) FROM delivery_jobs WHERE done),
'outbox_attempts',(SELECT coalesce(sum(attempts),0) FROM delivery_jobs),
'commands',(SELECT count(*) FROM commands),
'commands_terminal',(SELECT count(*) FROM commands WHERE terminal),
'command_attempts',(SELECT count(*) FROM command_attempts),
'db_bytes',pg_database_size(current_database()),
'ingress_bytes',pg_total_relation_size('ingress_messages'),
'outbox_bytes',pg_total_relation_size('delivery_jobs'),
'dead_rows',(SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables),
'xact_commit',xact_commit,'xact_rollback',xact_rollback,'blks_read',blks_read,'blks_hit',blks_hit,
'blk_read_time',blk_read_time,'blk_write_time',blk_write_time,
'connections',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()),
'active',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active'),
'lock_waiters',(SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'),
'wal_lsn',pg_current_wal_lsn()::text,
'wal_bytes',(SELECT wal_bytes FROM pg_stat_wal),
'io_reads',(SELECT coalesce(sum(reads),0) FROM pg_stat_io),
'io_writes',(SELECT coalesce(sum(writes),0) FROM pg_stat_io),
'backend_pids',(SELECT coalesce(json_agg(pid),'[]'::json) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid())
) FROM pg_stat_database WHERE datname=current_database()"""

def credential(i,width):
    return dict(credential_id=f'a{i}',secret_hex=SECRET,identity=dict(device_key=dict(tenant_id=f't{i//width}',product_id='p',device_id=f'd{i}'),credential_version=1,codec_id='netbaiot-json',codec_version=1,permissions=dict(publish=True,commands=True)))

def base_limits(count,width):
    return dict(max_connections=max(256,count+64),max_connections_per_ip=max(256,count+64),max_connections_per_tenant=max(64,width*2),max_network_bytes=1073741824,
        max_devices=max(1024,count),max_devices_per_tenant=max(128,width),max_replay_entries=max(1024,count),max_replay_entries_per_tenant=max(256,width*2),
        max_subscriptions=max(512,count*2+128),max_subscriptions_per_tenant=max(128,width*2+8),
        requests_per_second=100000,requests_per_ip_second=100000,messages_per_device_second=10000,messages_per_tenant_second=100000,
        max_stored_bytes=1073741824,max_stored_bytes_per_tenant=268435456,max_stored_bytes_per_device=16777216,
        max_stored_messages=100000,max_stored_messages_per_tenant=50000,max_stored_messages_per_device=5000,
        max_pending_commands=8192,max_pending_commands_per_tenant=1024,max_pending_commands_per_device=128)

def run(spec, output):
    output=pathlib.Path(output); output.parent.mkdir(parents=True,exist_ok=True)
    name=spec['name']; assert name.replace('_','').isalnum() and len(name)<48
    load=copy.deepcopy(spec.get('load',{})); all_loads=[load]+copy.deepcopy(spec.get('extra_loads',[])); assert len(all_loads)<=8
    assert 1<=spec.get('sample_secs',2)<=60 and 0<=spec.get('cooldown_secs',5)<=120
    assert len(spec.get('events',[]))<=32
    assert 1<=spec.get('sample_seconds',3)<=15
    for settings in [spec.get('sink',{})]+[e['settings'] for e in spec.get('events',[]) if e.get('kind')=='sink']:
        assert set(settings)<={'delay','status'}, 'sink settings use delay in seconds and status; unknown keys are rejected'
        assert 0<=float(settings.get('delay',0))<=10 and 100<=int(settings.get('status',204))<=599
    durations=[sum(p['seconds'] for p in v.get('phases',[])) or v.get('duration_secs',30) for v in all_loads]
    assert all(0<d<=86400 for d in durations)
    if spec.get('rust_log') or spec.get('sql_trace'):assert max(durations)<=120, 'bound diagnostic logs'
    assert all(e.get('count',1)<=32 and e.get('seconds',1)<=120 for e in spec.get('events',[]))
    count=int(spec.get('credentials',max(v.get('connections',100)+v.get('offset',0) for v in all_loads)+1));width=load.get('tenant_width',16)
    assert 0<count<=50001
    observer_id=spec.get('observer_credential',f'a{count-1}')
    assert observer_id.startswith('a') and observer_id[1:].isdigit() and int(observer_id[1:])<count
    database='cap_'+name if spec.get('postgres',True) else None
    result=dict(name=name,baseline_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),spec=spec,database=database,started_epoch=time.time(),samples=[],events=[])
    digest=hashlib.sha256()
    with (ROOT/'target/release/netbaiot-server').open('rb') as binary:
        for chunk in iter(lambda:binary.read(1048576),b''):digest.update(chunk)
    result['server_binary_sha256']=digest.hexdigest()
    digest=hashlib.sha256()
    with (ROOT/'target/release/netbaiot-loadgen').open('rb') as binary:
        for chunk in iter(lambda:binary.read(1048576),b''):digest.update(chunk)
    result['generator_binary_sha256']=digest.hexdigest()
    tmp=tempfile.TemporaryDirectory(prefix='netbaiot-capacity-');folder=pathlib.Path(tmp.name)
    procs=[];files=[];blockers=[];server=None;generator=None;sink=None;profiles=[];server_logs=[];generators=[];db_disabled=False
    try:
        sockets=[socket.socket(socket.AF_INET,socket.SOCK_DGRAM if k=='udp' else socket.SOCK_STREAM) for k in ['http','mqtt','tcp','udp','sink']]
        for sock in sockets:sock.bind(('127.0.0.1',0))
        ports=dict(zip(['http','mqtt','tcp','udp','sink'],[s.getsockname()[1] for s in sockets]))
        config=dict(http=f"127.0.0.1:{ports['http']}",mqtt=f"127.0.0.1:{ports['mqtt']}",tcp=f"127.0.0.1:{ports['tcp']}",udp=f"127.0.0.1:{ports['udp']}",development=not bool(database),limits=base_limits(count,width),credentials=[credential(i,width) for i in range(count)],tls=None,delivery_url=f"http://127.0.0.1:{ports['sink']}/ingress")
        if spec.get('observer_tenant_isolation',True) and observer_id==f'a{count-1}' and count>max(v.get('connections',100)+v.get('offset',0) for v in all_loads):
            config['credentials'][-1]['identity']['device_key']['tenant_id']='audit-observer'
            result['metrics_observer_isolated_tenant']=True
        config['limits'].update(spec.get('limits',{}))
        if spec.get('tls'):
            cert,key=folder/'cert.pem',folder/'key.pem'
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(key),'-out',str(cert),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1'],check=True,capture_output=True,timeout=15)
            config['tls']=dict(certificate=str(cert),private_key=str(key));load['tls_ca']=str(cert)
        server_config=folder/'server.json';server_config.write_text(json.dumps(config,separators=(',',':')))
        result['configuration_bytes']=server_config.stat().st_size;result['limits']=config['limits'];result['credentials']=count;result['metrics_credential']=observer_id
        if database:
            subprocess.run([PG+'/createdb',database],env=ENV,check=True,capture_output=True,timeout=10)
            if spec.get('sql_trace'):
                assert max(v.get('duration_secs',30) for v in all_loads)<=120, 'limit verbose SQL tracing duration'
                pg(database,f'ALTER DATABASE {database} SET log_min_duration_statement=0; ALTER DATABASE {database} SET log_parameter_max_length=0')
        control=folder/'sink-control.json';control.write_text(json.dumps(spec.get('sink',{})))
        for sock in sockets:sock.close()
        def spawn(args,label,env=ENV):
            path=folder/(label+'.log');log=path.open('w');files.append(log)
            p=subprocess.Popen(args,stdout=log,stderr=log,env=env,cwd=ROOT);p.audit_label=label;procs.append(p);return p,path
        sink,sink_log=spawn([sys.executable,str(ROOT/'scripts/perf/sink.py'),str(ports['sink']),str(control)],'sink')
        env=dict(ENV,NETBAIOT_ADMIN_SECRET=ADMIN,RUST_LOG=spec.get('rust_log','warn'),NO_COLOR='1')
        if database:env['DATABASE_URL']=f"postgres://{ENV['PGUSER']}@{ENV['PGHOST']}:{ENV['PGPORT']}/{database}"
        server,server_log=spawn([str(ROOT/'target/release/netbaiot-server'),str(server_config)],'server',env);server_logs.append(server_log)
        for _ in range(600):
            if server.poll() is not None:raise RuntimeError('server startup: '+server_log.read_text()[-3000:])
            try:
                with socket.create_connection(('127.0.0.1',ports['http']),timeout=.1):break
            except OSError:time.sleep(.05)
        else:raise RuntimeError('server startup timed out')
        time.sleep(1)
        if database and spec.get('preload'):
            from dataset import seed_sql
            pg(database,seed_sql(int(spec['preload']),commands=False,done=True),timeout=60)
        if database and spec.get('quiesce_checkpoint',True):
            at=time.monotonic();pg('postgres','CHECKPOINT',timeout=120);result['checkpoint_before_seconds']=time.monotonic()-at
            time.sleep(2)
        result['sql_log_start']=pathlib.Path('/tmp/netbaiot-capacity-postgres.log').stat().st_size
        result['baseline']=dict(server=process(server.pid,True),metrics=metrics(ports['http'],load.get('tls_ca'),observer_id))
        if database:result['baseline']['postgres']=json.loads(pg(database,DB_STATS))
        protocol=load.get('transport','mqtt');load['address']=f"127.0.0.1:{ports[protocol if protocol!='http' else 'http']}";load['http_url']=f"{'https://127.0.0.1' if spec.get('tls') else 'http://127.0.0.1'}:{ports['http']}"
        generator_config=folder/'load.json';generator_config.write_text(json.dumps(load))
        generator,generator_log=spawn([str(ROOT/'target/release/netbaiot-loadgen'),str(generator_config)],'generator')
        generators=[('generator',generator,generator_log)]
        for index, extra in enumerate(all_loads[1:],1):
            proto=extra.get('transport','mqtt');extra['address']=f"127.0.0.1:{ports[proto]}";extra['http_url']=load['http_url']
            if load.get('tls_ca'):extra['tls_ca']=load['tls_ca']
            path=folder/f'load-{index}.json';path.write_text(json.dumps(extra))
            proc,log=spawn([str(ROOT/'target/release/netbaiot-loadgen'),str(path)],f'generator_{index}')
            generators.append((f'generator_{index}',proc,log))
        start=time.monotonic();scheduled=list(spec.get('events',[]));profiled=False;peak_vmmap_done=False;last_deep=-100;step=spec.get('sample_secs',2)
        budget=max(v.get('connections',100)/v.get('ramp_per_sec',100)+v.get('warmup_secs',3)+sum(p['seconds'] for p in v.get('phases',[]))+ (0 if v.get('phases') else v.get('duration_secs',30))+v.get('cooldown_secs',3)+20 for v in all_loads)
        step=max(step,budget/10000)  # At most 10,000 samples; no unbounded observer history.
        while any(p.poll() is None for _,p,_ in generators) and time.monotonic()-start<budget:
            elapsed=time.monotonic()-start
            while scheduled and elapsed>=scheduled[0]['at']:
                event=scheduled.pop(0);result['events'].append(dict(elapsed=elapsed,**event))
                if event['kind']=='sink':control.write_text(json.dumps(event['settings']))
                elif event['kind']=='db_lock':
                    p=subprocess.Popen([PG+'/psql','-X',database,'-c',f"BEGIN; SELECT pg_advisory_xact_lock(782634291); SELECT pg_sleep({float(event['seconds'])}); COMMIT;"],env=ENV,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);blockers.append(p)
                elif event['kind']=='db_unavailable':
                    # PostgreSQL forbids disabling the database of this session.
                    # Set the restoration flag before terminating its backends.
                    pg('postgres',f'ALTER DATABASE {database} ALLOW_CONNECTIONS false');db_disabled=True
                    pg('postgres',f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='{database}'")
                elif event['kind']=='db_available':
                    pg('postgres',f'ALTER DATABASE {database} ALLOW_CONNECTIONS true');db_disabled=False
                elif event['kind']=='restart':
                    result.setdefault('shutdowns',[]).append(stop(server,config['limits'].get('shutdown_timeout_ms',30000)/1000+2));server,server_log=spawn([str(ROOT/'target/release/netbaiot-server'),str(server_config)],'server-restarted',env);server_logs.append(server_log)
                    result.setdefault('restarts',[]).append(dict(elapsed=elapsed,pid=server.pid))
                elif event['kind']=='external_db_sleep':
                    for _ in range(int(event.get('count',8))):
                        p=subprocess.Popen([PG+'/psql','-X',database,'-c',f"SELECT pg_sleep({float(event['seconds'])});"],env=ENV,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);blockers.append(p)
            if spec.get('profile') and not profiled and elapsed>=spec.get('profile_at',10):
                profile_path=output.with_suffix('.sample.txt');log=(folder/'sample.log').open('w');files.append(log)
                profiles.append(subprocess.Popen(['sample',str(server.pid),str(spec.get('sample_seconds',3)),'1','-file',str(profile_path)],stdout=log,stderr=log));profiled=True
                result.setdefault('diagnostic_events',[]).append(dict(kind='sample_start',elapsed_s=elapsed,seconds=spec.get('sample_seconds',3)))
                if spec.get('vmmap') and spec.get('profile_vmmap_overlap',False):
                    vm=subprocess.run(['vmmap','-summary',str(server.pid)],capture_output=True,text=True,timeout=20)
                    output.with_suffix('.peak-vmmap.txt').write_text(vm.stdout+vm.stderr)
                    peak_vmmap_done=True
                    result.setdefault('diagnostic_events',[]).append(dict(kind='vmmap_overlapping_sample',elapsed_s=elapsed))
            if profiled and spec.get('vmmap') and not peak_vmmap_done and all(p.poll() is not None for p in profiles):
                # vmmap may suspend the target. Keep it out of CPU sample stacks.
                result.setdefault('diagnostic_events',[]).append(dict(kind='vmmap_after_sample',elapsed_s=elapsed))
                vm=subprocess.run(['vmmap','-summary',str(server.pid)],capture_output=True,text=True,timeout=20)
                output.with_suffix('.peak-vmmap.txt').write_text(vm.stdout+vm.stderr);peak_vmmap_done=True
            deep=elapsed-last_deep>=10
            if deep:last_deep=elapsed
            row=dict(elapsed_s=round(elapsed,3),epoch=time.time(),host_load_average=os.getloadavg())
            for label,proc in [('server',server),('sink',sink)]+[(label,proc) for label,proc,_ in generators]:
                try:row[label]=process(proc.pid,deep)
                except Exception as e:row[label]={'error':type(e).__name__,'exit':proc.poll()}
            row['metrics']=metrics(ports['http'],load.get('tls_ca'),observer_id)
            if database:
                try:
                    row['postgres']=json.loads(pg(database,DB_STATS,timeout=3))
                    pids=row['postgres'].pop('backend_pids',[]);row['postgres']['backend_processes']={str(p):process(p) for p in pids}
                except Exception as e:row['postgres']={'error':type(e).__name__}
            result['samples'].append(row)
            if server.poll() is not None:result.setdefault('server_died_at',elapsed)
            # Persist progress; an interrupted experiment remains reviewable.
            output.write_text(json.dumps(result,separators=(',',':')))
            time.sleep(step)
        for label,proc,log in generators:
            if proc.poll() is None:result[label+'_deadline']=True;stop(proc)
            result[label+'_exit']=proc.wait();lines=[]
            for line in log.read_text().splitlines():
                try:lines.append(json.loads(line))
                except ValueError:result.setdefault('generator_log_errors',[]).append(line[:300])
            result[label]=lines
        result['after_disconnect']=dict(epoch=time.time(),server=process(server.pid,True),metrics=metrics(ports['http'],load.get('tls_ca'),observer_id)) if server.poll() is None else {'server_exit':server.returncode}
        if spec.get('vmmap') and server.poll() is None:
            vm=subprocess.run(['vmmap','-summary',str(server.pid)],capture_output=True,text=True,timeout=20)
            result['vmmap_exit']=vm.returncode;output.with_suffix('.vmmap.txt').write_text(vm.stdout+vm.stderr)
        time.sleep(spec.get('cooldown_secs',5))
        result['cooldown']=dict(epoch=time.time(),server=process(server.pid,True),metrics=metrics(ports['http'],load.get('tls_ca'),observer_id)) if server.poll() is None else {'server_exit':server.returncode}
        if database:
            result['cooldown']['postgres']=json.loads(pg(database,DB_STATS))
            result['final_storage_state']=json.loads(pg(database,"SELECT json_build_object('charge_bytes',(SELECT coalesce(sum(charge),0) FROM ingress_messages),'command_delivery',(SELECT json_object_agg(state,n) FROM (SELECT record->>'delivery' state,count(*) n FROM commands GROUP BY 1) s),'command_execution',(SELECT json_object_agg(state,n) FROM (SELECT record->>'execution' state,count(*) n FROM commands GROUP BY 1) s),'attempt_states',(SELECT json_object_agg(state,n) FROM (SELECT state,count(*) n FROM command_attempts GROUP BY 1) s),'delivery_outcomes',(SELECT json_object_agg(state,n) FROM (SELECT CASE WHEN NOT done THEN 'pending' WHEN last_error IS NULL THEN 'succeeded' ELSE last_error END state,count(*) n FROM delivery_jobs GROUP BY 1) s),'maximum_delivery_attempts',(SELECT coalesce(max(attempts),0) FROM delivery_jobs))"))
        result['sql_log_end']=pathlib.Path('/tmp/netbaiot-capacity-postgres.log').stat().st_size
        log_text='\n'.join(p.read_text() for p in server_logs)
        if spec.get('rust_log'):
            import re, collections
            clean_log=re.sub(r'\x1b\[[0-9;]*m','',log_text)
            result['connection_close_reasons']=dict(collections.Counter(line.split('error=',1)[1].split('connection closed',1)[0].strip() for line in clean_log.splitlines() if 'connection closed' in line and 'error=' in line))
            result['trace_measurements']=dict(pool_acquire_seconds=[float(x) for x in re.findall(r'aquired_after_secs=([0-9.e+-]+)',log_text)],command_send_start_to_puback_us=[int(x) for x in re.findall(r'command_ack_elapsed_us=(\d+)',log_text)])
        result['server_log_tail']=log_text[-3000:];result['sink_tail']=sink_log.read_text().splitlines()[-3:]
    except KeyboardInterrupt:
        result['error']='KeyboardInterrupt: experiment cancelled'; raise
    except Exception as e:
        result['error']=type(e).__name__+': '+str(e)
    finally:
        if db_disabled:
            try:pg('postgres',f'ALTER DATABASE {database} ALLOW_CONNECTIONS true')
            except Exception as e:result['database_restore_error']=str(e)
        for proc in blockers:stop(proc,1)
        for proc in profiles:stop(proc,5)
        for proc in reversed(procs):
            grace=(spec.get('limits',{}).get('shutdown_timeout_ms',30000)/1000+2) if getattr(proc,'audit_label','').startswith('server') else 8
            result.setdefault('shutdowns',[]).append(stop(proc,grace))
        if server:result['server_exit']=server.returncode
        for log in files:log.close()
        result['ended_epoch']=time.time();output.write_text(json.dumps(result,separators=(',',':'))+'\n');tmp.cleanup()
    print(json.dumps(dict(name=name,output=str(output),error=result.get('error'),generator_exit=result.get('generator_exit'),server_exit=result.get('server_exit'))),flush=True)
    return result

if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('case');parser.add_argument('output');parser.add_argument('--name')
    args=parser.parse_args();document=json.loads(pathlib.Path(args.case).read_text());spec=copy.deepcopy(document.get('spec',document));spec.update(document.get('reproduction_overrides',{}))
    if args.name:spec['name']=args.name
    run(spec,args.output)
