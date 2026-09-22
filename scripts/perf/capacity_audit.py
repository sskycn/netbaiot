#!/usr/bin/env python3
"""Bounded two-host audit coordinator. Test fixtures only; no server semantics changes.

prepare writes a portable bundle; serve owns Host A processes and a loopback-only
sampler; load runs on Host B (forward only the sampler port when remote). local
runs the same pair on one host and labels every result accordingly.
"""
import argparse
import fcntl
import hashlib
import http.client
import http.server
import json
import os
import pathlib
import platform
import pty
import threading
import select
import signal
import socket
import ssl
import statistics
import subprocess
import sys
import time
import uuid

from connection_memory import credential

ROOT = pathlib.Path(__file__).resolve().parents[2]
ADMIN = "ab" * 32  # Isolated benchmark fixture, never a production credential.
MAX_RESPONSE = 1024 * 1024


def command(argv, timeout=5):
    try:
        p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                           universal_newlines=True, timeout=timeout)
        return p.stdout.strip() if p.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def digest(path):
    result = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(65536), b""):
            result.update(block)
    return result.hexdigest()


def inventory(interface):
    return {"hostname": socket.gethostname(), "platform": platform.platform(),
            "kernel": platform.release(), "machine": platform.machine(),
            "logical_cpus": os.cpu_count(), "interface": interface,
            "cpu": command(["sysctl", "hw.model", "machdep.cpu.brand_string", "hw.physicalcpu",
                            "hw.logicalcpu", "hw.perflevel0.physicalcpu", "hw.perflevel1.physicalcpu", "hw.memsize"])
                   if sys.platform == "darwin" else command(["lscpu"]),
            "network": command(["ifconfig", interface]) if sys.platform == "darwin" else command(["ip", "address", "show", interface]),
            "rust": command(["rustc", "--version"]), "cargo": command(["cargo", "--version"]),
            "rustflags": os.environ.get("RUSTFLAGS", ""), "profile": "release", "features": "workspace defaults",
            "source_sha": command(["git", "-C", str(ROOT), "rev-parse", "HEAD"]),
            "source_status": command(["git", "-C", str(ROOT), "status", "--porcelain"])}


def process_sample(pid, fds=True):
    raw = command(["ps", "-o", "time=,rss=,%cpu=", "-p", str(pid)])
    if not raw:
        return None
    cpu, rss, percent = raw.split()
    days = 0
    if "-" in cpu:
        day, cpu = cpu.split("-", 1)
        days = int(day)
    seconds = 0.0
    for field in cpu.split(":"):
        seconds = seconds * 60 + float(field)
    result = {"pid": pid, "cpu_seconds": days * 86400 + seconds,
              "rss_kib": int(rss), "cpu_percent_ps": float(percent)}
    if sys.platform.startswith("linux"):
        try:
            result["fds"] = len(list(pathlib.Path('/proc/%s/fd' % pid).iterdir()))
            result["threads"] = len(list(pathlib.Path('/proc/%s/task' % pid).iterdir()))
        except OSError:
            pass
    else:
        if fds:
            rows = command(["lsof", "-n", "-P", "-p", str(pid), "-F", "f"])
            result["fds"] = sum(line[1:].isdigit() for line in rows.splitlines() if line.startswith("f")) if rows else None
        threads = command(["ps", "-M", "-p", str(pid)])
        result["threads"] = max(0, len(threads.splitlines()) - 1) if threads else None
    return result


def interface_sample(interface):
    if sys.platform.startswith("linux"):
        base = pathlib.Path('/sys/class/net') / interface / 'statistics'
        try:
            return {key: int((base / key).read_text()) for key in
                    ('rx_bytes', 'tx_bytes', 'rx_packets', 'tx_packets', 'rx_errors', 'tx_errors')}
        except OSError:
            return None
    raw = command(["netstat", "-ibn", "-I", interface])
    if not raw:
        return None
    rows = raw.splitlines()
    for row in rows[1:]:
        values = row.split()
        if len(values) >= 10 and values[0] == interface and values[2].startswith('<Link'):
            return dict(zip(('rx_packets', 'rx_errors', 'rx_bytes', 'tx_packets', 'tx_errors', 'tx_bytes'),
                            (int(v) for v in values[-7:-1])))
    return None


def network_recorder(pid, output):
    """Optional macOS byte counters. A PTY prevents nettop buffering short runs."""
    if sys.platform != 'darwin':
        return None
    master, slave = pty.openpty()
    try:
        child = subprocess.Popen(['nettop', '-P', '-L', '20000', '-x', '-n', '-p', str(pid),
                                  '-J', 'bytes_in,bytes_out', '-s', '1'],
                                 stdout=slave, stderr=subprocess.DEVNULL)
    except OSError:
        os.close(master)
        return None
    finally:
        os.close(slave)
    def copy():
        try:
            with pathlib.Path(output).open('w', buffering=1) as handle:
                handle.write('epoch,process,bytes_in,bytes_out\n')
                buffered=b''
                while True:
                    block = os.read(master, 4096)
                    if not block:
                        break
                    buffered += block
                    if len(buffered)>65536:
                        break
                    while b'\n' in buffered:
                        line,buffered=buffered.split(b'\n',1)
                        fields=line.decode(errors='replace').strip().strip(',').split(',')
                        if len(fields)==3 and fields[1].isdigit() and fields[2].isdigit():
                            handle.write(str(time.time())+','+','.join(fields)+'\n')
        except OSError:
            pass
        finally:
            os.close(master)
    worker = threading.Thread(target=copy, daemon=True)
    worker.start()
    return child, worker


def stop_recorder(recorder):
    if recorder is not None:
        child, worker = recorder
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=5)
        worker.join(timeout=5)


def get_json(port, path='/sample'):
    return json.loads(get_http('127.0.0.1', port, path))


def get_http(host, port, path, ca=None):
    context = ssl.create_default_context(cafile=ca) if ca else None
    connection = (http.client.HTTPSConnection(host, port, timeout=3, context=context)
                  if ca else http.client.HTTPConnection(host, port, timeout=3))
    try:
        connection.request('GET', path, headers={'Authorization': 'Bearer ' + ADMIN})
        response = connection.getresponse()
        body = response.read(MAX_RESPONSE + 1)
        if response.status != 200 or len(body) > MAX_RESPONSE:
            raise RuntimeError('bounded management/sampler request failed: %s' % response.status)
        return body.decode()
    finally:
        connection.close()


def prepare(args):
    root = pathlib.Path(args.bundle).resolve()
    root.mkdir(parents=True, exist_ok=True)
    if not 1 <= args.connections <= 10000 or not 0 <= args.rate <= 1000000:
        raise ValueError('connection/rate bound exceeded')
    if not 1 <= args.base_port <= 65525 or not 0 < args.duration <= 14400:
        raise ValueError('port/duration bound exceeded')
    if not 1 <= args.window <= 32 or not 0 <= args.payload <= 65000:
        raise ValueError('inflight/payload bound exceeded')
    if not 0 <= args.warmup <= 3600 or not 0 <= args.cooldown <= 3600:
        raise ValueError('warmup/cooldown bound exceeded')
    maximum = max(128, args.connections + 32)
    if not 65536 <= args.connection_reservation <= 2**32-1 or maximum*args.connection_reservation > 2**32-1:
        raise ValueError('connection logical budget exceeds runtime u32 bound; choose an explicit valid reservation for this diagnostic')
    limits = dict(max_connections=maximum, max_connections_per_ip=maximum,
                  max_connections_per_tenant=maximum, max_devices=maximum,
                  max_devices_per_tenant=maximum, max_persistent_sessions=maximum,
                  max_persistent_sessions_per_tenant=maximum, auth_cache_max_entries=maximum,
                  auth_cache_max_bytes=64*1024*1024, rate_entries=maximum,
                  requests_per_second=1000000, requests_per_ip_second=1000000,
                  messages_per_device_second=1000000, messages_per_tenant_second=1000000,
                  global_connection_logical_bytes=maximum*args.connection_reservation,
                  connection_memory_reservation=args.connection_reservation, max_ingress=maximum,
                  max_ingress_per_tenant=maximum, max_ingress_per_device=4,
                  sink_queue_max_count=50000, sink_queue_max_bytes=64*1024*1024,
                  global_event_max_count=50000, global_event_max_bytes=64*1024*1024,
                  sink_delivery_concurrency=8, mqtt_recovery_max_bytes=512*1024*1024)
    base = args.base_port
    external = args.bind_host not in ('127.0.0.1', '::1', 'localhost')
    if external and (not args.tls or args.sink_mode == 'audit'):
        raise ValueError('non-loopback server requires TLS and an explicit required sink; runtime protections are preserved')
    ca = str(pathlib.Path(args.certificate).resolve()) if args.tls else None
    config = dict(device_http='127.0.0.1:%d' % base, management_http='127.0.0.1:%d' % (base+1),
                  mqtt='%s:%d' % (args.bind_host, base+2), tcp='127.0.0.1:%d' % (base+3),
                  udp='127.0.0.1:%d' % (base+4), business_tcp=('127.0.0.1:%d' % (base+5)) if args.sink_mode=='tcp' else None,
                  development=not external, limits=limits, credentials=[credential(i) for i in range(args.connections)],
                  tls=dict(certificate=ca, private_key=str(pathlib.Path(args.private_key).resolve())) if ca else None,
                  delivery_url=('http://127.0.0.1:%d/events' % (base+6)) if args.sink_mode=='webhook' else None,
                  auth_provider_url=None, spool_directory=str(root/'spool'), device_configs=[])
    load = dict(audit_open_loop=args.rate > 0, transport='mqtt', address='%s:%d' % (args.host, base+2),
                connections=args.connections, tenant_width=args.connections+1, ramp_per_sec=min(200,args.connections),
                warmup_secs=args.warmup, duration_secs=args.duration, cooldown_secs=args.cooldown,
                publish_rate=args.rate, payload_bytes=args.payload, qos=args.qos, subscribe=False,
                window=args.window, timeout_secs=5, report_every_secs=1, tls_server_name=args.tls_server_name)
    if ca: load['tls_ca'] = ca
    manifest = dict(bundle_id=uuid.uuid4().hex,base_port=base, management_ca=ca, sampler_port=base+8, interface=args.interface,
                    source_sha=command(['git','-C',str(ROOT),'rev-parse','HEAD']),
                    sink_mode=args.sink_mode, sink_delay_ms=args.sink_delay_ms,
                    label=args.label, same_host=args.action=='local' or args.placement=='same-host',
                    placement='same-host' if args.action=='local' else args.placement,duration=args.duration,
                    tcp_connect_delay=args.tcp_connect_delay,tcp_disconnect_after=args.tcp_disconnect_after,
                    tcp_reconnect_delay=args.tcp_reconnect_delay,tcp_filter_mismatch_seconds=args.tcp_filter_mismatch_seconds)
    for name, data in (('server.json',config),('load.json',load),('manifest.json',manifest),
                       ('sink-control.json',dict(delay=args.sink_delay_ms/1000,status=204))):
        path=root/name
        with path.open('w') as out: json.dump(data,out,indent=2)
        path.chmod(0o600)
    return root


def metrics(raw):
    return {key:float(value) for key,value in (line.split() for line in raw.splitlines() if line and not line.startswith('#'))}


def serve(args):
    root=pathlib.Path(args.bundle).resolve(); manifest=json.loads((root/'manifest.json').read_text())
    base=manifest['base_port']; stopping=False; children=[]; handles=[]
    def stop(*_):
        nonlocal stopping
        stopping=True
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    env=os.environ.copy();env.update(NETBAIOT_ADMIN_SECRET=ADMIN, NETBAIOT_BUSINESS_STREAM_TOKEN=ADMIN,
                                   NETBAIOT_PERF_LOCK_METRICS='1', RUST_LOG='warn', NO_PROXY='127.0.0.1,localhost')
    def spawn(argv,name):
        out=(root/(name+'.log')).open('w');handles.append(out)
        child=subprocess.Popen(argv,stdout=out,stderr=subprocess.STDOUT,env=env);children.append(child);return child
    sink=None
    if manifest['sink_mode']=='webhook':
        sink=spawn([sys.executable,str(ROOT/'scripts/perf/sink.py'),str(base+6),str(root/'sink-control.json')],'sink')
    server=spawn([str(pathlib.Path(args.server_bin).resolve()),str(root/'server.json')],'server')
    listener=None
    recorder=None
    try:
        recorder=network_recorder(server.pid,root/'server-network.csv')
        (root/'tcp-before.txt').write_text(command(['netstat','-s','-p','tcp']) or 'unavailable')
        started=time.monotonic(); deadline=started+20
        while True:
            try:
                if server.poll() is not None:
                    raise RuntimeError('owned server exited before readiness')
                get_http('localhost',base+1,'/api/v1/ready',manifest['management_ca'])
                if server.poll() is not None:
                    raise RuntimeError('owned server exited during readiness')
                break
            except (OSError,RuntimeError,http.client.HTTPException):
                if server.poll() is not None or time.monotonic()>deadline:
                    raise RuntimeError('server readiness failed; see bundle/server.log')
                time.sleep(.05)
        if manifest['sink_mode']=='tcp':
            ready_file=root/'consumer.ready'
            if ready_file.exists():ready_file.unlink()
            sink=spawn([sys.executable,str(ROOT/'scripts/perf/capacity_consumer.py'),
                '--port',str(base+5),'--ack-delay-ms',str(manifest['sink_delay_ms']),
                '--connect-delay',str(manifest['tcp_connect_delay']),
                '--disconnect-after',str(manifest['tcp_disconnect_after']),
                '--reconnect-delay',str(manifest['tcp_reconnect_delay']),
                '--filter-mismatch-seconds',str(manifest['tcp_filter_mismatch_seconds']),
                '--max-seconds',str(min(20000,args.max_seconds+60)), '--ready-file',str(ready_file)],'sink')
            if not manifest['tcp_connect_delay']:
                ready_deadline=time.monotonic()+10
                while not ready_file.exists():
                    if sink.poll() is not None or time.monotonic()>ready_deadline:
                        raise RuntimeError('TCP consumer did not become ready')
                    time.sleep(.05)
        def sample():
            started = time.monotonic()
            counters = metrics(get_http('localhost', base+1, '/api/v1/metrics', manifest['management_ca']))
            timestamp = time.monotonic()
            state = json.loads(get_http('localhost', base+1, '/api/v1/status', manifest['management_ca']))
            return dict(monotonic=timestamp, epoch=time.time(), metrics=counters, status=state,
                        metric_read_seconds=timestamp-started, process=process_sample(server.pid),
                        sink_process=process_sample(sink.pid) if sink else None,
                        observer=process_sample(os.getpid(),False), network=interface_sample(manifest['interface']))
        class Handler(http.server.BaseHTTPRequestHandler):
            def setup(self):
                super().setup()
                self.connection.settimeout(3)
            def log_message(self,*_):pass
            def do_GET(self):
                if self.path not in ('/sample','/ready'):
                    self.send_error(404);return
                try:
                    body=json.dumps(sample() if self.path=='/sample' else dict(ready=True,pid=server.pid,bundle_id=manifest.get('bundle_id'),source_sha=manifest['source_sha'],hostname=socket.gethostname())).encode()
                    self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
                except (OSError,RuntimeError):self.send_error(503)
        listener=http.server.HTTPServer(('127.0.0.1',base+8),Handler);listener.timeout=.5
        (root/'server-metadata.json').write_text(json.dumps(dict(inventory=inventory(manifest['interface']),
            server_sha256=digest(args.server_bin),pid=server.pid,logging='RUST_LOG=warn',initial=sample()),indent=2))
        print(json.dumps(dict(event='ready',pid=server.pid,sampler_port=base+8)),flush=True)
        while not stopping and server.poll() is None and time.monotonic()-started < args.max_seconds:
            listener.handle_request()
    finally:
        if listener is not None:listener.server_close()
        stop_recorder(recorder)
        (root/'tcp-after.txt').write_text(command(['netstat','-s','-p','tcp']) or 'unavailable')
        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
            try:server.wait(timeout=60)
            except subprocess.TimeoutExpired:
                raise RuntimeError('server still owns work and did not finish graceful shutdown; PID=%d' % server.pid)
        for child in children:
            if child is not server and child.poll() is None:child.terminate();child.wait(timeout=10)
        for handle in handles:handle.close()


def load(args):
    root=pathlib.Path(args.bundle).resolve();manifest=json.loads((root/'manifest.json').read_text())
    configuration=json.loads((root/'load.json').read_text());out=pathlib.Path(args.output).resolve();out.mkdir(parents=True,exist_ok=True)
    if args.profile and not manifest['same_host']:
        raise ValueError('capture remote server profiles on Host A, never a Host B PID')
    port=args.sampler_port or manifest['sampler_port']
    ready=get_json(port,'/ready')
    if ready.get('bundle_id')!=manifest.get('bundle_id'):
        raise RuntimeError('sampler belongs to a different audit bundle')
    if ready.get('source_sha')!=manifest['source_sha']:
        raise RuntimeError('server manifest source SHA differs')
    local_inventory=inventory(args.interface)
    if local_inventory['source_sha']!=manifest['source_sha']:
        raise RuntimeError('loadgen checkout SHA differs from prepared server baseline')
    if manifest.get('placement')=='separate-host' and ready.get('hostname')==local_inventory['hostname']:
        raise RuntimeError('declared separate hosts have the same hostname; verify placement')
    metadata=dict(server_ready=ready,inventory=local_inventory,loadgen_sha256=digest(args.loadgen_bin),manifest=manifest,configuration=configuration)
    (out/'loadgen-metadata.json').write_text(json.dumps(metadata,indent=2))
    stderr=(out/'loadgen.stderr').open('w'); raw=(out/'loadgen.jsonl').open('w');samples_file=(out/'samples.jsonl').open('w')
    child=subprocess.Popen([str(pathlib.Path(args.loadgen_bin).resolve()),str(root/'load.json')],stdout=subprocess.PIPE,stderr=stderr,universal_newlines=True,bufsize=1)
    recorder=network_recorder(child.pid,out/'loadgen-network.csv')
    rows=[]; boundaries={};final=None;next_sample=0;profile=None
    deadline=time.monotonic()+configuration['connections']/configuration['ramp_per_sec']+configuration['warmup_secs']+configuration['duration_secs']+configuration['cooldown_secs']+60
    phase='setup'
    try:
        while child.poll() is None or select.select([child.stdout],[],[],0)[0]:
            if time.monotonic()>deadline:raise RuntimeError('bounded audit deadline exceeded')
            ready,_,_=select.select([child.stdout],[],[],.1)
            if ready:
                line=child.stdout.readline(MAX_RESPONSE+1)
                if not line:break
                if len(line)>MAX_RESPONSE:raise RuntimeError('loadgen output record bound')
                raw.write(line);raw.flush();record=json.loads(line)
                event=record.get('event')
                if event in ('warmup_start','measurement_start','measurement_end'):
                    phase={'warmup_start':'warmup','measurement_start':'measurement','measurement_end':'cooldown'}[event]
                    received_epoch=time.time()
                    point=get_json(port);point['loadgen']=process_sample(child.pid);point['record']=record
                    if manifest['same_host']:
                        point['boundary_receipt_delay_seconds']=received_epoch-record['epoch_ms']/1000
                    boundaries[event]=point
                    if event=='measurement_start' and args.profile and manifest['same_host'] and sys.platform=='darwin':
                        profile=subprocess.Popen(['sample',str(point['process']['pid']),'10','1','-file',str(out/'server.sample.txt')],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                if event=='sample':
                    record['host_sample_phase']=phase
                if event=='final':final=record
            if time.monotonic()>=next_sample and child.poll() is None:
                row=get_json(port);row.update(loadgen=process_sample(child.pid),loadgen_network=interface_sample(args.interface),phase=phase)
                samples_file.write(json.dumps(row)+'\n');samples_file.flush();rows.append(row);next_sample=time.monotonic()+1
        child.wait(timeout=10)
        if child.returncode or final is None:raise RuntimeError('loadgen failed; see output logs')
        if configuration['audit_open_loop'] and not all(k in boundaries for k in ('measurement_start','measurement_end')):
            raise RuntimeError('missing explicit measurement boundaries')
        if boundaries:
            begin=boundaries['measurement_start'];end=boundaries['measurement_end'];seconds=end['monotonic']-begin['monotonic']
            delta={k:v-begin['metrics'].get(k,0) for k,v in end['metrics'].items()}
            counters=final['stats']['counters'];duration=configuration['duration_secs'];intended=configuration['publish_rate']
            measured=[r for r in rows if r['phase']=='measurement'] or [begin, end]
            attempted=counters.get('measurement_attempted',0)/duration
            accepted=delta['netbaiot_events_accepted_total']/seconds
            ack_key={0:'measurement_published',1:'measurement_pubacks',2:'measurement_pubcomps'}[configuration['qos']]
            cpu=end['process']['cpu_seconds']-begin['process']['cpu_seconds'];client_cpu=end['loadgen']['cpu_seconds']-begin['loadgen']['cpu_seconds']
            result=dict(label=manifest['label'],placement=manifest.get('placement','unverified'),
                intended_s=intended,attempted_s=attempted,published_s=counters.get('measurement_published',0)/duration,
                accepted_s=accepted,client_completed_s=counters.get(ack_key,0)/duration,
                server_window_seconds=seconds,server_cpu_seconds=cpu,loadgen_cpu_seconds=client_cpu,
                server_cpu_cores=cpu/seconds,loadgen_cpu_cores=client_cpu/seconds,
                events_per_server_cpu_second=delta['netbaiot_events_accepted_total']/cpu if cpu else None,
                server_rss_peak_kib=max(r['process']['rss_kib'] for r in measured),
                loadgen_rss_peak_kib=max(r['loadgen']['rss_kib'] for r in measured),
                pending_peak=max(r['status']['pending_required'] for r in measured),
                pending_end=end['status']['pending_required'],event_bytes_peak=max(r['status']['event_bytes'] for r in measured),
                counters=counters,latencies=final['stats']['latencies'],inflight_peak=final['stats'].get('inflight_peak'),
                metric_deltas=delta,network_begin=begin['network'],network_end=end['network'],
                rss_first_kib=measured[0]['process']['rss_kib'],rss_last_kib=measured[-1]['process']['rss_kib'])
            histogram_name={0:None,1:'puback',2:'pubcomp'}[configuration['qos']]
            result['measurement_checks']=dict(
                scheduled_equals_attempted_plus_missed=counters.get('measurement_scheduled_slots',0)==counters.get('measurement_attempted',0)+counters.get('measurement_schedule_missed',0),
                ack_histogram_matches_measured_completions=(final['stats']['latencies'].get(histogram_name,{}).get('count',0)==counters.get(ack_key,0)) if histogram_name else None)
            result['actual_payload_bytes']=counters.get('measurement_payload_bytes',0)/max(1,counters.get('measurement_published',0))
            span=max(1,len(measured)//3)
            first_pending=statistics.median(r['status']['pending_required'] for r in measured[:span])
            last_pending=statistics.median(r['status']['pending_required'] for r in measured[-span:])
            result['pending_first_third_median']=first_pending
            result['pending_last_third_median']=last_pending
            result['growing_backlog']=last_pending>first_pending+max(10,intended*.01)
            rejects=sum(v for k,v in delta.items() if 'reject' in k and k.endswith('_total'))
            result['result']='LOADGEN-LIMITED' if attempted < intended*.99 else ('HEALTHY' if accepted>=intended*.99 and result['client_completed_s']>=intended*.99 and not counters.get('ack_timeouts',0) and not counters.get('forced_tasks',0) and not counters.get('client_errors',0) and not counters.get('measurement_window_full',0) and not rejects and not result['growing_backlog'] else 'OVERLOAD')
            result['result_note']='Provisional point classification; HEALTHY additionally requires inspecting time-series queues/RSS/tails. This does not establish a server knee.'
        else:
            connected=[r for r in rows if r['status']['active_connections']['mqtt'] >= configuration['connections']]
            initial=json.loads((root/'server-metadata.json').read_text())['initial'] if manifest['same_host'] else None
            held_rss=statistics.median(r['process']['rss_kib'] for r in connected) if connected else None
            result=dict(label=manifest['label'],placement=manifest.get('placement','unverified'),
                idle=True,final=final,connections_requested=configuration['connections'],
                connections_observed_max=max(r['status']['active_connections']['mqtt'] for r in rows),
                idle_full_connection_samples=len(connected),server_rss_idle_median_kib=held_rss,
                initial_rss_kib=initial['process']['rss_kib'] if initial else None,
                rss_delta_kib_per_connection=(held_rss-initial['process']['rss_kib'])/configuration['connections'] if held_rss and initial else None)

        (out/'boundaries.json').write_text(json.dumps(boundaries,indent=2))
        (out/'result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
    finally:
        if child.poll() is None:child.terminate();child.wait(timeout=10)
        stop_recorder(recorder)
        if profile and profile.wait(timeout=20)!=0:
            (out/'profile-error.txt').write_text('macOS sample failed; no valid profile claim\n')
        raw.close();stderr.close();samples_file.close()


def local(args):
    lock_path=ROOT/'target/perf-audit'/('capacity-port-%d.lock' % args.base_port)
    lock_path.parent.mkdir(parents=True,exist_ok=True)
    with lock_path.open('a') as lock:
        deadline=time.monotonic()+90
        while True:
            try:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic()>=deadline:
                    raise RuntimeError('another audit owns these fixed ports')
                time.sleep(.1)
        local_owned(args)


def local_owned(args):
    root=prepare(args)
    output=pathlib.Path(args.output).resolve();output.mkdir(parents=True,exist_ok=True)
    log=(output/'supervisor.log').open('w')
    supervisor=subprocess.Popen([sys.executable,__file__,'serve',str(root),'--server-bin',args.server_bin,
                                 '--max-seconds',str(args.duration+args.warmup+args.cooldown+args.connections/min(200,args.connections)+90)],stdout=log,stderr=subprocess.STDOUT)
    try:
        deadline=time.monotonic()+30
        while True:
            try:
                ready=get_json(args.base_port+8,'/ready')
                expected=json.loads((root/'manifest.json').read_text())['bundle_id']
                if ready.get('bundle_id')!=expected:
                    raise RuntimeError('fixed sampler port belongs to another run')
                break
            except (OSError,RuntimeError,http.client.HTTPException):
                if supervisor.poll() is not None or time.monotonic()>deadline:raise RuntimeError('audit supervisor not ready')
                time.sleep(.1)
        args.sampler_port=args.base_port+8;load(args)
    finally:
        supervisor.terminate();supervisor.wait(timeout=75);log.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__);subs=parser.add_subparsers(dest='action',required=True)
    for name in ('prepare','local'):
        p=subs.add_parser(name);p.add_argument('bundle');p.add_argument('--connections',type=int,default=64)
        p.add_argument('--connection-reservation',type=int,default=524288)
        p.add_argument('--placement',choices=('same-host','separate-host','unverified'),default='unverified')
        p.add_argument('--rate',type=float,default=10000);p.add_argument('--qos',type=int,choices=(0,1,2),default=1)
        p.add_argument('--payload',type=int,default=256);p.add_argument('--window',type=int,default=32)
        p.add_argument('--duration',type=float,default=60);p.add_argument('--warmup',type=float,default=30);p.add_argument('--cooldown',type=float,default=15)
        p.add_argument('--base-port',type=int,default=24000);p.add_argument('--bind-host',default='127.0.0.1');p.add_argument('--host',default='127.0.0.1')
        p.add_argument('--tls',action='store_true');p.add_argument('--tls-server-name',default='localhost')
        p.add_argument('--certificate',default=str(ROOT/'tests/fixtures/localhost-cert.pem'));p.add_argument('--private-key',default=str(ROOT/'tests/fixtures/localhost-key.pem'))
        p.add_argument('--sink-mode',choices=('audit','webhook','tcp'),default='audit');p.add_argument('--sink-delay-ms',type=float,default=0)
        p.add_argument('--tcp-connect-delay',type=float,default=0);p.add_argument('--tcp-disconnect-after',type=float,default=0)
        p.add_argument('--tcp-reconnect-delay',type=float,default=1);p.add_argument('--tcp-filter-mismatch-seconds',type=float,default=0)
        p.add_argument('--interface',default='lo0' if sys.platform=='darwin' else 'lo');p.add_argument('--label',default='control')
        if name=='local':
            p.add_argument('--output',required=True);p.add_argument('--server-bin',default=str(ROOT/'target/release/netbaiot-server'))
            p.add_argument('--loadgen-bin',default=str(ROOT/'target/release/netbaiot-loadgen'));p.add_argument('--profile',action='store_true')
    p=subs.add_parser('serve');p.add_argument('bundle');p.add_argument('--server-bin',default=str(ROOT/'target/release/netbaiot-server'));p.add_argument('--max-seconds',type=float,default=14400)
    p=subs.add_parser('load');p.add_argument('bundle');p.add_argument('--output',required=True);p.add_argument('--loadgen-bin',default=str(ROOT/'target/release/netbaiot-loadgen'))
    p.add_argument('--sampler-port',type=int);p.add_argument('--interface',default='lo0' if sys.platform=='darwin' else 'lo');p.add_argument('--profile',action='store_true')
    args=parser.parse_args()
    if args.action=='prepare':print(prepare(args))
    else:globals()[args.action](args)


if __name__=='__main__':main()
