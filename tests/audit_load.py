#!/usr/bin/env python3
"""Bounded local load and Paho interoperability audit, not production capacity.
Run: /tmp/netbaiot-audit-venv/bin/python tests/audit_load.py
Paho 2.1.0 is test-only. Every socket/process is owned and stopped.
"""
import contextlib
import copy
import http.client
import http.server
import threading
import json
import os
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import tempfile
import time
import uuid
from smoke_postgres import ROOT, SECRET, ADMIN, free_port, packet, text, read_packet, request


def credential(i):
    c = copy.deepcopy(json.loads((ROOT / 'configs/development.json').read_text())['credentials'][0])
    c['credential_id'] = f'a{i}'
    c['identity']['device_key'] = dict(tenant_id=f't{i//8}', product_id='p', device_id=f'd{i}')
    return c


@contextlib.contextmanager
def server(limits=None, count=1, delivery_url=None):
    with tempfile.TemporaryDirectory(prefix='netbaiot-audit-load-') as directory:
        config = json.loads((ROOT / 'configs/development.json').read_text())
        reservations = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM if k == 'udp' else socket.SOCK_STREAM) for k in ['http', 'mqtt', 'tcp', 'udp']]
        for sock in reservations:
            sock.bind(('127.0.0.1', 0))
        ports = {k: sock.getsockname()[1] for k,sock in zip(['http','mqtt','tcp','udp'], reservations)}
        config.update({k: f'127.0.0.1:{v}' for k,v in ports.items()})
        if delivery_url:
            config.update(development=False, delivery_url=delivery_url)
        config['credentials'] = [credential(i) for i in range(count)]
        config['limits'] = dict(requests_per_second=4000, requests_per_ip_second=2000, messages_per_device_second=500, messages_per_tenant_second=1000)
        config['limits'].update(limits or {})
        path = Path(directory) / 'config.json'
        path.write_text(json.dumps(config))
        with (Path(directory)/'server.log').open('w+') as log:
            for sock in reservations:
                sock.close()
            proc = subprocess.Popen([str(ROOT/'target/debug/netbaiot-server'), str(path)], stdout=log, stderr=log,
                env=dict(os.environ, NETBAIOT_ADMIN_SECRET=ADMIN, RUST_LOG='warn', NO_PROXY='127.0.0.1,localhost', no_proxy='127.0.0.1,localhost'))
            try:
                for _ in range(100):
                    if proc.poll() is not None:
                        log.seek(0)
                        raise AssertionError(str(ports)+'\n'+log.read())
                    try:
                        with socket.create_connection(('127.0.0.1', ports['http']), timeout=.1):
                            break
                    except OSError:
                        time.sleep(.02)
                else:
                    raise AssertionError('startup timeout')
                yield proc, ports, config
            finally:
                if proc.poll() is None:
                    proc.send_signal(signal.SIGTERM)
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=2)
                assert proc.returncode == 0, 'server shutdown failed'


def rss(proc):
    return int(subprocess.check_output(['ps', '-o', 'rss=', '-p', str(proc.pid)]))


def metrics(port):
    c = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
    try:
        c.request('GET', '/metrics', headers={'Authorization': 'Bearer a0:'+SECRET})
        r=c.getresponse()
        assert r.status == 200
        return {line.rsplit(' ',1)[0]: int(line.rsplit(' ',1)[1]) for line in r.read().decode().splitlines()}
    finally:
        c.close()


def raw_client(ports, i, subscribe=True):
    s = socket.create_connection(('127.0.0.1', ports['mqtt']), timeout=2)
    s.sendall(packet(0x10, text('MQTT')+b'\x04\xc2\x00\x1e'+text(f'a{i}')+text(f'a{i}')+text(SECRET)))
    assert read_packet(s) == (0x20,b'\x00\x00')
    if subscribe:
        prefix=f'v1/t/t{i//8}/p/p/d/d{i}/'
        s.sendall(packet(0x82, b'\x00\x01'+text(prefix+'down')+b'\x01'+text(prefix+'up_ack')+b'\x00'))
        assert read_packet(s) == (0x90,b'\x00\x01\x01\x00')
    return s


def pressure():
    samples=[]
    with server(dict(max_connections=64,max_connections_per_ip=64,max_connections_per_tenant=16),64) as (proc,ports,_):
        clients=[]
        try:
            baseline=rss(proc)
            for target in [8,16,32,63,64]:
                latencies=[]
                while len(clients)<target:
                    start=time.perf_counter_ns()
                    clients.append(raw_client(ports,len(clients)))
                    latencies.append((time.perf_counter_ns()-start)/1e6)
                item=dict(clients=target,subscriptions=2*target,rss_kib=rss(proc),connect_subscribe_p50_ms=round(statistics.median(latencies),3),connect_subscribe_max_ms=round(max(latencies),3))
                if target<64:
                    m=metrics(ports['http'])
                    assert m['netbaiot_active_connections{transport="mqtt"}']==target
                    item['connection_owner_tasks']=target
                samples.append(item)
            rejected=0
            for _ in range(16):
                with socket.create_connection(('127.0.0.1',ports['mqtt']),timeout=2) as extra:
                    try:
                        extra.sendall(packet(0x10,text('MQTT')+b'\x04\xc2\x00\x1e'+text('a0')+text('a0')+text(SECRET)))
                        data=extra.recv(1)
                        assert data==b''
                    except (ConnectionResetError,BrokenPipeError):
                        pass
                    rejected+=1
            samples[-1]['rejected_extra']=rejected
        finally:
            for c in clients:
                c.close()
        for _ in range(100):
            m=metrics(ports['http'])
            if m['netbaiot_active_connections{transport="mqtt"}']==0:
                break
            time.sleep(.01)
        assert m['netbaiot_active_connections{transport="mqtt"}']==0
        return dict(base_rss_kib=baseline,after_close_rss_kib=rss(proc),stages=samples,active_after_close=0)


def slow_consumer():
    with server(dict(max_outbound_messages_per_connection=4,max_outbound_bytes_per_connection=4096,idle_timeout_ms=2000)) as (proc,ports,config):
        with raw_client(ports,0) as client:
            initial=rss(proc)
            for _ in range(12):
                command=dict(command_id=str(uuid.uuid4()),device=config['credentials'][0]['identity']['device_key'],expires_at=int(time.time()*1000)+60000,payload=dict(name='load',arguments={'padding':'x'*256}))
                status,_=request(ports['http'],'/v1/admin/commands',command,ADMIN)
                assert status==202
            # Read no downlink bytes and send no PUBACK. In-flight records retain permits.
            for _ in range(100):
                m=metrics(ports['http'])
                if m['netbaiot_queue_rejects_total']>0:
                    break
                time.sleep(.01)
            assert m['netbaiot_queue_depth']==4
            assert 0<m['netbaiot_queue_bytes']<=4096
            result=dict(commands=12,retained_items=m['netbaiot_queue_depth'],retained_bytes=m['netbaiot_queue_bytes'],queue_rejects=m['netbaiot_queue_rejects_total'],rss_delta_kib=rss(proc)-initial)
            for _ in range(300):
                m=metrics(ports['http'])
                if m['netbaiot_active_connections{transport="mqtt"}']==0:
                    break
                time.sleep(.01)
            assert m['netbaiot_queue_depth']==0 and m['netbaiot_queue_bytes']==0
            result['after_timeout_items']=0
            return result


def interop():
    import paho.mqtt.client as mqtt
    with server() as (_,ports,config):
        state=dict(connected=[],messages=[],ping=False,sub=False,disconnected=False)
        client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='a0',clean_session=True,protocol=mqtt.MQTTv311)
        client.username_pw_set('a0',SECRET)
        client.on_connect=lambda c,u,f,r,p: state['connected'].append(r.value)
        def received(c,u,m):
            assert len(state['messages'])<16
            state['messages'].append((m.topic,json.loads(m.payload)))
        client.on_message=received
        client.on_subscribe=lambda c,u,m,r,p: state.update(sub=all(x.value==1 for x in r))
        client.on_disconnect=lambda c,u,f,r,p: state.update(disconnected=True)
        client.on_log=lambda c,u,l,b: state.update(ping=state['ping'] or 'Received PINGRESP' in b)
        def spin(predicate,seconds=5):
            until=time.monotonic()+seconds
            while not predicate():
                assert time.monotonic()<until, state
                client.loop(timeout=.01)
        try:
            client.connect('127.0.0.1',ports['mqtt'],keepalive=2)
            spin(lambda: bool(state['connected']))
            assert state['connected']==[0]
            prefix='v1/t/t0/p/p/d/d0/'
            client.subscribe([(prefix+'down',1),(prefix+'up_ack',1)])
            spin(lambda:state['sub'])
            for qos in [0,1]:
                body=dict(schema_version=1,source_message_id=f'interop-{qos}',kind='heartbeat',data={'sequence':qos})
                info=client.publish(prefix+'up',json.dumps(body),qos=qos)
                spin(lambda:info.is_published() and len(state['messages'])>=qos+1)
            command=dict(command_id=str(uuid.uuid4()),device=config['credentials'][0]['identity']['device_key'],expires_at=int(time.time()*1000)+60000,payload=dict(name='interop',arguments={}))
            assert request(ports['http'],'/v1/admin/commands',command,ADMIN)[0]==202
            spin(lambda:any(v.get('command_id')==command['command_id'] for _,v in state['messages']))
            spin(lambda:metrics(ports['http'])['netbaiot_command_received_total']==1)
            spin(lambda:state['ping'])
            client.disconnect()
            spin(lambda:state['disconnected'])
            state['disconnected']=False
            client.reconnect()
            spin(lambda:len(state['connected'])==2)
            client.disconnect()
            spin(lambda:state['disconnected'])
            bad=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='a0',clean_session=True,protocol=mqtt.MQTTv311)
            bad.username_pw_set('a0','00'*32)
            reasons=[]
            bad.on_connect=lambda c,u,f,r,p: reasons.append(r.value)
            bad.connect('127.0.0.1',ports['mqtt'])
            deadline=time.monotonic()+3
            while not reasons and time.monotonic()<deadline:
                bad.loop(timeout=.01)
            bad.disconnect()
            assert reasons==[134], reasons  # MQTT3 return 4 mapped to Paho's Bad username/password reason.
            return dict(client='paho-mqtt 2.1.0',connect=True,qos0=True,qos1=True,exact_subscriptions=True,downlink_puback=True,ping=True,reconnect=True,bad_credentials=True)
        finally:
            client.disconnect()



def slow_delivery():
    class SlowSink(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            size=int(self.headers['Content-Length'])
            assert size<=65536
            self.rfile.read(size)
            time.sleep(1.0)
            try:
                self.send_response(204)
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                pass
        def log_message(self,*args):
            pass
    assert os.environ.get('DATABASE_URL'), 'set DATABASE_URL to a fresh disposable database'
    sink=http.server.HTTPServer(('127.0.0.1',0),SlowSink)
    thread=threading.Thread(target=sink.serve_forever)
    thread.start()
    try:
        with server(dict(max_stored_messages_per_device=8,external_timeout_ms=500,lease_ms=2000,max_attempts=2,retry_base_ms=10,retry_max_ms=20,worker_poll_interval_ms=20),delivery_url=f'http://127.0.0.1:{sink.server_port}') as (proc,ports,_):
            baseline=rss(proc)
            statuses=[]
            for i in range(12):
                body=dict(schema_version=1,source_message_id=f'slow-{i}',kind='heartbeat',data={'sequence':i})
                statuses.append(request(ports['http'],'/v1/device/messages',body,'a0:'+SECRET)[0])
            assert statuses.count(202)==8 and statuses.count(429)==4,statuses
            for _ in range(2000):
                m=metrics(ports['http'])
                if m['netbaiot_delivery_failed_total']==16:
                    break
                time.sleep(.01)
            assert m['netbaiot_delivery_failed_total']==16 and m['netbaiot_delivery_success_total']==0
            env=dict(os.environ,DYLD_LIBRARY_PATH='/opt/local/lib/icu/lib:/opt/local/lib/pgsql/lib')
            result=subprocess.check_output(['/opt/local/lib/pgsql/bin/psql',os.environ['DATABASE_URL'],'-Atc','SELECT count(*),sum(attempts),count(*) FILTER (WHERE done) FROM delivery_jobs'],env=env,text=True).strip()
            assert result=='8|16|8',result
            return dict(accepted=8,rejected=4,terminal_jobs=8,total_attempts=16,rss_delta_kib=rss(proc)-baseline,in_memory_delivery_items=1)
    finally:
        sink.shutdown()
        sink.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()


if __name__=='__main__':
    results=dict(interop=interop(),connection_pressure=pressure(),slow_consumer=slow_consumer())
    if os.environ.get('DATABASE_URL'):
        results['slow_delivery']=slow_delivery()
    print(json.dumps(results,indent=2))
