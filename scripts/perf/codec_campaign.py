#!/usr/bin/env python3
"""Bounded real-server codec matrix on Linux/macOS; public test credentials only.

Closed-loop Python clients measure socket-send -> protocol receipt and socket-send
-> confirmed webhook response, separately. QoS0 has no EventAccepted receipt.
Command latency is management-submit -> device bytes, never device execution.
This shared-host regression harness does not measure production capacity.
"""
import argparse
import concurrent.futures
import hashlib
import hmac
import http.server
import importlib.util
import json
import os
from pathlib import Path
import platform
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "examples"))
from device_tcp import decode_ack
spec = importlib.util.spec_from_file_location('vectors', ROOT / 'crates/netbaiot-codecs/tests/fixtures/generate.py')
vectors = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vectors)
FORMATS = ('json', 'cbor', 'msgpack', 'protobuf')
ADMIN = 'ab' * 32
KEY = bytes(range(32))
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def encode(fmt, value):
    if fmt == 'json':
        return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
    return getattr(vectors, fmt)(value)


def text(value):
    b = value.encode()
    return struct.pack('!H', len(b)) + b


def packet(first, body):
    return bytes([first]) + vectors.varint(len(body)) + body


def exact(sock, size):
    if size > 65536:
        raise ValueError('response limit')
    result = bytearray()
    while len(result) < size:
        block = sock.recv(size - len(result))
        if not block:
            raise EOFError('connection closed')
        result.extend(block)
    return bytes(result)


def mqtt_read(sock):
    first = exact(sock, 1)[0]
    length = 0
    for i in range(4):
        b = exact(sock, 1)[0]
        length |= (b & 127) << (7 * i)
        if b < 128:
            return first, exact(sock, length)
    raise ValueError('remaining length')


def frame(body):
    return struct.pack('!I', len(body)) + body


def tcp_read(sock):
    return exact(sock, struct.unpack('!I', exact(sock, 4))[0])


def management(address, path, value=None):
    req = urllib.request.Request(f'http://{address}/api/v1/{path}',
        data=None if value is None else json.dumps(value).encode(),
        headers={'Authorization': 'Bearer ' + ADMIN, 'Content-Type': 'application/json'})
    with OPENER.open(req, timeout=3) as response:
        data = response.read(1048577)
        if len(data) > 1048576:
            raise ValueError('management response limit')
        return data


def quantiles(samples):
    if not samples:
        return None
    values = sorted(samples)
    return dict(count=len(values), **{f'p{p}_ms': values[min(len(values)-1, (len(values)*p+99)//100-1)] * 1000 for p in (50, 95, 99)})


def usage(pid):
    if platform.system() == 'Linux':
        # ps TIME on Linux rounds to whole seconds; procfs retains clock ticks.
        # comm is parenthesized and may itself contain spaces or parentheses.
        fields = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()
        return dict(cpu_seconds=(int(fields[11])+int(fields[12]))/os.sysconf('SC_CLK_TCK'),
                    rss_kib=int(fields[21])*os.sysconf('SC_PAGE_SIZE')//1024)
    fields = subprocess.check_output(['ps', '-p', str(pid), '-o', 'time=,rss='], text=True, timeout=2).split()
    cpu = 0
    for part in fields[0].split(':'):
        cpu = cpu * 60 + float(part)
    return dict(cpu_seconds=cpu, rss_kib=int(fields[1]))


class Receiver(http.server.ThreadingHTTPServer):
    def __init__(self, delay):
        super().__init__(('127.0.0.1', 0), Handler)
        self.slots = threading.BoundedSemaphore(8)
        self.pending = {}
        self.condition = threading.Condition()
        self.delay = delay
        self.duplicates = 0

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(3)
        return sock, address

    def process_request(self, request, address):
        if not self.slots.acquire(timeout=3):
            self.close_request(request)
            return
        try:
            super().process_request(request, address)
        except BaseException:
            self.slots.release()
            raise

    def process_request_thread(self, request, address):
        try:
            super().process_request_thread(request, address)
        finally:
            self.slots.release()


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def do_POST(self):
        size = int(self.headers.get('Content-Length', '0'))
        if not 0 < size <= 65536:
            self.send_error(413)
            return
        event = json.loads(self.rfile.read(size))
        if self.server.delay:
            time.sleep(self.server.delay)
        self.send_response(204)
        self.send_header('Content-Length', '0')
        self.end_headers()
        self.wfile.flush()
        with self.server.condition:
            slot = self.server.pending.get(event['source_message_id'])
            if slot is not None:
                if slot[1] is not None:
                    self.server.duplicates += 1
                slot[1] = time.perf_counter() - slot[0]
                self.server.condition.notify_all()

    def log_message(self, *args):
        pass


def command_bytes(fmt, cmd):
    if fmt == 'json':
        return encode(fmt, cmd)
    if fmt in ('cbor', 'msgpack'):
        return getattr(vectors, fmt + '_command')({'schema_version': 1, **cmd})
    p = vectors.pfield
    device = b''.join(p(i, cmd['device'][key]) for i, key in enumerate(('tenant_id','product_id','device_id'), 1))
    args = b''.join(p(6, p(1, k) + p(2, vectors.proto_scalar(v))) for k, v in cmd['payload']['arguments'].items())
    return p(1, 1, 0) + p(2, cmd['command_id']) + p(3, device) + p(4, cmd['expires_at'], 0) + p(5, cmd['payload']['name']) + args


def client(device, management_address, receiver, fmt, protocol, qos, version, workload, worker, seconds, run):
    credential = f'{fmt}-{worker}'
    prefix = f'v1/t/demo/p/{fmt}/d/d{worker}/'
    mqtt = protocol == 'mqtt'
    udp = protocol == 'udp'
    v5 = b'\0' if version == 5 else b''
    transport, sink, commands = [], [], []
    failures = {}
    boot = uuid.uuid4().bytes
    sock = socket.socket(type=socket.SOCK_DGRAM if udp else socket.SOCK_STREAM)
    sock.settimeout(3)
    sock.connect(device)
    try:
        if mqtt:
            body = text('MQTT') + bytes([version, 0xc2, 0, 30]) + v5 + text(credential) + text(credential) + text(KEY.hex())
            sock.sendall(packet(0x10, body))
            header, reply = mqtt_read(sock)
            assert header == 0x20 and reply[1] == 0
            if workload == 'command':
                sock.sendall(packet(0x82, b'\0\1' + v5 + text(prefix + 'down') + b'\1'))
                assert mqtt_read(sock)[0] == 0x90
        elif not udp:
            sock.sendall(frame(json.dumps({'credential_id': credential, 'secret': KEY.hex()}).encode()))
            assert json.loads(tcp_read(sock))['authenticated']
        deadline = time.perf_counter() + seconds
        sequence = 0
        while time.perf_counter() < deadline:
            sequence += 1
            source = f'{run}:{worker}:{sequence}'
            if workload == 'command':
                cmd = dict(command_id=str(uuid.uuid4()), device=dict(tenant_id='demo', product_id=fmt, device_id=f'd{worker}'),
                    expires_at=int(time.time()*1000)+60000, payload=dict(name='set', arguments={'number': 42.0, 'text': 'x'*128}))
                at = time.perf_counter()
                management(management_address, 'devices/commands', cmd)
                if mqtt:
                    header, data = mqtt_read(sock)
                    assert header >> 4 == 3
                    end = 2 + struct.unpack('!H', data[:2])[0]
                    command_qos = (header >> 1) & 3
                    pid = data[end:end+2] if command_qos else b''
                    end += len(pid)
                    if version == 5:
                        length = 0
                        for shift in range(0, 28, 7):
                            b = data[end]; end += 1
                            length |= (b & 127) << shift
                            if b < 128:
                                break
                        else:
                            raise ValueError('command properties')
                        end += length
                        assert end <= len(data)
                    data = data[end:]
                    if command_qos == 1:
                        sock.sendall(packet(0x40, pid))
                    elif command_qos == 2:
                        sock.sendall(packet(0x50, pid))
                        assert mqtt_read(sock)[0] == 0x62
                        sock.sendall(packet(0x70, pid))
                else:
                    data = tcp_read(sock)
                assert data == command_bytes(fmt, cmd), 'command wire differs'
                commands.append(time.perf_counter() - at)
                # Management deliberately closes responses. Bound fresh sockets so
                # repeated cases cannot exhaust the shared host's ephemeral ports.
                time.sleep(.01)
                continue
            if workload == 'heartbeat':
                kind, fields = 'heartbeat', {'sequence': sequence}
            else:
                # UDP's large case remains below the unchanged 1200-byte envelope.
                count = 4 if workload == 'large' and udp else 64 if workload == 'large' else 2
                fields = {f'f{i}': 'x'*128 if workload == 'large' else i+0.5 for i in range(count)}
                kind = 'telemetry'
            payload = encode(fmt, dict(schema_version=1, source_message_id=source, kind=kind, data=fields))
            at = time.perf_counter()
            with receiver.condition:
                if len(receiver.pending) >= 64:
                    raise ValueError('pending bound')
                receiver.pending[source] = [at, None]
            try:
                if mqtt:
                    sock.sendall(packet(0x30 | (qos << 1), text(prefix+'up') + (b'\0\2' if qos else b'') + v5 + payload))
                    if qos:
                        header, data = mqtt_read(sock)
                        assert header == (0x40 if qos == 1 else 0x50) and data[:2] == b'\0\2'
                        if qos == 2:
                            sock.sendall(packet(0x62, b'\0\2'))
                            assert mqtt_read(sock)[0] == 0x70
                        transport.append(time.perf_counter()-at)
                elif udp:
                    name = credential.encode()
                    wire = b'NBI1'+bytes([len(name)])+name+struct.pack('!I',1)+boot+struct.pack('!QqH',sequence,int(time.time()*1000),len(payload))+payload
                    assert len(wire)+32 <= 1200
                    sock.send(wire+hmac.digest(KEY,wire,'sha256'))
                    ack = sock.recv(65)
                    assert len(ack)==64 and ack[:4]==b'NBA1' and ack[4:32]==struct.pack('!I',1)+boot+struct.pack('!Q',sequence)
                    assert hmac.compare_digest(ack[32:],hmac.digest(KEY,ack[:32],'sha256'))
                    transport.append(time.perf_counter()-at)
                else:
                    # Exercise split headers and payloads on every TCP frame.
                    data = frame(payload)
                    sock.sendall(data[:3]); sock.sendall(data[3:])
                    decode_ack(tcp_read(sock))
                    transport.append(time.perf_counter()-at)
                with receiver.condition:
                    if not receiver.condition.wait_for(lambda: receiver.pending[source][1] is not None, timeout=3):
                        raise TimeoutError('webhook response')
                    sink.append(receiver.pending[source][1])
            finally:
                with receiver.condition:
                    receiver.pending.pop(source, None)
    except urllib.error.HTTPError as exc:
        failures[f'HTTP_{exc.code}'] = 1
    except (OSError, EOFError) as exc:
        failures[type(exc).__name__] = 1
    finally:
        if mqtt:
            try:
                sock.sendall(b'\xe0\0')
            except OSError:
                pass
        sock.close()
    return transport, sink, commands, failures


def counters(address):
    result = {}
    for line in management(address, 'metrics').decode().splitlines():
        parts = line.split()
        if len(parts)==2 and parts[0].startswith('netbaiot_') and '{' not in parts[0] and parts[1].isdecimal():
            result[parts[0]] = int(parts[1])
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--server', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--revision', required=True)
    p.add_argument('--seconds', type=float, default=1)
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--sink-delay-ms', type=float, default=0)
    p.add_argument('--codecs', nargs='+', choices=FORMATS, default=FORMATS)
    p.add_argument('--protocols', nargs='+', choices=('mqtt', 'tcp', 'udp'), default=('mqtt', 'tcp', 'udp'))
    p.add_argument('--workloads', nargs='+', choices=('small', 'large', 'heartbeat', 'command'), default=('small', 'large', 'heartbeat', 'command'))
    args = p.parse_args()
    if not (0.05<=args.seconds<=3600 and 1<=args.workers<=32 and 1<=args.repeats<=10 and 0<=args.sink_delay_ms<=500):
        p.error('bounded arguments required')
    if all(w == 'command' for w in args.workloads) and all(t == 'udp' for t in args.protocols):
        p.error('commands require a live MQTT/TCP session')
    args.output.mkdir(parents=True, exist_ok=False)
    receiver = Receiver(args.sink_delay_ms/1000)
    owner = threading.Thread(target=receiver.serve_forever)
    owner.start()
    reservation = socket.socket(); reservation.bind(('127.0.0.1',0)); device=reservation.getsockname()
    udp = socket.socket(type=socket.SOCK_DGRAM); udp.bind(device)
    admin = socket.socket(); admin.bind(('127.0.0.1',0)); management_address=f'127.0.0.1:{admin.getsockname()[1]}'
    config = dict(development=True, device_ingress=f'{device[0]}:{device[1]}', management_http=management_address,
        spool_directory=str(args.output.resolve()/'spool'), delivery_url=f'http://127.0.0.1:{receiver.server_port}/events',
        limits=dict(max_connections_per_ip=256, requests_per_second=100000, requests_per_ip_second=100000,
            messages_per_device_second=100000, messages_per_tenant_second=100000, sink_delivery_concurrency=4,
            max_replay_entries_per_device=32, max_replay_entries_per_tenant=4096, max_replay_entries=4096,
            # Unique synthetic commands need a short, explicit bounded retention
            # window during long sweeps; keep the 4096-entry admission limit.
            command_dedup_ttl_ms=1000),
        credentials=[dict(credential_id=f'{f}-{i}',secret_hex=KEY.hex(),identity=dict(device_key=dict(tenant_id='demo',product_id=f,device_id=f'd{i}'),
            credential_version=1,auth_generation=1,codec_id=f'netbaiot-{f}',codec_version=1,permissions=dict(publish=True,commands=True))) for f in FORMATS for i in range(args.workers)])
    path=args.output/'server.json';path.write_text(json.dumps(config))
    rows=[];server=None
    environment=dict(platform=platform.platform(),machine=platform.machine(),cpu_count=os.cpu_count(),
        rust=subprocess.check_output(['rustc','-Vv'],text=True),revision=args.revision,
        server_sha256=hashlib.sha256(args.server.read_bytes()).hexdigest(), harness_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        arguments={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        timing='socket send to receipt; socket send to webhook response flushed; HTTP command submit to device bytes',
        cpu_measurement='Linux procfs clock ticks; macOS ps process time',
        limitations=['Python closed loop and shared host, no production capacity claim','RSS sampled at 100ms, not allocator peak',
            'CPU clock resolution is platform dependent; short windows are noisy','No application-final-processing measurement','UDP large payload has 4 fields; stream large has 64',
            'QoS0 receipt unavailable; throughput uses observed required webhook responses','No per-connection allocator isolation'])
    (args.output/'environment.json').write_text(json.dumps(environment,indent=2))
    try:
        with (args.output/'server.log').open('w') as log:
            for s in (reservation,udp,admin):s.close()
            server=subprocess.Popen([str(args.server.resolve()),str(path.resolve())],cwd=ROOT,
                env={**os.environ,'NETBAIOT_ADMIN_SECRET':ADMIN,'RUST_LOG':'error','TOKIO_WORKER_THREADS':'10'},stdout=log,stderr=log)
            for _ in range(160):
                try:
                    management(management_address,'ready');break
                except OSError:
                    if server.poll() is not None:raise RuntimeError('server startup failed; inspect server.log')
                    time.sleep(.05)
            else:raise TimeoutError('readiness')
            cases=[('mqtt',v,q) for v in (4,5) for q in (0,1,2)]+[('tcp',4,0),('udp',4,0)]
            for repeat in range(args.repeats):
                for fmt in dict.fromkeys(args.codecs):
                    for protocol,version,qos in cases:
                        if protocol not in args.protocols:continue
                        for workload in dict.fromkeys(args.workloads):
                            if workload=='command' and (protocol=='udp' or (protocol=='mqtt' and qos!=1)):continue
                            before=usage(server.pid);initial=counters(management_address);at=time.perf_counter();samples=[]
                            run=uuid.uuid4().hex[:12]
                            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                                futures=[pool.submit(client,device,management_address,receiver,fmt,protocol,qos,version,workload,i,args.seconds,run) for i in range(args.workers)]
                                while not all(f.done() for f in futures):
                                    samples.append(usage(server.pid));time.sleep(.1)
                                results=[f.result() for f in futures]
                            elapsed=time.perf_counter()-at;after=usage(server.pid);final=counters(management_address)
                            transport=[v for r in results for v in r[0]];sink=[v for r in results for v in r[1]];commands=[v for r in results for v in r[2]]
                            row=dict(repeat=repeat,codec=fmt,protocol=protocol,mqtt_version=version if protocol=='mqtt' else None,qos=qos,workload=workload,
                                elapsed_s=elapsed,events=len(sink),events_per_s=len(sink)/elapsed,command_count=len(commands),commands_per_s=len(commands)/elapsed,
                                receipt=quantiles(transport),webhook_response=quantiles(sink),command_received=quantiles(commands),
                                cpu_pct=(after['cpu_seconds']-before['cpu_seconds'])/elapsed*100,
                                idle_rss_kib=before['rss_kib'],median_rss_kib=statistics.median(s['rss_kib'] for s in samples),peak_sampled_rss_kib=max(s['rss_kib'] for s in samples),
                                sampled_growth_kib_per_connection=(max(s['rss_kib'] for s in samples)-before['rss_kib'])/args.workers,
                                errors=[r[3] for r in results if r[3]],status=json.loads(management(management_address,'status')),
                                counter_delta={k:final[k]-initial.get(k,0) for k in final if final[k]!=initial.get(k,0)})
                            rows.append(row)
                            (args.output/'summary.json').write_text(json.dumps(rows,indent=2))
                            print(json.dumps({k:row[k] for k in ('codec','protocol','qos','workload','events','command_count','errors')}),flush=True)
                            assert not row['errors'], 'client errors; inspect summary.json and counter_delta'
                            assert row['events'] or row['command_count'], 'no successful work; inspect summary.json'
            server.terminate()
            assert server.wait(timeout=40)==0, 'graceful shutdown failed'
    finally:
        if server is not None and server.poll() is None:
            server.kill();server.wait(timeout=5)
        receiver.shutdown();owner.join(timeout=3);receiver.server_close()
        for s in (reservation,udp,admin):s.close()


if __name__=='__main__':
    main()
