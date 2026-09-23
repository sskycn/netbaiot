#!/usr/bin/env python3
"""Small paired connection+one accepted-event probe; not a production capacity test."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import socket
import ssl
import statistics
import struct
import subprocess
import tempfile
import time

from connection_memory import ROOT, SECRET, status


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("unexpected EOF")
        data.extend(chunk)
    return bytes(data)


def text(value):
    value = value.encode() if isinstance(value, str) else value
    return struct.pack('!H', len(value)) + value


def packet(first, body):
    length = len(body)
    header = bytearray([first])
    while True:
        digit = length % 128
        length //= 128
        header.append(digit | (128 if length else 0))
        if not length:
            return bytes(header) + body


def frame(body):
    return struct.pack('!I', len(body)) + body


def read_frame(sock):
    length, = struct.unpack('!I', receive(sock, 4))
    assert 0 < length <= 65536
    return receive(sock, length)


def cycle(port, protocol, context):
    sock = socket.socket()
    sock.settimeout(2)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(('127.0.0.1', 0))
    sock.connect(('127.0.0.1', port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if context:
        sock = context.wrap_socket(sock, server_hostname='localhost')
    payload = b'{"schema_version":1,"source_message_id":"probe","kind":"heartbeat","data":{"sequence":1}}'
    with sock:
        if protocol == 'http':
            request = (f'POST /v1/device/data HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer demo-device:{SECRET}\r\nContent-Length: {len(payload)}\r\nConnection: close\r\n\r\n').encode()+payload
            sock.sendall(request)
            response = bytearray()
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                response.extend(chunk)
                assert len(response) <= 8192
            assert response.startswith(b'HTTP/1.1 202 '), response[:128]
        elif protocol == 'mqtt':
            hello = text('MQTT')+b'\x04\xc2\x00\x3c'+text('ingress-probe')+text('demo-device')+text(SECRET)
            sock.sendall(packet(0x10, hello))
            assert receive(sock, 4) == b'\x20\x02\x00\x00'
            sock.sendall(packet(0x32, text('v1/t/demo/p/sensor/d/device-1/up')+b'\x00\x01'+payload))
            assert receive(sock, 4) == b'\x40\x02\x00\x01'
            sock.sendall(b'\xe0\x00')
            assert sock.recv(1) == b''  # Server owns the active close after DISCONNECT.
        else:
            sock.sendall(frame(json.dumps(dict(credential_id='demo-device', secret=SECRET)).encode()))
            assert json.loads(read_frame(sock)) == {'authenticated': True}
            sock.sendall(frame(payload))
            assert 'event_id' in json.loads(read_frame(sock))


def run(binary, shared, tls, count, verify_clients):
    with tempfile.TemporaryDirectory(prefix='netbaiot-ingress-') as directory:
        # Reserve all TCP addresses and the matching UDP address simultaneously.
        reservations = []
        for _ in range(5):
            sock = socket.socket(); sock.bind(('127.0.0.1', 0)); reservations.append(sock)
        ports = [sock.getsockname()[1] for sock in reservations]
        udp = socket.socket(type=socket.SOCK_DGRAM)
        udp.bind(('127.0.0.1', ports[0] if shared else 0))
        config = json.loads(Path(ROOT, 'configs/development.json').read_text())
        config['device_ingress'] = f'127.0.0.1:{ports[0]}'
        if not shared:
            del config['device_ingress']
            config.update(device_http=f'127.0.0.1:{ports[0]}', mqtt=f'127.0.0.1:{ports[2]}', tcp=f'127.0.0.1:{ports[3]}', udp=f'127.0.0.1:{udp.getsockname()[1]}')
        config['management_http'] = f'127.0.0.1:{ports[1]}'
        config['spool_directory'] = directory+'/spool'
        config['limits'].update(requests_per_second=1000000, requests_per_ip_second=1000000, messages_per_device_second=1000000, messages_per_tenant_second=1000000, max_connections_per_device=64)
        cert = str(Path(ROOT, 'tests/fixtures/localhost-cert.pem'))
        context = ssl.create_default_context(cafile=cert) if tls else None
        if tls:
            config['tls'] = dict(certificate=cert, private_key=str(Path(ROOT, 'tests/fixtures/localhost-key.pem')))
        config_path = Path(directory, 'config.json'); config_path.write_text(json.dumps(config)); config_path.chmod(0o600)
        for sock in reservations: sock.close()
        udp.close()
        with open(Path(directory, 'server.log'), 'w+') as log:
            child = subprocess.Popen([binary, str(config_path)], stdout=log, stderr=log, env={**os.environ, 'NETBAIOT_ADMIN_SECRET': 'ab'*32, 'RUST_LOG': 'error'})
            try:
                deadline = time.monotonic()+10
                while True:
                    try:
                        status(ports[1], tls)
                        break
                    except (OSError, RuntimeError):
                        if child.poll() is not None or time.monotonic() > deadline:
                            log.seek(0); raise RuntimeError(log.read())
                        time.sleep(.01)
                targets = dict(http=ports[0], mqtt=ports[0] if shared else ports[2], tcp=ports[0] if shared else ports[3])
                if verify_clients:
                    for qos in range(3):
                        subprocess.run(['mosquitto_pub', '-h', 'localhost', '-p', str(targets['mqtt']), '-V', 'mqttv311', '-q', str(qos), '-u', 'demo-device', '-P', SECRET, '-t', 'v1/t/demo/p/sensor/d/device-1/up', '-m', '{"schema_version":1,"source_message_id":"mosquitto","kind":"heartbeat","data":{"sequence":1}}'] + (['--cafile', cert] if tls else []), check=True, timeout=5, capture_output=True)
                result = {}
                for protocol, port in targets.items():
                    for _ in range(50): cycle(port, protocol, context)
                    started = time.perf_counter()
                    for _ in range(count): cycle(port, protocol, context)
                    seconds = time.perf_counter()-started
                    result[protocol] = dict(count=count, seconds=seconds, connections_events_per_second=count/seconds)
                return result
            finally:
                child.send_signal(signal.SIGTERM)
                try: child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill(); child.wait(); raise
                if child.returncode: raise RuntimeError(f'server exit {child.returncode}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before', required=True)
    parser.add_argument('--after', required=True)
    parser.add_argument('--count', type=int, default=500)
    parser.add_argument('--repetitions', type=int, default=5)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    assert 1 <= args.count <= 10000 and 1 <= args.repetitions <= 10
    result = dict(environment=platform.platform(), baseline='bae843e302ce931f859523ae425ed17db0e4b820', binaries={label: hashlib.sha256(Path(binary).read_bytes()).hexdigest() for label,binary in [('before',args.before),('after',args.after)]}, runs=[], summary={})
    for tls in [False, True]:
        for repetition in range(args.repetitions):
            for label in (['before','after'] if repetition % 2 == 0 else ['after','before']):
                measurement = run(getattr(args,label), label=='after', tls, args.count, label=='after' and repetition==0)
                result['runs'].append(dict(label=label, tls=tls, repetition=repetition, protocols=measurement))
                print(f'{label} tls={tls} repetition={repetition}: '+str({p: round(v['connections_events_per_second'],1) for p,v in measurement.items()}), flush=True)
        for protocol in ['http','mqtt','tcp']:
            rates = {label: statistics.median(row['protocols'][protocol]['connections_events_per_second'] for row in result['runs'] if row['tls']==tls and row['label']==label) for label in ['before','after']}
            rates['delta_percent'] = 100*(rates['after']/rates['before']-1)
            result['summary'][f'{protocol}_{"tls" if tls else "plain"}'] = rates
    Path(args.output).write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result['summary'],indent=2))

if __name__ == '__main__': main()
