#!/usr/bin/env python3
"""Bounded real TLS/UDP acceptance, required-sink and mixed restart regression.

Four events fill a stopped required sink. No transport may ACK rejected fifth work.
Planned restart must recover those exact event IDs and a pending MQTT QoS1 delivery.
"""
import argparse
import hashlib
import hmac
import http.client
import http.server
import json
import os
from pathlib import Path
import signal
import socket
import ssl
import struct
import subprocess
import tempfile
import threading
import time

from mixed_ingress_audit import ROOT, CERT, SECRET, Control, server_config, stop


def exact(sock, count):
    assert 0 <= count <= 65536
    data = bytearray()
    while len(data) < count:
        part = sock.recv(count - len(data))
        if not part:
            raise EOFError('closed')
        data.extend(part)
    return bytes(data)


def text(value):
    data = value.encode()
    return struct.pack('>H', len(data)) + data


def packet(first, data):
    size = len(data); header = bytearray([first])
    while True:
        digit = size % 128; size //= 128
        header.append(digit | (128 if size else 0))
        if not size:
            return bytes(header) + data


def mqtt_read(sock):
    first = exact(sock, 1)[0]; size = 0
    for n in range(4):
        byte = exact(sock, 1)[0]; size += (byte & 127) << (7*n)
        if byte < 128:
            return first, exact(sock, size)
    raise AssertionError('malformed MQTT length')


def payload(source):
    return json.dumps(dict(schema_version=1, source_message_id=source, kind='heartbeat', data=dict(sequence=1))).encode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    seen = {}; healthy = threading.Event(); errors = []

    class Sink(http.server.BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                self.connection.settimeout(2)
                size = int(self.headers.get('Content-Length', '-1'))
                assert 0 <= size <= 65536
                event = json.loads(self.rfile.read(size))
                source = event['source_message_id']; event_id = event['event_id']
                assert len(seen) < 32 or source in seen
                seen.setdefault(source, set()).add(event_id)
                assert len(seen[source]) <= 4
                self.send_response(204 if healthy.is_set() else 503)
                self.send_header('Content-Length', '0'); self.send_header('Connection', 'close'); self.end_headers()
            except Exception as exc:
                errors.append(str(exc)[:128])
        def log_message(self, *_):
            pass
    sink = http.server.HTTPServer(('127.0.0.1', 0), Sink)
    thread = threading.Thread(target=sink.serve_forever, daemon=True); thread.start()
    context = ssl.create_default_context(cafile=str(CERT))
    sockets = []; child = None
    result = dict(test='mixed-tls-required-boundary-restart', binary_sha256=hashlib.sha256(args.server.read_bytes()).hexdigest())
    try:
        with tempfile.TemporaryDirectory(prefix='netbaiot-mixed-semantics-') as temp:
            folder = Path(temp)
            reserve = [socket.socket() for _ in range(2)]
            for s in reserve:
                s.bind(('127.0.0.1', 0))
            device, management = [s.getsockname()[1] for s in reserve]
            plan = dict(groups=[dict(workers=1, offset=i) for i in range(4)], sink=True,
                        limits=dict(global_event_max_count=4, sink_queue_max_count=4, sink_delivery_concurrency=1,
                                    sink_max_attempts=1, shutdown_drain_timeout_ms=1200, requests_per_ip_second=100000))
            config = server_config(folder, plan, device, management, sink.server_port)
            (folder / 'config.json').write_text(json.dumps(config))
            for s in reserve: s.close()
            def start():
                log = open(folder / 'server.log', 'a')
                process = subprocess.Popen([str(args.server.resolve()), str(folder / 'config.json')], cwd=ROOT,
                    env={**os.environ, 'NETBAIOT_ADMIN_SECRET': 'ab' * 32, 'RUST_LOG': 'error'}, stdout=log, stderr=log)
                log.close()
                for _ in range(100):
                    try:
                        c = Control(management, True); c.get('ready'); c.close(); return process
                    except (OSError, ValueError, http.client.HTTPException):
                        if process.poll() is not None: raise AssertionError((folder / 'server.log').read_text())
                        time.sleep(.05)
                process.kill(); process.wait(); raise AssertionError('startup timeout')
            def tls():
                s = context.wrap_socket(socket.create_connection(('127.0.0.1', device), timeout=2), server_hostname='localhost')
                sockets.append(s); return s
            def request(method, path, identity, data=None, admin=False):
                c = http.client.HTTPSConnection('localhost', management if admin else device, timeout=2, context=context)
                try:
                    token = 'ab' * 32 if admin else f'a{identity}:{SECRET}'
                    c.request(method, path, body=data, headers={'Authorization': 'Bearer ' + token})
                    r = c.getresponse(); b = r.read(65537); assert len(b) <= 65536
                    return r.status, b
                finally:
                    c.close()
            def mqtt_open():
                s = tls()
                s.sendall(packet(0x10, text('MQTT') + bytes([4, 0xc0, 0, 30]) + text('semantic-persistent') + text('a1') + text(SECRET)))
                return s, mqtt_read(s)
            def udp_wire(seq, source):
                body = payload(source); auth = b'a3'
                data = b'NBI1' + bytes([len(auth)]) + auth + struct.pack('>I', 1) + bytes([9])*16 + struct.pack('>QqH', seq, int(time.time()*1000), len(body)) + body
                return data + hmac.new(bytes(range(32)), data, hashlib.sha256).digest()
            def udp_ack(seq):
                data = udp.recv(65)
                assert len(data) == 64 and data[:4] == b'NBA1' and data[4:8] == struct.pack('>I', 1)
                assert data[8:24] == bytes([9])*16 and data[24:32] == struct.pack('>Q', seq)
                assert hmac.compare_digest(data[32:], hmac.new(bytes(range(32)), data[:32], hashlib.sha256).digest())
            def no_ack(s, read):
                s.settimeout(.15)
                try:
                    data = read()
                    assert not data, 'unaccepted work received ACK'
                except (TimeoutError, socket.timeout, EOFError, ConnectionError, ssl.SSLError):
                    pass
                finally:
                    s.settimeout(2)
            child = start()
            status, body = request('POST', '/v1/device/data', 0, payload('accepted-http'))
            assert status == 202; http_id = json.loads(body)['event_id']
            mqtt, connack = mqtt_open(); assert connack == (0x20, b'\x00\x00')
            topic = 'v1/t/t0/p/p/d/d1/up'
            mqtt.sendall(packet(0x82, b'\x00\x09' + text(topic) + b'\x01'))
            assert mqtt_read(mqtt) == (0x90, b'\x00\x09\x01')
            mqtt.sendall(packet(0x32, text(topic) + b'\x00\x01' + payload('accepted-mqtt')))
            assert mqtt_read(mqtt) == (0x40, b'\x00\x01')
            first, outbound = mqtt_read(mqtt); assert first == 0x32
            topic_len = struct.unpack('>H', outbound[:2])[0]; old_pid = outbound[2+topic_len:4+topic_len]
            tcp = tls(); auth = json.dumps(dict(credential_id='a2', secret=SECRET)).encode()
            tcp.sendall(struct.pack('>I', len(auth)) + auth)
            assert json.loads(exact(tcp, struct.unpack('>I', exact(tcp, 4))[0])) == {'authenticated': True}
            body = payload('accepted-tcp'); tcp.sendall(struct.pack('>I', len(body)) + body)
            tcp_id = json.loads(exact(tcp, struct.unpack('>I', exact(tcp, 4))[0]))['event_id']
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM); udp.settimeout(2); udp.connect(('127.0.0.1', device)); sockets.append(udp)
            udp.send(udp_wire(1, 'accepted-udp')); udp_ack(1)
            c = Control(management, True); state = c.sample(); c.close()
            assert state['status']['pending_required'] == 4 and state['counters']['events_accepted'] == 4
            assert request('POST', '/v1/device/data', 0, payload('rejected-http'))[0] == 429
            mqtt.sendall(packet(0x32, text(topic) + b'\x00\x02' + payload('rejected-mqtt')))
            no_ack(mqtt, lambda: exact(mqtt, 1))
            body = payload('rejected-tcp'); tcp.sendall(struct.pack('>I', len(body)) + body)
            no_ack(tcp, lambda: exact(tcp, 1))
            for _ in range(2):
                udp.send(udp_wire(2, 'rejected-udp')); no_ack(udp, lambda: udp.recv(65))
            c = Control(management, True); state = c.sample(); c.close()
            assert state['counters']['events_accepted'] == 4 and state['counters']['udp_acks_sent'] == 1
            assert state['status']['pending_required'] == 4
            result['full_bus'] = state
            assert request('POST', '/api/v1/drain', 0, admin=True)[0] == 202
            management_unready = False; device_closed = False
            for _ in range(20):
                try:
                    status, _ = request('GET', '/api/v1/ready', 0, admin=True)
                    management_unready |= status == 503
                except (OSError, http.client.HTTPException):
                    break
                try:
                    response, _ = request('POST', '/v1/device/data', 0, payload('after-quiesce'))
                    assert response != 202
                except (OSError, http.client.HTTPException):
                    device_closed = True
                udp.send(udp_wire(3, 'after-quiesce-udp')); no_ack(udp, lambda: udp.recv(65))
                time.sleep(.03)
            assert management_unready and device_closed
            assert child.wait(timeout=5) == 0
            spools = list((folder / 'spool').glob('*.spool')); assert spools
            result['shutdown'] = dict(management_observed_unready=True, device_ingress_closed=True, spool_bytes=sum(p.stat().st_size for p in spools), exit_code=0)
            healthy.set(); child = start()
            resumed, connack = mqtt_open(); assert connack == (0x20, b'\x01\x00')
            first, replay = mqtt_read(resumed); assert first == 0x3a and replay[2+topic_len:4+topic_len] == old_pid and replay.endswith(payload('accepted-mqtt'))
            resumed.sendall(packet(0x40, old_pid))
            for _ in range(100):
                c = Control(management, True); state = c.sample(); c.close()
                if state['status']['pending_required'] == 0: break
                time.sleep(.05)
            else: raise AssertionError('required replay did not drain')
            assert set(seen) == {'accepted-http', 'accepted-mqtt', 'accepted-tcp', 'accepted-udp'} and not errors
            assert seen['accepted-http'] == {http_id} and seen['accepted-tcp'] == {tcp_id}
            assert all(len(ids) == 1 for ids in seen.values())
            result['recovered_ids'] = {source: sorted(ids) for source, ids in seen.items()}
            result['recovery'] = state
            # Previously rejected sequence is still usable; only now receives signed acceptance.
            udp.send(udp_wire(2, 'retry-udp')); udp_ack(2)
            resumed.sendall(b'\xe0\x00'); resumed.close()
            result['retry_previously_rejected_udp'] = True
            assert request('POST', '/api/v1/drain', 0, admin=True)[0] == 202
            assert child.wait(timeout=5) == 0
            result['pass'] = True
    finally:
        result['cleanup'] = stop(child, 5)
        for s in sockets: s.close()
        sink.shutdown(); sink.server_close(); thread.join(timeout=2)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
