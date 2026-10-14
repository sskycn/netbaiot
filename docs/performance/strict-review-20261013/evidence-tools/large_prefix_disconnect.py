#!/usr/bin/env python3
"""Bounded live-socket probe of maximum TCP frames interrupted after a large prefix."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import time

p = argparse.ArgumentParser()
p.add_argument('--repo', type=Path, required=True)
p.add_argument('--server', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
a.output.mkdir(exist_ok=False)
sys.path.insert(0, str(a.repo / 'scripts/perf'))
import connection_memory as memory
import event_load

def reserve_pair():
    for _ in range(32):
        tcp = socket.socket()
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            tcp.bind(('127.0.0.1', 0))
            udp.bind(tcp.getsockname())
            return tcp, udp
        except OSError:
            tcp.close()
            udp.close()
    raise RuntimeError('fixture port reservation exhausted')

def exact(s, size):
    data = bytearray()
    while len(data) < size:
        part = s.recv(size - len(data))
        if not part:
            raise RuntimeError('unexpected authentication EOF')
        data.extend(part)
    return bytes(data)

defaults = json.loads(subprocess.check_output([str(a.server), '--print-default-limits'], text=True))
assert defaults['max_tcp_frame_size'] == 65536
assert defaults['max_read_buffer_per_connection'] == 65540
device_pair, management_pair = reserve_pair(), reserve_pair()
device = device_pair[0].getsockname()[1]
management = management_pair[0].getsockname()[1]
config = {'device_ingress': f'127.0.0.1:{device}', 'management_http': f'127.0.0.1:{management}',
          'development': True, 'credentials': [memory.credential(i) for i in range(16)],
          'business_tcp': None, 'tls': None, 'delivery_url': None, 'auth_provider_url': None,
          'spool_directory': str(a.output / 'spool')}
config_path = a.output / 'server.json'
config_path.write_text(json.dumps(config))
environment = os.environ.copy()
environment.update(NETBAIOT_ADMIN_SECRET='ab' * 32, NETBAIOT_ALLOCATION_PROFILE='1', TOKIO_WORKER_THREADS='10')
log = (a.output / 'server.log').open('w+')
for s in (*device_pair, *management_pair):
    s.close()
server = subprocess.Popen([str(a.server), str(config_path)], stdout=subprocess.DEVNULL, stderr=log, env=environment)
clients = []
result = {'server_sha256': hashlib.sha256(a.server.read_bytes()).hexdigest(),
          'wrapper_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
          'connections': 16, 'declared_payload_bytes': 65536, 'sent_partial_payload_bytes': 32768,
          'limits': {k: defaults[k] for k in ['max_tcp_frame_size', 'max_read_buffer_per_connection',
                     'global_connection_logical_bytes', 'max_connection_bytes_per_tenant']},
          'snapshots': {}, 'notes': ['Allocator requests are process-wide, not RSS or private quota balances.',
              'No limits are overridden. Partial payloads never become accepted events.',
              'The configuration contains synthetic test credentials and is not published.']}

def snapshot(name):
    time.sleep(1.3)
    log.flush()
    log.seek(0)
    records = []
    for line in log:
        try:
            r = json.loads(line)
            if r.get('event') == 'allocation_profile':
                records.append(r)
        except (ValueError, AttributeError):
            pass
    if not records:
        raise RuntimeError('no actual allocator sample')
    state = memory.status(management, False)
    metrics = event_load.management_get(management, '/api/v1/metrics')
    accepted = re.search(r'^netbaiot_events_accepted_total (\d+)$', metrics, re.M)
    assert accepted
    r = {'status': state, 'allocator': records[-1], 'accepted_total': int(accepted.group(1)),
         'monotonic_seconds': time.monotonic()}
    result['snapshots'][name] = r
    return r

try:
    deadline = time.monotonic() + 10
    while True:
        try:
            memory.status(management, False)
            break
        except Exception:
            if server.poll() is not None or time.monotonic() >= deadline:
                raise RuntimeError('child failed before readiness')
            time.sleep(.05)
    idle = snapshot('idle')
    for i in range(16):
        s = socket.create_connection(('127.0.0.1', device), timeout=3)
        clients.append(s)
        hello = json.dumps({'credential_id': f'a{i}', 'secret': memory.SECRET}).encode()
        s.sendall(len(hello).to_bytes(4, 'big') + hello)
        size = int.from_bytes(exact(s, 4), 'big')
        if not 1 <= size <= 1024:
            raise RuntimeError('unbounded authentication response')
        assert json.loads(exact(s, size))['authenticated'] is True
    connected = snapshot('authenticated_idle')
    assert memory.active_connection_count(connected['status']) == 16
    prefix = (65536).to_bytes(4, 'big') + b'{' + b' ' * 32767
    for s in clients:
        s.sendall(prefix)
    held = snapshot('partial_held')
    assert memory.active_connection_count(held['status']) == 16
    assert held['accepted_total'] == 0
    for s in clients:
        s.close()
    clients.clear()
    deadline = time.monotonic() + 10
    while True:
        state = memory.status(management, False)
        if memory.active_connection_count(state) == 0 and state['runtime_tasks'] == idle['status']['runtime_tasks']:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError('connection cleanup deadline')
        time.sleep(.05)
    cleaned = snapshot('post_disconnect')
    assert cleaned['accepted_total'] == 0
    assert cleaned['status']['pending_required'] == 0
    assert cleaned['status']['runtime_tasks'] == idle['status']['runtime_tasks']
    live = lambda s: s['allocator']['requested_live_bytes']
    result['held_minus_authenticated_requested_bytes'] = live(held) - live(connected)
    result['held_minus_post_disconnect_requested_bytes'] = live(held) - live(cleaned)
    assert result['held_minus_authenticated_requested_bytes'] >= 16 * 65540
    assert result['held_minus_post_disconnect_requested_bytes'] >= 16 * 65540
    server.send_signal(signal.SIGTERM)
    result['server_exit_code'] = server.wait(timeout=15)
    assert result['server_exit_code'] == 0
    result['measurement_valid'] = True
except BaseException as error:
    result['measurement_valid'] = False
    result['error'] = {'type': type(error).__name__, 'text': str(error)}
    raise
finally:
    for s in clients:
        s.close()
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=15)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)
    result.setdefault('server_exit_code', server.returncode)
    log.close()
    (a.output / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
