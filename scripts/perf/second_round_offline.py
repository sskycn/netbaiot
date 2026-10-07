#!/usr/bin/env python3
"""Bounded own-device persistent-session fanout and reconnect replay measurement."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import tempfile
import time

from connection_memory import SECRET, credential, free_ports, rss_kib, status
from event_load import management_get
from mqtt_broker_network import mqtt_packet, mqtt_text, recv_exact

ROOT = Path(__file__).resolve().parents[2]
TOPIC = 'v1/t/t0/p/p/d/d0/up'


def receive(stream):
    first = recv_exact(stream, 1)[0]
    length = 0
    multiplier = 1
    for _ in range(4):
        digit = recv_exact(stream, 1)[0]
        length += (digit & 127) * multiplier
        if length > 65536:
            raise RuntimeError('oversized measurement response')
        if not digit & 128:
            return first, recv_exact(stream, length)
        multiplier *= 128
    raise RuntimeError('invalid remaining length')


def connect(port, client, clean):
    stream = socket.create_connection(('127.0.0.1', port), timeout=5)
    try:
        stream.sendall(mqtt_packet(0x10, mqtt_text('MQTT') + bytes([4, 0xC2 if clean else 0xC0, 1, 44]) + mqtt_text(client) + mqtt_text('a0') + mqtt_text(SECRET)))
        first, body = receive(stream)
        if first != 0x20 or len(body) != 2 or body[1] != 0:
            raise RuntimeError('CONNECT failed')
        return stream, bool(body[0] & 1)
    except BaseException:
        stream.close()
        raise


def disconnect(stream):
    try:
        stream.sendall(b'\xe0\x00')
    finally:
        stream.close()


def quantiles(values):
    ordered = sorted(values)
    return {'count': len(values), 'mean_ms': statistics.mean(values), **{f'p{p}_ms': ordered[min(len(ordered) - 1, (len(ordered) * p + 99) // 100 - 1)] for p in (50, 95, 99)}, 'max_ms': ordered[-1]}


def run(binary, sessions, messages, output):
    device, admin = free_ports(2)
    with tempfile.TemporaryDirectory(prefix='netbaiot-offline-perf-') as temporary:
        limits = {'max_subscriptions_per_device': sessions + 16, 'max_subscriptions_per_tenant': sessions + 16, 'max_subscriptions': max(512, sessions + 16), 'mqtt_recovery_max_bytes': 512 * 1024 * 1024, 'requests_per_second': 1000000, 'requests_per_ip_second': 1000000, 'messages_per_device_second': 1000000, 'messages_per_tenant_second': 1000000}
        config = {'development': True, 'device_ingress': f'127.0.0.1:{device}', 'management_http': f'127.0.0.1:{admin}', 'business_tcp': None, 'credentials': [credential(0)], 'limits': limits, 'tls': None, 'delivery_url': None, 'auth_provider_url': None, 'spool_directory': temporary + '/spool'}
        config_path = Path(temporary) / 'config.json'
        config_path.write_text(json.dumps(config))
        environment = os.environ.copy()
        environment['NETBAIOT_ADMIN_SECRET'] = 'ab' * 32
        environment['NETBAIOT_PERF_LOCK_METRICS'] = '0'
        with output.with_suffix('.log').open('w') as log:
            server = subprocess.Popen([str(binary), str(config_path)], cwd=ROOT, env=environment, stdout=log, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 10
                while True:
                    if server.poll() is not None:
                        raise RuntimeError('gateway exited before readiness')
                    try:
                        status(admin, False)
                        break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise
                        time.sleep(.005)
                for i in range(sessions):
                    stream, present = connect(device, f'offline-{i}', False)
                    try:
                        if present:
                            raise RuntimeError('new session already exists')
                        stream.sendall(mqtt_packet(0x82, b'\x00\x01' + mqtt_text(TOPIC) + b'\x01'))
                        if receive(stream) != (0x90, b'\x00\x01\x01'):
                            raise RuntimeError('SUBSCRIBE failed')
                    finally:
                        disconnect(stream)
                    time.sleep(.005)  # Untimed setup; one live session per DeviceKey.
                publisher, _ = connect(device, 'producer', True)
                ack_ms = []
                try:
                    for i in range(messages):
                        payload = json.dumps({'schema_version': 1, 'source_message_id': f'offline-{i}', 'kind': 'telemetry', 'data': {f'f{n}': 'x' * 225 for n in range(4)}}, separators=(',', ':')).encode()
                        packet_id = (i + 1).to_bytes(2, 'big')
                        started = time.perf_counter()
                        publisher.sendall(mqtt_packet(0x32, mqtt_text(TOPIC) + packet_id + payload))
                        if receive(publisher) != (0x40, packet_id):
                            raise RuntimeError('producer PUBACK failed')
                        ack_ms.append((time.perf_counter() - started) * 1000)
                finally:
                    disconnect(publisher)
                peak_rss = rss_kib(server.pid)
                replay_ms = []
                replayed = 0
                for i in range(sessions):
                    started = time.perf_counter()
                    stream, present = connect(device, f'offline-{i}', False)
                    try:
                        if not present:
                            raise RuntimeError('persistent session lost')
                        seen = set()
                        for _ in range(messages):
                            first, body = receive(stream)
                            if first >> 4 != 3 or (first >> 1) & 3 != 1:
                                raise RuntimeError('unexpected replay packet')
                            topic_bytes = int.from_bytes(body[:2], 'big')
                            if body[2:2 + topic_bytes].decode() != TOPIC:
                                raise RuntimeError('replay topic changed')
                            packet_id = body[2 + topic_bytes:4 + topic_bytes]
                            decoded = json.loads(body[4 + topic_bytes:])
                            seen.add(decoded['source_message_id'])
                            stream.sendall(mqtt_packet(0x40, packet_id))
                        if seen != {f'offline-{n}' for n in range(messages)}:
                            raise RuntimeError('replay payload missing/duplicated')
                        replayed += len(seen)
                    finally:
                        disconnect(stream)
                    replay_ms.append((time.perf_counter() - started) * 1000)
                metrics = management_get(admin, '/api/v1/metrics')
                result = {'server_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'sessions': sessions, 'messages': messages, 'replayed': replayed, 'expected_replayed': sessions * messages, 'producer_ack': quantiles(ack_ms), 'session_connect_and_replay': quantiles(replay_ms), 'rss_with_backlog_kib': peak_rss, 'metrics': metrics, 'limits_overlay': limits, 'errors': 0, 'scope': 'One authenticated DeviceKey, multiple offline MQTT persistent ClientIds, one live connection at a time; no offline DeviceCommand.'}
            finally:
                if server.poll() is None:
                    server.send_signal(signal.SIGTERM)
                    server.wait(timeout=30)
                if server.returncode:
                    raise RuntimeError('planned shutdown failed; inspect log')
            result['server_exit'] = server.returncode
            output.write_text(json.dumps(result, indent=2))
            print(output, result['replayed'], flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--before', required=True, type=Path)
    parser.add_argument('--after', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--sessions', type=int, default=128)
    parser.add_argument('--messages', type=int, default=32)
    args = parser.parse_args()
    if not (1 <= args.sessions <= 128 and 1 <= args.messages <= 32):
        parser.error('sessions 1..128, messages 1..32 (at most 4096 offline responsibilities)')
    args.output.mkdir(parents=True, exist_ok=True)
    for index, label in enumerate('ABBA'):
        run((args.before if label == 'A' else args.after).resolve(), args.sessions, args.messages, args.output / f'offline-{index}-{label}.json')
