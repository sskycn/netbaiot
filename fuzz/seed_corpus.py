#!/usr/bin/env python3
"""Seed the production parser with valid deep paths before mutation fuzzing."""
from pathlib import Path
import struct
import hashlib

root = Path(__file__).parent / 'corpus'
def text(b):
    return struct.pack('!H', len(b)) + b

def packet(first, b):
    n, v = len(b), bytearray([first])
    while True:
        digit, n = n % 128, n // 128
        v.append(digit | (128 if n else 0))
        if not n:
            return bytes(v) + b

def seed(target, name, data):
    path = root / target
    path.mkdir(parents=True, exist_ok=True)
    (path / name).write_bytes(data)

for flags in [0xc2, 0xc0, 0xc6, 0xd6]:
    body = text(b'MQTT') + bytes([4, flags, 0, 30]) + text(b'a')
    if flags & 4:
        body += text(b'will') + text(b'payload')
    body += text(b'a') + text(b'0'*64)
    seed('mqtt_packet', f'connect-{flags}', packet(0x10, body))
for i, topic in enumerate([b'v1/t/t/p/p/d/a/down', b'#', b'a/+', b'a/#/b', b'a+', b'\x00', b'\xef\xbb\xbf']):
    seed('mqtt_packet', f'subscribe-{i}', packet(0x82, b'\x00\x01' + text(topic) + b'\x01'))
    seed('mqtt_packet', f'unsubscribe-{i}', packet(0xa2, b'\x00\x01' + text(topic)))
for size in [1, 128, 16384, 65500]:
    seed('mqtt_packet', f'publish-{size}', packet(0x32, text(b'v1/t/t/p/p/d/a/up') + b'\x00\x01' + b'x'*size))
    seed('tcp_frame', f'frame-{size}', struct.pack('!I',size)+b'x'*size)
for i, body in enumerate([b'{"schema_version":1,"source_message_id":"1","kind":"heartbeat","data":{"sequence":1}}', b'{"schema_version":1,"source_message_id":"1","kind":"telemetry","data":{"temperature":25.3}}']):
    seed('json_codec', f'valid-{i}', body)
    udp = b'NBI1\x01a' + struct.pack('!I',1) + b'\x01'*16 + struct.pack('!QqH',1,1000,len(body)) + body + b'\x00'*32
    seed('udp_envelope', f'valid-{i}', udp)
for i, data in enumerate([b'\x7f', b'\x80\x01', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff']):
    seed('mqtt_remaining_length', str(i), data)
    seed('mqtt_fixed_header', str(i), b'\x30'+data)

# Current recovery images include authoritative integrity and minimal unsupported headers.
header = b'NBSP' + struct.pack('!IQ', 3, 1)
trailer = b'SEND' + struct.pack('!QQ', 0, len(header))
seed('restart_spool', 'v3-empty', header + trailer + hashlib.sha256(header + trailer).digest())
for fixture in (Path(__file__).parents[1] / 'tests/mqtt_conformance/fixtures/mqtt_recovery').glob('*.nbmq'):
    seed('mqtt_recovery', fixture.stem, fixture.read_bytes())

for version in [0, 1, 2, 4, 0xffffffff]:
    seed('restart_spool', f'unsupported-{version}', b'NBSP' + struct.pack('!I', version))
for version in [0, 1, 2, 3, 4, 5, 7, 0xffffffff]:
    seed('mqtt_recovery', f'unsupported-{version}', b'NBMQ' + struct.pack('!I', version))

import json
limits = {"max_frame_payload_bytes": 8192, "max_concurrent_streams": 256,
          "initial_stream_window_bytes": 262144, "initial_connection_window_bytes": 4194304,
          "heartbeat_ms": 5000}
for version in [0, 1, 2, 3, 4, 65535]:
    bootstrap = json.dumps({"type": "hello", "version": version, "token": "test-token", "limits": limits}).encode()
    seed('business_rpc_bootstrap', f'hello-{version}', bootstrap)

for flags in [0, 1]:
    header = struct.pack('!IIBBH', 1, 2, 4, flags, 0)
    seed('business_rpc_v3', f'data-{flags}', header + b'x')
open_request = json.dumps({"kind": "rpc", "parent_stream_id": None, "request_id": "00000000-0000-0000-0000-000000000001",
                           "method": "device.command.send", "deadline_ms": 1000, "content_length": 0}).encode()
seed('business_rpc_v3', 'rpc-open', struct.pack('!IIBBH', len(open_request), 1, 1, 0, 0) + open_request)

record = json.dumps({"event": {"event_id": "00000000-0000-0000-0000-000000000001", "source_message_id": "seed:1",
    "device": {"tenant_id": "demo", "product_id": "sensor", "device_id": "device-1"}, "received_at": 1,
    "kind": {"kind": "heartbeat", "data": {"sequence": 1}}}, "pending_sinks": ["rpc"],
    "routing_revision": 1, "accepted_at": 1, "attempts": {"rpc": 2}}, separators=(',', ':')).encode()
seed('restart_spool', 'current-record-body', record)
framed = b'NBSP' + struct.pack('!IQ', 3, 1) + struct.pack('!I', len(record)) + record + hashlib.sha256(record).digest()
trailer = b'SEND' + struct.pack('!QQ', 1, len(framed))
seed('restart_spool', 'current-heartbeat', framed + trailer + hashlib.sha256(framed + trailer).digest())
provider = json.dumps({"kind": "provider", "provider_id": "primary"}).encode()
seed('business_rpc_v3', 'provider-rpc', struct.pack('!IIBBH', len(provider), 1, 1, 0, 0) + provider
     + struct.pack('!IIBBH', len(open_request), 3, 1, 0, 0) + open_request.replace(b'null', b'1   ', 1))
