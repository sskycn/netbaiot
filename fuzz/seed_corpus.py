#!/usr/bin/env python3
"""Seed the production parser with valid deep paths before mutation fuzzing."""
from pathlib import Path
import struct

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
