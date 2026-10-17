#!/usr/bin/env python3
"""Seed the production parser with valid deep paths before mutation fuzzing."""
from pathlib import Path
import struct
import hashlib
import importlib.util
import json

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

# Independent wire vectors cover valid kinds, field boundaries and semantic errors.
fixtures = Path(__file__).parents[1] / 'crates/netbaiot-codecs/tests/fixtures'
spec = importlib.util.spec_from_file_location('codec_vectors', fixtures / 'generate.py')
vectors = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vectors)
for codec in ['cbor', 'msgpack', 'protobuf']:
    target = codec + '_codec'
    for fixture in sorted(fixtures.glob('*.' + codec)):
        if fixture.stem == 'command':
            continue
        data = fixture.read_bytes()
        seed(target, fixture.stem, data)
        seed(target, fixture.stem + '-truncated', data[:-1])
        seed(target, fixture.stem + '-trailing', data + b'\xff')
    hostile = {
        'cbor': [b'\xbb' + b'\xff'*8, b'\x7b' + b'\xff'*8,
                 b'\xa2\x61x\x01\x61x\x02', b'\xa1\x61x'*65 + b'\x00', b'\xbf\xff'],
        'msgpack': [b'\xdf' + b'\xff'*4, b'\xdb' + b'\xff'*4,
                    b'\x82\xa1x\x01\xa1x\x02', b'\x81\xa1x'*65 + b'\x00', b'\xc7\xff\0'],
        'protobuf': [b'\x12' + b'\xff'*10, b'\x08\x01\x08\x01', b'\xaa\x06\x03abc',
                     b'\x62\x02\x08\x01', b'\xff'*11],
    }
    for i, data in enumerate(hostile[codec]):
        seed(target, f'hostile-{i}', data)
    for count, width in [(1,256), (64,128), (64,256), (65,1), (1,257)]:
        value = {'schema_version':1, 'source_message_id':'s'*64, 'kind':'telemetry',
                 'data':{f'f{i}':'x'*width for i in range(count)}}
        seed(target, f'fields-{count}-text-{width}', getattr(vectors,codec)(value))
    valid = (fixtures / ('telemetry.'+codec)).read_bytes()
    if codec == 'protobuf':
        seed(target, 'valid-with-unknown-fields', valid + b'\xa0\x06\x01\xaa\x06\x03abc')
        seed(target, 'valid-with-duplicate-version', valid + b'\x08\x01')
    else:
        enc = getattr(vectors,codec)
        duplicate = bytes([valid[0]+1]) + valid[1:] + enc('schema_version') + enc(1)
        seed(target, 'duplicate-envelope', duplicate)
        data_key = enc('data')
        at = valid.index(data_key) + len(data_key)
        marker = b'\xa2\x61n\x01\x61n\x02' if codec == 'cbor' else b'\x82\xa1n\x01\xa1n\x02'
        seed(target, 'duplicate-telemetry-fields', valid[:at] + marker)

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
# Identical bodies with disjoint transport types; also exercise ACK body validation.
ack_body = struct.pack('!BB16sqQQ', 1, 1, bytes(range(16)), -2, 1, 0)
seed('tcp_frame', 'ack-body', ack_body)
seed('tcp_frame', 'typed-ack', struct.pack('!I', 0x8000002a) + ack_body)
seed('tcp_frame', 'ack-shaped-payload', struct.pack('!I', 42) + ack_body)
for length in [0, 41, 43, 0x7fffffff]:
    seed('tcp_frame', f'invalid-ack-length-{length}', struct.pack('!I', 0x80000000 | length))
for i, body in enumerate([b'{"schema_version":1,"source_message_id":"1","kind":"heartbeat","data":{"sequence":1}}', b'{"schema_version":1,"source_message_id":"1","kind":"telemetry","data":{"temperature":25.3}}']):
    seed('json_codec', f'valid-{i}', body)
    udp = b'NBI1\x01a' + struct.pack('!I',1) + b'\x01'*16 + struct.pack('!QqH',1,1000,len(body)) + body + b'\x00'*32
    seed('udp_envelope', f'valid-{i}', udp)
for count in [34, 35, 64, 65]:
    body = json.dumps({"schema_version": 1, "source_message_id": "decimal", "kind": "telemetry",
                       "data": {f"f{i}": {"decimal": "25.3"} for i in range(count)}},
                      separators=(',', ':')).encode()
    seed('json_codec', f'decimal-fields-{count}', body)
for name, scalar in [('decimal-extra-key', {"decimal": "1", "extra": True}),
                     ('decimal-array', [{"decimal": "1"}]),
                     ('decimal-nested', {"decimal": {"decimal": {"decimal": "1"}}})]:
    seed('json_codec', name, json.dumps({"schema_version": 1, "source_message_id": "decimal",
         "kind": "telemetry", "data": {"value": scalar}}, separators=(',', ':')).encode())
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

limits = {"max_frame_payload_bytes": 8192, "max_concurrent_streams": 256,
          "initial_stream_window_bytes": 262144, "initial_connection_window_bytes": 4194304,
          "heartbeat_ms": 5000}
for version in [0, 1, 2, 3, 4, 65535]:
    bootstrap = json.dumps({"type": "hello", "version": version, "token": "test-token", "limits": limits}).encode()
    seed('business_rpc_bootstrap', f'hello-{version}', bootstrap)

for flags in [0, 1]:
    header = struct.pack('!IIBBH', 1, 2, 4, flags, 0)
    seed('business_rpc_wire', f'data-{flags}', header + b'x')
open_request = json.dumps({"kind": "rpc", "parent_stream_id": None, "request_id": "00000000-0000-0000-0000-000000000001",
                           "method": "device.command.send", "deadline_ms": 1000, "content_length": 0}).encode()
seed('business_rpc_wire', 'rpc-open', struct.pack('!IIBBH', len(open_request), 1, 1, 0, 0) + open_request)

record = json.dumps({"event": {"event_id": "00000000-0000-0000-0000-000000000001", "source_message_id": "seed:1",
    "device": {"tenant_id": "demo", "product_id": "sensor", "device_id": "device-1"}, "received_at": 1,
    "kind": {"kind": "heartbeat", "data": {"sequence": 1}}}, "pending_sinks": ["rpc"],
    "routing_revision": 1, "accepted_at": 1, "attempts": {"rpc": 2}}, separators=(',', ':')).encode()
seed('restart_spool', 'current-record-body', record)
framed = b'NBSP' + struct.pack('!IQ', 3, 1) + struct.pack('!I', len(record)) + record + hashlib.sha256(record).digest()
trailer = b'SEND' + struct.pack('!QQ', 1, len(framed))
seed('restart_spool', 'current-heartbeat', framed + trailer + hashlib.sha256(framed + trailer).digest())
provider = json.dumps({"kind": "provider", "provider_id": "primary"}).encode()
seed('business_rpc_wire', 'provider-rpc', struct.pack('!IIBBH', len(provider), 1, 1, 0, 0) + provider
     + struct.pack('!IIBBH', len(open_request), 3, 1, 0, 0) + open_request.replace(b'null', b'1   ', 1))

# Vendor payloads and configurations need valid seeds to pass header/layout/checksum gates.
import copy
vendor = json.loads((Path(__file__).parents[1] / 'configs/codecs/acme-v1.json').read_text())
seed('vendor_config', 'acme-valid', json.dumps(vendor).encode())
for tag, body in [(1, struct.pack('<Ih', 42, -123) + struct.pack('!H', 654) + b'\x01A100'),
                  (2, struct.pack('<I', 42) + struct.pack('!Q', 2**64-1)),
                  (3, struct.pack('<I', 42) + b'\0\x07'),
                  (4, struct.pack('<I', 42) + bytes(15) + b'\x01\x02')]:
    wire = b'\xa5\x5a' + struct.pack('!HB', len(body)+6, tag) + body
    wire += bytes([sum(wire) & 255])
    seed('vendor_codec', f'valid-{tag}', wire)
    for n in range(len(wire)):
        seed('vendor_codec', f'truncated-{tag}-{n}', wire[:n])
    seed('vendor_codec', f'trailing-{tag}', wire+b'\0')
    for n in [0, 2, 4, len(wire)-1]:
        mutated = bytearray(wire)
        mutated[n] ^= 255
        seed('vendor_codec', f'malformed-{tag}-{n}', mutated)
for value in [0., 0.3, 1.7, 5.1, -0.3, -1.7, -5.1, 5.15, 1.25, 16777217., 1e-300, float('inf'), float('nan')]:
    for scale, bias in [(1., 0.), (0.1, 0.), (1., 1e20), (1e300, 0.)]:
        seed('vendor_codec', f'numeric-{value}-{scale}-{bias}', struct.pack('<ddd', value, scale, bias))
for kind in ['u8', 'u16', 'u32', 'u64']:
    config = copy.deepcopy(vendor)
    config['commands'][0]['expires_at']['type'] = kind
    seed('vendor_config', f'expiry-{kind}', json.dumps(config).encode())
for field, values in [('scale', [0, 0.1, -1, 1e300]), ('offset', [0, 21, 2**64-1]), ('type', ['f32', 'f64', 'u64'])]:
    for value in values:
        config = copy.deepcopy(vendor)
        config['commands'][0]['expires_at'] = None
        config['commands'][0]['checksum'] = None
        config['commands'][0]['fields'][0]['field'][field] = value
        seed('vendor_config', f'field-{field}-{value}', json.dumps(config).encode())
for size in [0, 31, 32, 65536, 65537]:
    config = copy.deepcopy(vendor)
    config['commands'][0]['size'] = size
    seed('vendor_config', f'size-{size}', json.dumps(config).encode())
seed('vendor_codec', 'oversized', bytes(65537))
seed('vendor_config', 'oversized', b' ' * (262144+1))
seed('vendor_config', 'unknown-field', json.dumps(dict(vendor, unknown='rejected')).encode())
for setting in ['scale', 'bias']:
    config = copy.deepcopy(vendor)
    config['messages'][0]['payload']['fields'][0]['field'][setting] = None
    seed('vendor_config', f'integer-{setting}-null', json.dumps(config).encode())
config = {"id": "vendor-fuzz-u64", "version": 1, "header": [17],
          "message_type": {"offset": 1, "type": "u8"},
          "messages": [{"tag": 1, "size": 11,
                        "source_message_id": {"offset": 2, "type": "u8"},
                        "payload": {"kind": "telemetry", "fields": [{"name": "v",
                                    "field": {"offset": 3, "type": "u64"}}]}}]}
seed('vendor_config', 'integer-u64-identity', json.dumps(config).encode())

valid = (root / 'vendor_codec' / 'valid-1').read_bytes()
for name, offset, value in [('bool', 13, 2), ('utf8', 14, 255), ('text-control', 14, 0), ('tag', 4, 255), ('length', 3, 18)]:
    malformed = bytearray(valid)
    malformed[offset] = value
    malformed[-1] = sum(malformed[:-1]) & 255
    seed('vendor_codec', f'invalid-{name}-with-checksum', malformed)
