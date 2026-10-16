"""Cross-language golden vector and bounded Python TCP ACK reads."""
import importlib.util
from pathlib import Path
import struct
import unittest

spec = importlib.util.spec_from_file_location('device_tcp', Path(__file__).resolve().parents[2] / 'examples/device_tcp.py')
tcp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tcp)


class FragmentedSocket:
    def __init__(self, data):
        self.data = data

    def recv(self, size):
        size = min(size, 3)
        part, self.data = self.data[:size], self.data[size:]
        return part


class TcpAckTests(unittest.TestCase):
    def test_golden_fragmented_and_coalesced(self):
        body = bytes.fromhex('0101000102030405060708090a0b0c0d0e0ffffffffffffffffe00000000010203040000000005060708')
        socket = FragmentedSocket(struct.pack('!I', tcp.ACK_FLAG | 42) + body + struct.pack('!I', tcp.ACK_FLAG | 42) + body)
        expected = dict(event_id='00010203-0405-0607-0809-0a0b0c0d0e0f', accepted_at=-2,
                        required_deliveries=0x01020304, best_effort_deliveries=0x05060708)
        self.assertEqual(tcp.recv_ack(socket), expected)
        self.assertEqual(tcp.recv_ack(socket), expected)
        self.assertEqual(socket.data, b'')

    def test_invalid_header_never_reads_payload(self):
        for length in [0, 41, 42, 43, tcp.ACK_FLAG, tcp.ACK_FLAG | 41, tcp.ACK_FLAG | 43, 0xffffffff]:
            socket = FragmentedSocket(struct.pack('!I', length) + b'unread')
            with self.assertRaises(ValueError):
                tcp.recv_ack(socket)
            self.assertEqual(socket.data, b'unread')

    def test_rejects_invalid_version_type_legacy_and_truncation(self):
        for body in [b'', bytes(42), b'\x02\x01' + bytes(40), b'\x01\x02' + bytes(40), b'{"event_id":"old"}']:
            with self.assertRaises(ValueError):
                tcp.decode_ack(body)
        for length in range(42):
            with self.assertRaises(ConnectionError):
                tcp.recv_ack(FragmentedSocket(struct.pack('!I', tcp.ACK_FLAG | 42) + bytes(length)))

    def test_conflicting_commands_interleaved_with_acks(self):
        body = struct.pack('!BB16sqQQ', 1, 1, bytes(16), -2, 1, 0)
        payloads = [body, b'\x01', b'\x01\xff' + bytes(40), bytes(range(256))]
        wire = b''.join(struct.pack('!I', len(p)) + p + struct.pack('!I', tcp.ACK_FLAG | 42) + body
                        for p in payloads)
        socket = FragmentedSocket(wire)
        for p in payloads:
            self.assertEqual(tcp.recv_downlink(socket), (tcp.PAYLOAD, p))
            self.assertEqual(tcp.recv_downlink(socket), (tcp.EVENT_ACCEPTED, tcp.decode_ack(body)))
        self.assertEqual(socket.data, b'')

    def test_all_partial_frame_disconnects_and_malformed_acks(self):
        body = struct.pack('!BB16sqQQ', 1, 1, bytes(16), 0, 0, 0)
        for header in [42, tcp.ACK_FLAG | 42]:
            wire = struct.pack('!I', header) + body
            for end in range(len(wire)):
                with self.assertRaises(ConnectionError):
                    tcp.recv_downlink(FragmentedSocket(wire[:end]))
        for offset in [0, 1]:
            bad = bytearray(body)
            bad[offset] = 255
            with self.assertRaises(ValueError):
                tcp.recv_downlink(FragmentedSocket(struct.pack('!I', tcp.ACK_FLAG | 42) + bad))

    def test_signed_and_unsigned_boundaries(self):
        for timestamp in [-2**63, 0, 2**63 - 1]:
            decoded = tcp.decode_ack(struct.pack('!BB16sqQQ', 1, 1, bytes(16), timestamp, 0, 2**64 - 1))
            self.assertEqual(decoded['accepted_at'], timestamp)
            self.assertEqual(decoded['best_effort_deliveries'], 2**64 - 1)

    def test_handshake_json_is_unchanged(self):
        body = b'{"authenticated":true}'
        self.assertEqual(tcp.recv_frame(FragmentedSocket(struct.pack('!I', len(body)) + body)), {'authenticated': True})


if __name__ == '__main__':
    unittest.main()
