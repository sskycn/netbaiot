"""Focused regression checks for measurement integrity and bounded framing."""
import asyncio
import argparse
import json
import pathlib
import struct
import tempfile
import unittest
from unittest.mock import patch

import capacity_audit
import capacity_consumer
import capacity_summary


class SamplingTests(unittest.TestCase):
    def test_loopback_netstat_has_no_mac_address_column(self):
        text = 'Name Mtu Network Address Ipkts Ierrs Ibytes Opkts Oerrs Obytes Coll\nlo0 16384 <Link#1> 100 0 1200 110 0 1400 0\n'
        with patch.object(capacity_audit.sys, 'platform', 'darwin'), patch.object(capacity_audit, 'command', return_value=text):
            result = capacity_audit.interface_sample('lo0')
        self.assertEqual(result['rx_bytes'], 1200)
        self.assertEqual(result['tx_bytes'], 1400)
        self.assertEqual(result['tx_packets'], 110)

    def test_10k_default_reservation_fails_before_invalid_server_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            args=argparse.Namespace(bundle=directory,connections=10000,rate=0,base_port=24000,
                duration=15,window=32,payload=256,warmup=3,cooldown=3,connection_reservation=524288)
            with self.assertRaisesRegex(ValueError,'logical budget'):
                capacity_audit.prepare(args)

    def test_network_data_without_timestamps_is_not_invented(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory)/'network.csv'
            path.write_text(',bytes_in,bytes_out,\nserver.1,100,200,\n')
            self.assertIsNone(capacity_summary.network_rates(path, 1, 2))
            path.write_text('epoch,process,bytes_in,bytes_out\n10,server.1,100,200\n12,server.1,500,300\n')
            result = capacity_summary.network_rates(path, 10, 12)
            self.assertEqual(result['rx_bytes_s'], 200)
            self.assertEqual(result['tx_bytes_s'], 50)
            self.assertIsNone(capacity_summary.network_rates(path, 100, 102))


class FramingTests(unittest.IsolatedAsyncioTestCase):
    async def test_partial_frame_survives_observer_deadline(self):
        reader = asyncio.StreamReader(limit=65540)
        payload = json.dumps({'type':'ready','version':1}).encode()
        wire = struct.pack('!I',len(payload))+payload
        reader.feed_data(wire[:6])
        task = asyncio.create_task(capacity_consumer.read_frame(reader))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), .001)
        reader.feed_data(wire[6:])
        self.assertEqual((await task)['type'], 'ready')

    async def test_oversize_is_rejected_before_waiting_for_body(self):
        reader = asyncio.StreamReader(limit=65540)
        reader.feed_data(struct.pack('!I',65537))
        with self.assertRaises(ValueError):
            await capacity_consumer.read_frame(reader)


if __name__ == '__main__':
    unittest.main()
