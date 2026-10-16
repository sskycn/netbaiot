"""CPU and acceptance evidence must use explicit, consistent windows."""
import json
from pathlib import Path
import tempfile
import unittest

import tcp_ack_summary
import test_udp_capacity_summary


class TcpSummaryTests(unittest.TestCase):
    def fixture(self):
        raw = test_udp_capacity_summary.UdpSummaryTests().fixture()
        raw['result']['groups']['tcp'] = raw['result']['groups'].pop('udp')
        raw['plan']['groups'] = [dict(label='tcp', rate=10)]
        raw['loadgen_exit'] = 0
        status = dict(event_count=0, event_bytes=0, pending_required=0,
                      active_connections=dict(tcp=0), runtime_tasks=2)
        raw['cooldown']['status'] = status
        raw['idle'] = dict(status=status, counters={})
        return raw

    def summarize(self, data):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'run.json'
            path.write_text(json.dumps(data))
            return tcp_ack_summary.summarize(path)

    def test_cpu_and_acceptance_use_separate_matching_windows(self):
        row = self.summarize(self.fixture())
        self.assertEqual(row['cpu_us_per_event'], 20000)
        self.assertEqual(row['cpu_sample_events'], 100)
        self.assertEqual(row['receipt_per_second'], 9)
        self.assertEqual(row['receipt_percent'], 90)
        self.assertEqual(row['offered_percent'], 90)
        self.assertIsNone(row['host_tcp_retransmits'])

    def test_counter_reset_or_pending_work_fails_validation(self):
        raw = self.fixture()
        raw['samples'][2]['counters']['events_accepted'] = 1
        with self.assertRaises(AssertionError):
            self.summarize(raw)
        raw = self.fixture()
        raw['cooldown']['status']['pending_required'] = 1
        with self.assertRaises(AssertionError):
            self.summarize(raw)

    def test_all_zero_kernel_counters_are_not_zero_loss_evidence(self):
        raw = self.fixture()
        raw['tcp_before'] = '0 packet sent\n0 data packet (0 byte) retransmitted'
        raw['tcp_after'] = raw['tcp_before']
        row = self.summarize(raw)
        self.assertFalse(row['tcp_kernel_counters_usable'])
        self.assertIsNone(row['host_tcp_retransmits'])
        raw['tcp_after'] = '20 packets sent\n3 data packets (45 bytes) retransmitted'
        row = self.summarize(raw)
        self.assertTrue(row['tcp_kernel_counters_usable'])
        self.assertEqual(row['host_tcp_retransmits'], 3)

    def test_tcp_retransmits(self):
        self.assertEqual(tcp_ack_summary.retransmits('12 data packets (45 bytes) retransmitted'), 12)
        self.assertIsNone(tcp_ack_summary.retransmits('unavailable'))


if __name__ == '__main__':
    unittest.main()
