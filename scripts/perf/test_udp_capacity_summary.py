"""Keep ACK and gateway measurement windows distinct in UDP evidence."""
import json
from pathlib import Path
import tempfile
import unittest

import udp_capacity_summary


class UdpSummaryTests(unittest.TestCase):
    def fixture(self):
        return dict(
            name='fixture', server_sha256='server', loadgen_sha256='loadgen',
            plan=dict(seconds=10), server_stop=dict(exit_code=0, forced=False),
            result=dict(groups=dict(udp=dict(
                counters=dict(accepted=90, attempted=100, udp_no_ack=12),
                latencies=dict(acceptance=dict(p99_ms=2))))),
            samples=[dict(t=t, server=dict(cpu_seconds=cpu, rss_kib=32),
                          counters=dict(events_accepted=events),
                          status=dict(pending_required=1, runtime_tasks=2))
                     for t, cpu, events in [(-1, 0, 0), (1, 1, 10), (9, 3, 110), (11, 9, 300)]],
            cooldown=dict(counters=dict(events_accepted=300)),
            network_before='5 dropped due to full socket buffers',
            network_after='8 dropped due to full socket buffers',
        )

    def summarize(self, data):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'run.json'
            path.write_text(json.dumps(data))
            return udp_capacity_summary.summarize(path)

    def test_cpu_uses_matching_samples_not_receipt_or_cooldown_counts(self):
        result = self.summarize(self.fixture())
        self.assertEqual(result['cpu_us_per_event'], 20_000)
        self.assertEqual(result['cpu_percent'], 25)
        self.assertEqual(result['gateway_events_per_second'], 12.5)
        self.assertEqual(result['receipt_per_second'], 9)
        self.assertEqual(result['receipt_percent'], 90)
        self.assertEqual(result['client']['udp_no_ack'], 12)
        self.assertEqual(result['kernel_full_socket_drops'], 3)
        self.assertEqual(result['full_run_counters']['events_accepted'], 300)

    def test_missing_kernel_counter_stays_unknown(self):
        data = self.fixture()
        data['network_after'] = 'unavailable'
        self.assertIsNone(self.summarize(data)['kernel_full_socket_drops'])

    def test_counter_reset_is_rejected(self):
        data = self.fixture()
        data['samples'][2]['counters']['events_accepted'] = 1
        with self.assertRaises(ValueError):
            self.summarize(data)


if __name__ == '__main__':
    unittest.main()
