import importlib.util
import pathlib
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
spec = importlib.util.spec_from_file_location('production', ROOT / 'production_readiness.py')
production = importlib.util.module_from_spec(spec)
spec.loader.exec_module(production)


class EvidenceTests(unittest.TestCase):
    def test_percentiles_use_window_delta_and_preserve_overflow(self):
        prefix = 'netbaiot_codec_latency_us'
        before = {prefix + '_count': 100, prefix + '_bucket{le="10"}': 100, prefix + '_bucket{le="+Inf"}': 100}
        after = {prefix + '_count': 200, prefix + '_bucket{le="10"}': 150, prefix + '_bucket{le="+Inf"}': 200}
        result = production.histograms(before, after)['codec_latency_us']
        self.assertEqual(result['count'], 100)
        self.assertEqual(result['p50_upper_ms'], .01)
        self.assertIsNone(result['p99_upper_ms'])

    def test_ramp_memory_is_not_a_leak_and_fixed_population_growth_is_a_suspect(self):
        def row(t, rss):
            return dict(measurement_s=t, rss_kib=rss, runtime_tasks=108, active_sessions=100, event_count=0, sink_pending=0)
        self.assertEqual(production.growth([row(0, 1000), row(5, 50000)])['status'], 'insufficient_observation')
        stable = [row(60 + i * 60, 50000) for i in range(20)]
        self.assertFalse(production.growth(stable)['suspects'])
        growing = [row(60 + i * 60, 50000 + i * 2000) for i in range(20)]
        self.assertEqual(production.growth(growing)['suspects'], ['rss_kib'])


if __name__ == '__main__':
    unittest.main()
