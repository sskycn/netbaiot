import importlib.util
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("connection_memory", ROOT / "connection_memory.py")
memory = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(memory)


def final(established=1000, **counters):
    return {
        "counters": {
            "connect_attempts": established,
            "connected": established,
            "disconnected": established,
            **counters,
        },
        "error_samples": [],
    }


class PopulationTests(unittest.TestCase):
    def test_final_record_is_required_and_samples_cannot_prove_completion(self):
        sample = {"event": "sample", "stats": final()}
        complete = {"event": "final", "stats": final(192)}
        import json
        self.assertIsNone(memory.parse_load_final(json.dumps(sample)))
        self.assertEqual(
            memory.parse_load_final("not json\nfalse\n[]\n" + json.dumps(sample) + "\n" + json.dumps(complete)),
            final(192),
        )

    def test_active_population_uses_actual_connections_and_rejects_partial_success(self):
        denominator, errors = memory.validate_population(1000, 1000, False, final(), 0)
        self.assertEqual(denominator, 1000)
        self.assertFalse(errors)
        denominator, errors = memory.validate_population(1000, 192, False, final(192), 0)
        self.assertEqual(denominator, 192)
        self.assertTrue(errors)
        self.assertTrue(memory.validate_population(1000, 0, False, final(0), 0)[1])

    def test_disconnected_zero_active_uses_proven_unique_persistent_population(self):
        denominator, errors = memory.validate_population(1000, 0, True, final(), 0)
        self.assertEqual(denominator, 1000)
        self.assertFalse(errors)
        self.assertTrue(memory.validate_population(1000, 1, True, final(), 0)[1])
        self.assertTrue(memory.validate_population(1000, 0, True, final(192), 0)[1])

    def test_completed_workload_errors_and_retries_invalidate_the_sample(self):
        for name in ["client_errors", "connect_rejected", "forced_tasks", "panicked_tasks", "reconnects"]:
            with self.subTest(counter=name):
                self.assertTrue(memory.validate_population(1000, 1000, False, final(**{name: 1}), 0)[1])
        self.assertTrue(memory.validate_population(1000, 1000, False, None, 0)[1])
        self.assertTrue(memory.validate_population(1000, 1000, False, final(), -2)[1])
        self.assertTrue(memory.validate_population(1000, 1000, False, final(connect_attempts=1001), 0)[1])
        self.assertTrue(memory.validate_population(1000, 1000, False, final(disconnected=999), 0)[1])
        failed = final()
        failed["error_samples"] = ["CONNACK rejection"]
        self.assertTrue(memory.validate_population(1000, 1000, False, failed, 0)[1])

    def test_status_counts_support_current_and_legacy_device_only_shapes(self):
        self.assertEqual(memory.active_connection_count({"active_connections": {"mqtt": 192, "tcp": 0, "udp": 0}}), 192)
        self.assertEqual(memory.active_connection_count({"active_connections": [0, 1000, 0]}), 1000)


if __name__ == "__main__":
    unittest.main()
