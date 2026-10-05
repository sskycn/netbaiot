#!/usr/bin/env python3
"""Deterministic release evidence checks, with no broker scheduling assumptions."""
import json
import unittest
from unittest.mock import MagicMock, patch

import run


class KeepaliveEvidenceTests(unittest.TestCase):
    def observe(self, *, net_elapsed=1.5, reference_elapsed=2.5,
                net_error=None, reference_error=None, reference_connack_error=None):
        clients = [MagicMock(), MagicMock()]
        for client in clients:
            client.recv.return_value = (0x20, b"\x00\x00")
        clients[0].expect_closed.side_effect = net_error
        clients[1].expect_closed.side_effect = reference_error
        if reference_connack_error:
            clients[1].recv.side_effect = reference_connack_error
        clock = [0, 10]
        if net_error is None:
            clock += [10 + net_elapsed]
            if 1.0 <= net_elapsed <= 2.5 and reference_connack_error is None:
                clock += [20]
                if reference_error is None:
                    clock += [20 + reference_elapsed]
        clock += [30]
        results = run.Results("test", "DIFF-KEEPALIVE-001")
        with patch.object(run, "RawClient", side_effect=clients), patch.object(
            run.time, "monotonic", side_effect=clock
        ):
            run.differential_vectors(1, 2, results)
        clients[0].close.assert_called_once()
        return results, clients

    def test_reference_timeout_is_observable_without_failing_gate(self):
        results, clients = self.observe(reference_error=TimeoutError())
        item = results.items[0]
        self.assertEqual(item["result"], "REFERENCE_TIMEOUT")
        self.assertIn("4.5s", item["details"])
        self.assertIn("NetbaIoT met differential keepalive bound", item["details"])
        self.assertIn("equality unverified", item["details"])
        clients[0].expect_closed.assert_called_once_with(timeout=2.6)
        clients[1].expect_closed.assert_called_once_with(timeout=4.5)
        clients[1].close.assert_called_once()
        self.assertEqual(results.summary(), dict(total=1, **{"pass": 0}, reference_observations=1, fail=0))
        self.assertEqual(json.loads(json.dumps(results.items))[0]["result"], "REFERENCE_TIMEOUT")

    def test_netbaiot_timeout_and_timing_mismatch_still_fail(self):
        for arguments in [dict(net_error=TimeoutError()), dict(net_elapsed=2.51), dict(net_elapsed=0.1)]:
            with self.subTest(arguments=arguments):
                results, clients = self.observe(**arguments)
                self.assertEqual(results.items[0]["result"], "FAIL")
                self.assertEqual(results.summary()["fail"], 1)
                clients[1].expect_closed.assert_not_called()

    def test_reference_timing_difference_is_not_a_pass(self):
        for elapsed in [0.1, 4.2]:
            with self.subTest(elapsed=elapsed):
                results, _ = self.observe(reference_elapsed=elapsed)
                self.assertEqual(results.items[0]["result"], "REFERENCE_DIFFERENT")
                self.assertEqual(results.summary()["pass"], 0)
                self.assertEqual(results.summary()["fail"], 0)

    def test_normal_observations_pass(self):
        results, _ = self.observe()
        self.assertEqual(results.items[0]["result"], "PASS")
        self.assertEqual(results.summary()["reference_observations"], 0)

    def test_reference_protocol_error_and_handshake_timeout_still_fail(self):
        for arguments in [dict(reference_error=AssertionError("unexpected packet")),
                          dict(reference_connack_error=TimeoutError())]:
            with self.subTest(arguments=arguments):
                results, _ = self.observe(**arguments)
                self.assertEqual(results.items[0]["result"], "FAIL")
                self.assertEqual(results.summary()["fail"], 1)

    def test_reference_outcome_is_restricted_to_keepalive(self):
        for test_id in ["KEEPALIVE-001", "DIFF-CONNECT-001", "DIFF-QOS1-001"]:
            results = run.Results("test")
            results.run(test_id, "test", "test", lambda: run.ReferenceObservation("REFERENCE_TIMEOUT", "test"))
            self.assertEqual(results.items[0]["result"], "FAIL")
            self.assertFalse(run.release_evidence_satisfied(test_id, "REFERENCE_TIMEOUT"))
        for result in [None, "FAIL", "SKIPPED_REQUIRED", "REFERENCE_SKIPPED", "UNKNOWN"]:
            self.assertFalse(run.release_evidence_satisfied("DIFF-KEEPALIVE-001", result))

    def test_other_differential_mismatch_still_fails(self):
        clients = [MagicMock(), MagicMock()]
        clients[0].recv.return_value = (0x20, b"\x00\x00")
        clients[1].recv.return_value = (0x20, b"\x01\x00")
        results = run.Results("test", "DIFF-CONNECT-001")
        with patch.object(run, "RawClient", side_effect=clients):
            run.differential_vectors(1, 2, results)
        self.assertEqual(results.items[0]["result"], "FAIL")

    def test_shared_compare_still_rejects_session_mismatch(self):
        clients = [MagicMock() for _ in range(8)]
        for index, client in enumerate(clients):
            client.recv.return_value = (0x20, b"\x01\x00" if index == 1 else b"\x00\x00")
        results = run.Results("test", "DIFF-SESSION-001")
        with patch.object(run, "RawClient", side_effect=clients):
            run.differential_vectors(1, 2, results)
        self.assertEqual(results.items[0]["result"], "FAIL")
        self.assertEqual(results.summary()["fail"], 1)

    def test_normative_evidence_requires_literal_pass(self):
        catalog = json.loads((run.ROOT / "tests/mqtt_conformance/catalog.json").read_text())
        known_ids, _ = run.validate_catalog(catalog)
        by_id = dict.fromkeys(known_ids, "PASS")
        by_id["DIFF-KEEPALIVE-001"] = "REFERENCE_TIMEOUT"
        self.assertIn("125/125", run.normative_coverage(catalog, by_id))
        self.assertTrue(all(run.release_evidence_satisfied(test_id, by_id[test_id]) for test_id in known_ids))
        for result in [None, "FAIL", "REFERENCE_TIMEOUT"]:
            by_id["KEEPALIVE-001"] = result
            with self.assertRaises(AssertionError):
                run.normative_coverage(catalog, by_id)


if __name__ == "__main__":
    unittest.main()
