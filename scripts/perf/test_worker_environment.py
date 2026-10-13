#!/usr/bin/env python3
"""Parser and child-environment checks; no workload or child process is started."""

import argparse
import contextlib
import io
import os
import unittest
from unittest.mock import patch

import event_load
import mixed_load


class WorkerEnvironmentTests(unittest.TestCase):
    def parser(self, module):
        parser = argparse.ArgumentParser()
        module.add_server_workers_argument(parser)
        return parser

    def test_omitted_flag_keeps_existing_environment(self):
        for module in (event_load, mixed_load):
            with self.subTest(module=module.__name__):
                args = self.parser(module).parse_args([])
                self.assertIsNone(args.server_workers)
                for base in (
                    {"OTHER": "same"},
                    {"TOKIO_WORKER_THREADS": "10", "OTHER": "same"},
                ):
                    server = module.server_environment(base, args.server_workers)
                    self.assertEqual(server, base)
                    self.assertIsNot(server, base)

    def test_parser_accepts_only_bounded_integers(self):
        for module in (event_load, mixed_load):
            for value in ("1", "2", "4", "10", "64"):
                with self.subTest(module=module.__name__, valid=value):
                    args = self.parser(module).parse_args(["--server-workers", value])
                    self.assertEqual(args.server_workers, int(value))
            for value in ("0", "-1", "65", "1.5", "nan", "abc", ""):
                with self.subTest(module=module.__name__, invalid=value):
                    with contextlib.redirect_stderr(io.StringIO()):
                        with self.assertRaises(SystemExit) as error:
                            self.parser(module).parse_args(["--server-workers", value])
                    self.assertEqual(error.exception.code, 2)

    def test_override_does_not_change_parent_or_other_children(self):
        for module in (event_load, mixed_load):
            with self.subTest(module=module.__name__):
                with patch.dict(
                    os.environ,
                    {"TOKIO_WORKER_THREADS": "10", "OTHER": "same"},
                    clear=True,
                ):
                    before = os.environ.copy()
                    server = module.server_environment(os.environ, 2)
                    self.assertEqual(server["TOKIO_WORKER_THREADS"], "2")
                    self.assertEqual(os.environ.copy(), before)
                    self.assertEqual(server, {**before, "TOKIO_WORKER_THREADS": "2"})
                    # Loadgen and sink inherit this unchanged parent environment.
                    self.assertEqual(os.environ["TOKIO_WORKER_THREADS"], "10")

    def test_helper_rejects_out_of_range_overrides(self):
        base = {"TOKIO_WORKER_THREADS": "10"}
        for workers in (0, -1, 65, "invalid"):
            with self.subTest(workers=workers), self.assertRaises(argparse.ArgumentTypeError):
                event_load.server_environment(base, workers)
            self.assertEqual(base, {"TOKIO_WORKER_THREADS": "10"})


if __name__ == "__main__":
    unittest.main()
