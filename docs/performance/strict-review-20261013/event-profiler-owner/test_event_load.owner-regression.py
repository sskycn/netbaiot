#!/usr/bin/env python3
"""Run the real measurement main with fake owned processes; no network or child starts."""

import contextlib
import io
import json
import subprocess
import unittest
from unittest.mock import patch

import event_load


class OwnedProcess:
    def __init__(self, name, pid, events, output=""):
        self.name = name
        self.pid = pid
        self.events = events
        self.stdout = io.StringIO("ready\n" if name == "sink" else output)
        self.stderr = io.StringIO()
        self.output = output
        self.returncode = None
        self.load_polls = 0
        self.killed = False
        self.signals = []

    def poll(self):
        if self.name == "loadgen" and self.returncode is None:
            self.load_polls += 1
            if self.load_polls == 2:
                self.returncode = 0
        return self.returncode

    def communicate(self, timeout=None):
        self.events.append(f"{self.name}.communicate")
        self.returncode = 0
        return self.output, ""

    def send_signal(self, value):
        self.events.append(f"{self.name}.signal")
        self.signals.append(value)

    def wait(self, timeout=None):
        self.events.append(f"{self.name}.wait")
        if self.returncode is None:
            self.returncode = 0
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9


class EventLoadCpuTests(unittest.TestCase):
    def run_main(self, profiled, fail_cpu_after=False):
        events = []
        processes = {
            "sink": OwnedProcess("sink", 101, events, '{"confirmed":1}\n'),
            "server": OwnedProcess("server", 102, events),
            "loadgen": OwnedProcess("loadgen", 103, events, '{"event":"final","stats":{"published":1}}\n'),
            "profiler": OwnedProcess("profiler", 104, events),
        }

        def start(arguments, **kwargs):
            name = {
                "python3": "sink", "fake-server": "server",
                "fake-loadgen": "loadgen", "sample": "profiler",
            }[arguments[0]]
            if name == "profiler":
                self.assertTrue(profiled)
            events.append(f"{name}.start")
            return processes[name]

        readings = iter((100.0, 105.0))

        def process_cpu(pid):
            self.assertEqual(pid, processes["server"].pid)
            value = next(readings)
            if value == 105.0 and fail_cpu_after:
                events.append("server.cpu=error")
                raise subprocess.CalledProcessError(1, ["ps", "-o", "time=", "-p", str(pid)])
            events.append(f"server.cpu={value}")
            return value

        def legacy_cpu(pid):
            self.assertEqual(pid, processes["server"].pid)
            events.append("server.legacy_cpu=108.0")
            return 108.0

        def metrics(*args):
            events.append("metrics")
            return "fixture_metrics 1\n"

        arguments = [
            "event_load.py", "--rate", "1", "--duration", "20",
            "--connections", "1", "--warmup", "2", "--cooldown", "3",
            "--server-bin", "fake-server", "--loadgen-bin", "fake-loadgen",
        ]
        if profiled:
            arguments.extend(("--sample-output", "fake-sample.txt", "--sample-seconds", "1"))
        output = io.StringIO()
        with contextlib.ExitStack() as stack:
            stack.enter_context(patch("sys.argv", arguments))
            start_mock = stack.enter_context(patch.object(event_load.subprocess, "Popen", side_effect=start))
            stack.enter_context(patch.object(event_load.subprocess, "check_output", return_value="0.0"))
            stack.enter_context(patch.object(event_load, "free_ports", return_value=(11111, 11112, 11113)))
            stack.enter_context(patch.object(event_load, "status", return_value={
                "event_count": 0, "event_bytes": 0, "pending_required": 0, "runtime_tasks": 4,
            }))
            stack.enter_context(patch.object(event_load, "rss_kib", return_value=1024))
            stack.enter_context(patch.object(event_load, "fd_count", return_value=8))
            cpu_mock = stack.enter_context(patch.object(event_load, "process_cpu_seconds", side_effect=process_cpu))
            legacy_mock = stack.enter_context(patch.object(event_load, "cpu_seconds", side_effect=legacy_cpu))
            stack.enter_context(patch.object(event_load, "management_get", side_effect=metrics))
            stack.enter_context(patch.object(event_load.time, "monotonic", side_effect=(10.0, 11.0, 35.0)))
            stack.enter_context(patch.object(event_load.time, "time", side_effect=(100.0, 200.0, 300.0)))
            stack.enter_context(patch.object(event_load.time, "sleep"))
            stack.enter_context(contextlib.redirect_stdout(output))
            if fail_cpu_after:
                with self.assertRaises(subprocess.CalledProcessError):
                    event_load.main()
            else:
                event_load.main()

        result = json.loads(output.getvalue())
        if fail_cpu_after:
            self.assertIn("setup_failure", result)
            self.assertNotIn("server_cpu_load_seconds", result)
            self.assertEqual(start_mock.call_count, 4)
            self.assertEqual(cpu_mock.call_count, 2)
            legacy_mock.assert_not_called()
            self.assertIn("profiler.start", events)
            self.assertLess(events.index("loadgen.communicate"), events.index("server.cpu=error"))
            self.assertNotIn("profiler.communicate", events)
            profiler = processes["profiler"]
            print(f"profiler_error_cleanup returncode={profiler.returncode} killed={profiler.killed}")
            self.assertTrue(
                profiler.killed,
                f"profiler remained alive after CPU error: returncode={profiler.returncode}, killed={profiler.killed}",
            )
            self.assertEqual(profiler.returncode, -9)
            for name in ("server", "sink"):
                self.assertTrue(processes[name].killed)
                self.assertEqual(processes[name].returncode, -9)
            self.assertFalse(processes["loadgen"].killed)
            self.assertEqual(processes["loadgen"].returncode, 0)
            return events, processes
        self.assertEqual(start_mock.call_count, 4 if profiled else 3)
        self.assertEqual(cpu_mock.call_count, 2)
        legacy_mock.assert_called_once_with(processes["server"].pid)
        self.assertEqual(result["server_cpu_seconds"], 108.0)
        self.assertEqual(result.get("server_cpu_load_seconds"), 5.0)
        self.assertEqual(result["server_cpu_load_before_seconds"], 100.0)
        self.assertEqual(result["server_cpu_load_after_seconds"], 105.0)
        self.assertEqual(result["server_cpu_load_started_monotonic_seconds"], 10.0)
        self.assertEqual(result["server_cpu_load_finished_monotonic_seconds"], 35.0)
        self.assertEqual(result["server_cpu_load_wall_seconds"], 25.0)
        self.assertEqual(result["duration_seconds"], 20.0)
        self.assertEqual(result["warmup_seconds"], 2.0)
        self.assertEqual(result["cooldown_seconds"], 3.0)
        self.assertEqual(result["server_cpu_load_scope"], "loadgen_lifecycle_including_ramp_warmup_and_cooldown")
        self.assertEqual(result["load_exit"], 0)
        self.assertEqual(result["server_exit"], 0)
        self.assertEqual(len(result["samples"]), 1)
        for name in ("loadgen", "server", "sink"):
            self.assertEqual(processes[name].returncode, 0)
            self.assertFalse(processes[name].killed)
        self.assertTrue(processes["server"].signals)
        self.assertTrue(processes["sink"].signals)
        self.assertLess(events.index("server.cpu=100.0"), events.index("loadgen.start"))
        self.assertLess(events.index("loadgen.communicate"), events.index("server.cpu=105.0"))
        self.assertLess(events.index("server.cpu=105.0"), events.index("metrics"))
        self.assertLess(events.index("metrics"), events.index("server.legacy_cpu=108.0"))
        return events, processes

    def test_main_keeps_cumulative_cpu_and_reports_the_full_loadgen_delta(self):
        events, _ = self.run_main(profiled=False)
        self.assertNotIn("profiler.start", events)

    def test_main_excludes_profiler_tail_cpu_from_the_loadgen_delta(self):
        events, processes = self.run_main(profiled=True)
        self.assertLess(events.index("server.cpu=105.0"), events.index("profiler.communicate"))
        self.assertLess(events.index("profiler.communicate"), events.index("server.legacy_cpu=108.0"))
        self.assertEqual(processes["profiler"].returncode, 0)
        self.assertFalse(processes["profiler"].killed)

    def test_main_reaps_the_running_profiler_when_post_load_cpu_read_fails(self):
        self.run_main(profiled=True, fail_cpu_after=True)


if __name__ == "__main__":
    unittest.main()
