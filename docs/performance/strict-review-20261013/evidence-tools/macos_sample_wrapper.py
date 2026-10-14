#!/usr/bin/env python3
"""Run the unchanged memory harness with a separate macOS thread-stack sample.

This is a profiling run. It does not participate in the quiet density A/B.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def option(arguments, name, default=None):
    if name not in arguments:
        return default
    position = arguments.index(name)
    if position + 1 >= len(arguments):
        raise SystemExit(f"missing value for {name}")
    return arguments[position + 1]


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--phase", choices=["heartbeat", "disconnect"], required=True)
arguments, harness_arguments = parser.parse_known_args()
arguments.repo = arguments.repo.resolve()
arguments.output = arguments.output.resolve()
if harness_arguments and harness_arguments[0] == "--":
    harness_arguments = harness_arguments[1:]
if sys.platform != "darwin":
    raise SystemExit("this wrapper requires macOS /usr/bin/sample")
if arguments.output.exists():
    raise SystemExit("output already exists; preserve previous evidence")
if "--density" not in harness_arguments:
    raise SystemExit("the bounded density load generator is required")
expected_connections = int(option(harness_arguments, "--connections", "0"))
hold_seconds = float(option(harness_arguments, "--hold-seconds", "0"))
if expected_connections != 10000 or hold_seconds != 20:
    raise SystemExit("this isolated probe requires 10000 connections and hold-seconds 20")
if option(harness_arguments, "--transport") != "mqtt":
    raise SystemExit("this isolated heartbeat/disconnect probe requires MQTT")
if option(harness_arguments, "--ping-seconds") != "2":
    raise SystemExit("this isolated probe requires ping-seconds 2")
if arguments.phase == "disconnect":
    if "--disconnected" not in harness_arguments or "--persistent" not in harness_arguments:
        raise SystemExit("disconnect phase requires --persistent --disconnected")
elif "--disconnected" in harness_arguments:
    raise SystemExit("heartbeat phase must sample held active connections")

arguments.output.mkdir(parents=True)
sys.path.insert(0, str(arguments.repo / "scripts/perf"))
import connection_memory as harness

original_rss = harness.rss_kib
original_status = harness.status
original_finish = harness.finish_load
original_popen = subprocess.Popen
duration_seconds = 10
records = {
    "phase": arguments.phase,
    "prepared_or_run_utc": utc_now(),
    "harness_source_sha256": hashlib.sha256(
        (arguments.repo / "scripts/perf/connection_memory.py").read_bytes()
    ).hexdigest(),
    "wrapper_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    "notes": [
        "Independent profiling workload; do not combine with quiet A/B timing results.",
        "macOS sample records wall-clock thread stacks. Blocked-stack sample share is not CPU utilization.",
        "Configured credentials establish one synthetic tenant; active population is separately measured through management status.",
        "No private quota balance, malloc usable size, per-task allocation, or kernel socket byte count is exported by this wrapper.",
        "The repository memory harness and server/runtime configuration algorithms are unchanged.",
    ],
    "status_snapshots": [],
    "rss_snapshots": [],
}
endpoint = None
latest_status = None
server_pid = None
rss_calls = 0
load_started = None
load_configuration = None
sampler = None
sampler_log = None
sampling_started = None


def record_status(port, tls):
    global endpoint, latest_status
    result = original_status(port, tls)
    endpoint = (port, tls)
    latest_status = result
    if "baseline_runtime_tasks" not in records:
        records["baseline_runtime_tasks"] = result["runtime_tasks"]
    if len(records["status_snapshots"]) >= 256:
        raise RuntimeError("bounded supplemental status sample limit exceeded")
    records["status_snapshots"].append({
        "monotonic_seconds": time.monotonic(),
        "utc": utc_now(),
        "active_connections": harness.active_connection_count(result),
        "status": result,
    })
    return result


def observe_status():
    if endpoint is None:
        raise RuntimeError("management readiness must be observed before profiling")
    return record_status(*endpoint)


def start_sample():
    global sampler, sampler_log, sampling_started
    if sampler is not None:
        raise RuntimeError("sample already started")
    measured = observe_status()
    measured_active = harness.active_connection_count(measured)
    if measured_active != expected_connections:
        raise RuntimeError(f"sample requires 10000 online connections; measured {measured_active}")
    command = ["/usr/bin/sample", str(server_pid), str(duration_seconds), "1", "-mayDie",
               "-file", str(arguments.output / "server-stacks.txt")]
    sampler_log = (arguments.output / "sample-command.log").open("w", encoding="utf-8")
    sampling_started = time.monotonic()
    sampler = original_popen(command, stdout=sampler_log, stderr=subprocess.STDOUT, text=True)
    records["sample"] = {"command": command, "server_pid": server_pid,
                         "started_monotonic_seconds": sampling_started,
                         "started_utc": utc_now(), "measured_active_at_start": measured_active}


def finish_sample():
    if sampler is None:
        raise RuntimeError("sample never started")
    remaining = max(1, duration_seconds + 10 - (time.monotonic() - sampling_started))
    code = sampler.wait(timeout=remaining)
    records["sample"].update({"returncode": code,
                              "finished_monotonic_seconds": time.monotonic(),
                              "finished_utc": utc_now()})
    sampler_log.flush()
    output = arguments.output / "server-stacks.txt"
    records["sample"]["output_exists"] = output.is_file()
    if output.is_file():
        records["sample"]["output_bytes"] = output.stat().st_size
        records["sample"]["output_sha256"] = hashlib.sha256(output.read_bytes()).hexdigest()
    if code != 0 or not output.is_file() or output.stat().st_size == 0:
        raise RuntimeError(f"macOS sample failed with exit {code}; preserve command log")


def await_connection_cleanup():
    deadline = time.monotonic() + 10
    while True:
        snapshot = observe_status()
        active = harness.active_connection_count(snapshot)
        tasks = snapshot["runtime_tasks"]
        if active == 0 and tasks == records["baseline_runtime_tasks"]:
            records["post_client_cleanup"] = {
                "measurement_valid": True,
                "monotonic_seconds": time.monotonic(),
                "active_connections": active,
                "runtime_tasks": tasks,
                "baseline_runtime_tasks": records["baseline_runtime_tasks"],
                "note": "Public API observes connection/task cleanup; private byte quota is not exported."}
            return
        if time.monotonic() >= deadline:
            records["post_client_cleanup"] = {
                "measurement_valid": False,
                "active_connections": active,
                "runtime_tasks": tasks,
                "baseline_runtime_tasks": records["baseline_runtime_tasks"]}
            raise RuntimeError(f"post-client cleanup deadline: active={active}, tasks={tasks}, baseline={records['baseline_runtime_tasks']}")
        time.sleep(0.1)


def hooked_popen(command, *positional, **keyword):
    global load_started, load_configuration
    process = original_popen(command, *positional, **keyword)
    if isinstance(command, list) and "--density" in command:
        load_started = time.monotonic()
        load_configuration = json.loads(Path(command[-1]).read_text())
        records["load"] = {"pid": process.pid, "started_monotonic_seconds": load_started,
                           "started_utc": utc_now(), "configuration": load_configuration}
    else:
        # This hook only wraps the harness's direct server/load Popen calls.
        config = json.loads(Path(command[-1]).read_text())
        credentials = config.get("credentials", [])
        tenants = sorted({entry["identity"]["device_key"]["tenant_id"] for entry in credentials})
        records["configured_identity_population"] = {
            "credentials_count": len(credentials), "tenant_ids": tenants,
            "classification": "generated input provenance; not a measured population or capacity"}
        if len(credentials) != 10000 or tenants != ["t0"]:
            process.terminate()
            process.wait(timeout=5)
            raise RuntimeError("expected one synthetic tenant t0 with 10000 unique credentials")
    return process


def rss_with_sample(pid):
    global server_pid, rss_calls
    server_pid = pid
    rss_calls += 1
    value = original_rss(pid)
    records["rss_snapshots"].append({"pid": pid, "monotonic_seconds": time.monotonic(),
                                     "rss_kib": value, "ordinal": rss_calls})
    if arguments.phase == "heartbeat" and rss_calls == 2:
        # Loaded status has already been fetched by the unchanged harness.
        if latest_status is None or harness.active_connection_count(latest_status) != 10000:
            raise RuntimeError("the second RSS snapshot does not have 10000 verified online connections")
        start_sample()
    return value


def finish_load_with_sample(load, timeout):
    if arguments.phase == "heartbeat":
        result = original_finish(load, timeout)
        records["load_final"] = harness.parse_load_final(result[0])
        await_connection_cleanup()
        finish_sample()
        return result
    if load_started is None or load_configuration is None:
        raise RuntimeError("the density load launch was not observed")
    ramp_seconds = load_configuration["connections"] / load_configuration["ramp_per_sec"]
    completion_deadline = time.monotonic() + timeout
    online_deadline = load_started + ramp_seconds + 12
    while True:
        active = harness.active_connection_count(observe_status())
        if active == 10000:
            records["verified_online_monotonic_seconds"] = time.monotonic()
            break
        if load.poll() is not None or time.monotonic() >= online_deadline:
            raise RuntimeError(f"disconnect probe failed to establish 10000 online; measured {active}")
        time.sleep(0.5)
    # density.rs has one common end = start + ramp + hold for every owned socket.
    # Start two seconds before that estimated end; actual zero and final counters
    # below determine whether the sample really crossed the disconnect phase.
    planned_start = load_started + ramp_seconds + load_configuration["hold_secs"] - 2
    records["planned_disconnect_sample_start_monotonic_seconds"] = planned_start
    while time.monotonic() < planned_start:
        if load.poll() is not None:
            raise RuntimeError("load completed before the planned disconnect sample")
        time.sleep(min(0.2, max(0.001, planned_start - time.monotonic())))
    start_sample()
    result = original_finish(load, max(1, completion_deadline - time.monotonic()))
    records["load_final"] = harness.parse_load_final(result[0])
    await_connection_cleanup()
    records["verified_disconnected_monotonic_seconds"] = records["post_client_cleanup"]["monotonic_seconds"]
    finish_sample()
    records["active_after_sample"] = harness.active_connection_count(observe_status())
    if records["verified_disconnected_monotonic_seconds"] >= records["sample"]["finished_monotonic_seconds"]:
        raise RuntimeError("sample did not overlap measured connection cleanup")
    return result


# Proxy only this harness module, leaving the wrapper's native subprocess object
# and the Linux helper module unchanged.
harness.subprocess = SimpleNamespace(**vars(subprocess))
harness.subprocess.Popen = hooked_popen
harness.status = record_status
harness.rss_kib = rss_with_sample
harness.finish_load = finish_load_with_sample
sys.argv = [str(arguments.repo / "scripts/perf/connection_memory.py"), *harness_arguments]
try:
    harness.main()
except BaseException as error:
    records["error"] = {"type": type(error).__name__, "text": str(error)}
    raise
finally:
    if sampler is not None and sampler.poll() is None:
        sampler.terminate()
        try:
            sampler.wait(timeout=2)
        except subprocess.TimeoutExpired:
            sampler.kill()
            sampler.wait(timeout=2)
        records.setdefault("sample", {})["interrupted_returncode"] = sampler.returncode
    if sampler_log is not None:
        sampler_log.close()
    records["finished_utc"] = utc_now()
    (arguments.output / "profile-metadata.json").write_text(json.dumps(records, indent=2) + "\n")
