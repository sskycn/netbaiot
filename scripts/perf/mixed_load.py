#!/usr/bin/env python3
"""Run the bounded 1,000-device mixed MQTT baseline against a real webhook."""

import argparse
import json
import os
import signal
import subprocess
import tempfile
import time

from connection_memory import fd_count, free_port, rss_kib, status
from event_load import ROOT, credential, last_json, management_get


def process_cpu(process):
    if process.poll() is not None:
        return 0.0
    try:
        return float(
            subprocess.check_output(
                ["ps", "-o", "%cpu=", "-p", str(process.pid)], text=True
            ).strip()
        )
    except (subprocess.CalledProcessError, ValueError):
        return 0.0


def burst_phases(duration):
    """Alternate quiet/burst intervals while staying within the 32-phase limit."""
    interval = max(5.0, duration / 30.0)
    phases = []
    remaining = duration
    quiet = True
    while remaining > 0 and len(phases) < 32:
        seconds = min(interval, remaining)
        phases.append({"seconds": seconds, "rate": 0.0 if quiet else 200.0})
        remaining -= seconds
        quiet = not quiet
    if remaining > 0:
        phases[-1]["seconds"] += remaining
    return phases


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=1800)
    parser.add_argument("--warmup", type=float, default=30)
    parser.add_argument("--cooldown", type=float, default=10)
    parser.add_argument("--sample-every", type=float, default=5)
    parser.add_argument("--profile-output")
    parser.add_argument("--profile-seconds", type=int, default=10)
    args = parser.parse_args()
    if args.duration <= 0 or args.duration > 14400:
        raise SystemExit("duration must be in (0, 14400]")
    if (
        args.warmup < 0
        or args.cooldown < 0
        or args.sample_every <= 0
        or args.profile_seconds <= 0
    ):
        raise SystemExit("warmup/cooldown must be nonnegative and sample-every positive")

    device_http, management, mqtt, tcp, udp, sink_port = [free_port() for _ in range(6)]
    maximum = 1100
    limits = {
        "max_connections": maximum,
        "max_connections_per_ip": maximum,
        "max_connections_per_tenant": maximum,
        "max_devices": maximum,
        "max_devices_per_tenant": maximum,
        "max_persistent_sessions": maximum,
        "max_persistent_sessions_per_tenant": maximum,
        "max_subscriptions": maximum * 2,
        "max_subscriptions_per_tenant": maximum * 2,
        "auth_cache_max_entries": maximum,
        "auth_cache_max_bytes": 64 * 1024 * 1024,
        "rate_entries": maximum * 4,
        "requests_per_second": 1_000_000,
        "requests_per_ip_second": 1_000_000,
        "messages_per_device_second": 1_000_000,
        "messages_per_tenant_second": 1_000_000,
        "global_connection_logical_bytes": maximum * 524_288,
        "max_ingress": maximum,
        "max_ingress_per_tenant": maximum,
        "max_ingress_per_device": 4,
        "sink_queue_max_count": 50_000,
        "sink_queue_max_bytes": 64 * 1024 * 1024,
        "global_event_max_count": 50_000,
        "global_event_max_bytes": 64 * 1024 * 1024,
        "sink_delivery_concurrency": 8,
        "mqtt_recovery_max_bytes": 512 * 1024 * 1024,
    }
    # Device mix: 70% idle, 20% at 1 msg/s, 9% at 10 msg/s, 1% bursty.
    # Message mix at average burst rate: QoS0=720/s, QoS1=360/s, QoS2=120/s.
    profiles = [
        ("idle", 0, 699, 0, 0.0, False, 0.0, None),
        ("command-idle", 699, 1, 0, 0.0, False, 0.1, None),
        ("slow-q0", 700, 200, 0, 200.0, False, 0.0, None),
        ("medium-q0", 900, 52, 0, 520.0, False, 0.0, None),
        ("medium-q1", 952, 36, 1, 360.0, True, 0.0, None),
        ("medium-q2", 988, 2, 2, 20.0, False, 0.0, None),
        ("bursty-q2", 990, 10, 2, 0.0, False, 0.0, burst_phases(args.duration)),
    ]

    with tempfile.TemporaryDirectory(prefix="netbaiot-mixed-") as temporary:
        control = os.path.join(temporary, "sink.json")
        server_config = os.path.join(temporary, "server.json")
        with open(control, "w", encoding="utf-8") as output:
            json.dump({"delay": 0, "status": 204}, output)
        with open(server_config, "w", encoding="utf-8") as output:
            json.dump(
                {
                    "device_http": f"127.0.0.1:{device_http}",
                    "management_http": f"127.0.0.1:{management}",
                    "mqtt": f"127.0.0.1:{mqtt}",
                    "tcp": f"127.0.0.1:{tcp}",
                    "udp": f"127.0.0.1:{udp}",
                    "business_tcp": None,
                    "development": True,
                    "limits": limits,
                    "credentials": [credential(index) for index in range(1000)],
                    "tls": None,
                    "delivery_url": f"http://127.0.0.1:{sink_port}/events",
                    "auth_provider_url": None,
                    "spool_directory": os.path.join(temporary, "spool"),
                    "device_configs": [],
                },
                output,
            )
        workload_paths = []
        for name, offset, connections, qos, rate, reconnect, command_rate, phases in profiles:
            path = os.path.join(temporary, f"{name}.json")
            workload = {
                "transport": "mqtt",
                "address": f"127.0.0.1:{mqtt}",
                "management_url": f"http://127.0.0.1:{management}",
                "connections": connections,
                "offset": offset,
                "tenant_width": 1001,
                "ramp_per_sec": min(100, connections),
                "warmup_secs": args.warmup,
                "duration_secs": args.duration,
                "cooldown_secs": args.cooldown,
                "publish_rate": rate,
                "payload_bytes": 256,
                "qos": qos,
                "subscribe": False,
                "mqtt_clean_session": not reconnect,
                "reconnect_every_secs": 60.0 if reconnect else 0.0,
                "reconnect_fraction": 0.1 if reconnect else 1.0,
                "command_rate": command_rate,
                "command_concurrency": 1,
                "report_every_secs": 10,
            }
            if phases is not None:
                workload["phases"] = phases
            with open(path, "w", encoding="utf-8") as output:
                json.dump(workload, output)
            workload_paths.append((name, path))

        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = "ab" * 32
        environment["NETBAIOT_PERF_LOCK_METRICS"] = "1"
        environment["NO_PROXY"] = "127.0.0.1,localhost"
        environment["no_proxy"] = "127.0.0.1,localhost"
        sink = subprocess.Popen(
            ["python3", os.path.join(ROOT, "scripts/perf/sink.py"), str(sink_port), control],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        server = None
        profiler = None
        loads = []
        try:
            if not sink.stdout.readline():
                raise RuntimeError("sink did not start")
            server = subprocess.Popen(
                [os.path.join(ROOT, "target/release/netbaiot-server"), server_config],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
            )
            deadline = time.time() + 10
            while True:
                try:
                    status(management, False)
                    break
                except Exception:
                    if server.poll() is not None or time.time() > deadline:
                        raise RuntimeError("server did not start")
                    time.sleep(0.05)
            for name, path in workload_paths:
                load_output = open(
                    os.path.join(temporary, f"{name}-output.jsonl"),
                    "w+",
                    encoding="utf-8",
                )
                loads.append(
                    (
                        name,
                        subprocess.Popen(
                            [os.path.join(ROOT, "target/release/netbaiot-loadgen"), path],
                            stdout=load_output,
                            stderr=subprocess.PIPE,
                            text=True,
                        ),
                        load_output,
                    )
                )
            samples = []
            profile_at = time.time() + 7 + args.warmup + 5
            while any(process.poll() is None for _, process, _ in loads):
                if args.profile_output and profiler is None and time.time() >= profile_at:
                    profiler = subprocess.Popen(
                        [
                            "sample",
                            str(server.pid),
                            str(args.profile_seconds),
                            "-file",
                            args.profile_output,
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                state = status(management, False)
                samples.append(
                    {
                        "elapsed_seconds": len(samples) * args.sample_every,
                        "event_count": state["event_count"],
                        "event_bytes": state["event_bytes"],
                        "pending_required": state["pending_required"],
                        "rss_kib": rss_kib(server.pid),
                        "fds": fd_count(server.pid),
                        "tasks": state["runtime_tasks"],
                        "server_cpu_percent": process_cpu(server),
                        "loadgen_cpu_percent": sum(process_cpu(p) for _, p, _ in loads),
                        "sink_cpu_percent": process_cpu(sink),
                    }
                )
                time.sleep(args.sample_every)
            load_results = {}
            for name, process, load_output in loads:
                _, stderr = process.communicate(timeout=10)
                load_output.flush()
                load_output.seek(0)
                stdout = load_output.read()
                load_output.close()
                load_results[name] = {
                    "final": last_json(stdout, "final"),
                    "stderr": stderr[-512:],
                    "exit": process.returncode,
                }
            profile_stderr = ""
            if profiler is not None:
                _, profile_stderr = profiler.communicate(timeout=args.profile_seconds + 10)
            metrics = management_get(management, "/api/v1/metrics", False)
            server.send_signal(signal.SIGTERM)
            server.wait(timeout=30)
            sink.send_signal(signal.SIGTERM)
            sink_out, sink_err = sink.communicate(timeout=10)
            print(
                json.dumps(
                    {
                        "duration_seconds": args.duration,
                        "warmup_seconds": args.warmup,
                        "cooldown_seconds": args.cooldown,
                        "connections": 1000,
                        "nominal_message_mix": {"qos0": 0.6, "qos1": 0.3, "qos2": 0.1},
                        "profiles": load_results,
                        "profile_stderr": profile_stderr[-512:],
                        "sink": last_json(sink_out),
                        "sink_stderr": sink_err[-512:],
                        "samples": samples,
                        "metrics": metrics,
                        "server_exit": server.returncode,
                    },
                    sort_keys=True,
                )
            )
        finally:
            for _, process, load_output in loads:
                if process.poll() is None:
                    process.kill()
                    process.wait()
                if not load_output.closed:
                    load_output.close()
            for process in (server, sink):
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait()
            if profiler is not None and profiler.poll() is None:
                profiler.kill()
                profiler.wait()


if __name__ == "__main__":
    main()
