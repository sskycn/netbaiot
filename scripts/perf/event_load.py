#!/usr/bin/env python3
"""Run a bounded real MQTT -> EventAccepted -> confirmed webhook measurement."""

import argparse
import json
import os
import signal
import socket
import ssl
import subprocess
import tempfile
import time

from connection_memory import SECRET, fd_count, free_port, rss_kib, status


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))


def credential(index):
    return {
        "credential_id": f"a{index}",
        "secret_hex": SECRET,
        "identity": {
            "device_key": {"tenant_id": "t0", "product_id": "p", "device_id": f"d{index}"},
            "credential_version": 1,
            "auth_generation": 1,
            "codec_id": "netbaiot-json",
            "codec_version": 1,
            "permissions": {"publish": True, "commands": True},
        },
    }


def last_json(output, event=None):
    values = []
    for line in output.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event is None or value.get("event") == event:
            values.append(value)
    return values[-1] if values else None


def cpu_seconds(pid):
    """Cumulative process CPU time; includes setup and warmup, excludes shutdown."""
    value = subprocess.check_output(
        ["ps", "-o", "time=", "-p", str(pid)], text=True
    ).strip()
    days, _, clock = value.rpartition("-")
    parts = [float(part) for part in clock.split(":")]
    seconds = 0.0
    for part in parts:
        seconds = seconds * 60 + part
    return seconds + (int(days) * 86400 if days else 0)


def management_get(port, path, tls=False):
    stream = socket.create_connection(("127.0.0.1", port), timeout=3)
    if tls:
        stream = ssl._create_unverified_context().wrap_socket(stream, server_hostname="localhost")
    request = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1:{port}\r\n"
        f"Authorization: Bearer {'ab' * 32}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    stream.sendall(request)
    response = bytearray()
    while True:
        chunk = stream.recv(65536)
        if not chunk:
            break
        response.extend(chunk)
    stream.close()
    head, separator, body = bytes(response).partition(b"\r\n\r\n")
    if not separator or b" 200 " not in head.split(b"\r\n", 1)[0]:
        raise RuntimeError(f"management response: {response[:256]!r}")
    return body.decode()


def process_cpu_seconds(pid):
    """Cumulative process user+system CPU from ps, independent of sample shares."""
    value = subprocess.check_output(["ps", "-o", "time=", "-p", str(pid)], text=True).strip()
    days = 0
    if "-" in value:
        prefix, value = value.split("-", 1)
        days = int(prefix)
    seconds = 0.0
    for component in value.split(":"):
        seconds = seconds * 60 + float(component)
    return days * 86400 + seconds


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, required=True)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--sink-delay-ms", type=float, default=0)
    parser.add_argument("--sink-mode", choices=("none", "webhook"), default="webhook")
    parser.add_argument("--qos", type=int, choices=(0, 1, 2), default=1)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--warmup", type=float, default=2)
    parser.add_argument("--cooldown", type=float, default=2)
    parser.add_argument("--sample-output")
    parser.add_argument("--sample-seconds", type=int, default=10)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument(
        "--server-bin", default=os.path.join(ROOT, "target/release/netbaiot-server")
    )
    parser.add_argument(
        "--loadgen-bin", default=os.path.join(ROOT, "target/release/netbaiot-loadgen")
    )
    args = parser.parse_args()
    ports = [free_port() for _ in range(6)]
    device_http, management, mqtt, tcp, udp, sink_port = ports
    maximum = max(128, args.connections + 16)
    limits = {
        "max_connections": maximum,
        "max_connections_per_ip": maximum,
        "max_connections_per_tenant": maximum,
        "max_devices": maximum,
        "max_devices_per_tenant": maximum,
        "max_persistent_sessions": maximum,
        "max_persistent_sessions_per_tenant": maximum,
        "auth_cache_max_entries": maximum,
        "auth_cache_max_bytes": 64 * 1024 * 1024,
        "rate_entries": maximum,
        "requests_per_second": 1000000,
        "requests_per_ip_second": 1000000,
        "messages_per_device_second": 1000000,
        "messages_per_tenant_second": 1000000,
        "global_connection_logical_bytes": maximum * 524288,
        "max_ingress": maximum,
        "max_ingress_per_tenant": maximum,
        "max_ingress_per_device": 4,
        "sink_queue_max_count": 50000,
        "sink_queue_max_bytes": 64 * 1024 * 1024,
        "global_event_max_count": 50000,
        "global_event_max_bytes": 64 * 1024 * 1024,
        "sink_delivery_concurrency": 8,
        # Scaling admitted MQTT sessions also scales the validated maximum
        # recovery image. This is a ceiling; the benchmark does not allocate it.
        "mqtt_recovery_max_bytes": 512 * 1024 * 1024,
    }
    with tempfile.TemporaryDirectory(prefix="netbaiot-load-") as temporary:
        control = os.path.join(temporary, "sink.json")
        server_config = os.path.join(temporary, "server.json")
        load_config = os.path.join(temporary, "load.json")
        with open(control, "w", encoding="utf-8") as output:
            json.dump({"delay": args.sink_delay_ms / 1000, "status": 204}, output)
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
                    "credentials": [credential(i) for i in range(args.connections)],
                    "tls": (
                        {
                            "certificate": os.path.join(ROOT, "tests/fixtures/localhost-cert.pem"),
                            "private_key": os.path.join(ROOT, "tests/fixtures/localhost-key.pem"),
                        }
                        if args.tls
                        else None
                    ),
                    "delivery_url": (
                        f"http://127.0.0.1:{sink_port}/events"
                        if args.sink_mode == "webhook"
                        else None
                    ),
                    "auth_provider_url": None,
                    "spool_directory": os.path.join(temporary, "spool"),
                    "device_configs": [],
                },
                output,
            )
        with open(load_config, "w", encoding="utf-8") as output:
            workload = {
                "transport": "mqtt",
                "address": f"127.0.0.1:{mqtt}",
                "connections": args.connections,
                "tenant_width": args.connections + 1,
                # Stay below the host's small listen backlog when publisher
                # counts are large; connection-ramp loss is not publish load.
                "ramp_per_sec": min(200, args.connections),
                "warmup_secs": args.warmup,
                "duration_secs": args.duration,
                "cooldown_secs": args.cooldown,
                "publish_rate": args.rate,
                "payload_bytes": args.payload_bytes,
                "qos": args.qos,
                "subscribe": False,
                "report_every_secs": 5,
            }
            if args.tls:
                workload["tls_ca"] = os.path.join(ROOT, "tests/fixtures/localhost-cert.pem")
            json.dump(workload, output)
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
        server = load = None
        try:
            ready = sink.stdout.readline()
            if not ready:
                raise RuntimeError("sink did not start")
            server = subprocess.Popen(
                [args.server_bin, server_config],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                text=True,
                env=environment,
            )
            deadline = time.time() + 10
            while True:
                try:
                    status(management, args.tls)
                    break
                except Exception:
                    if server.poll() is not None or time.time() > deadline:
                        raise RuntimeError("server did not start")
                    time.sleep(.05)
            server_cpu_before = process_cpu_seconds(server.pid)
            load = subprocess.Popen(
                [args.loadgen_bin, load_config],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            samples = []
            profiler = None
            profile_at = time.time() + args.warmup + 1
            while load.poll() is None:
                if args.sample_output and profiler is None and time.time() >= profile_at:
                    profiler = subprocess.Popen(
                        [
                            "sample",
                            str(server.pid),
                            str(args.sample_seconds),
                            "-file",
                            args.sample_output,
                        ],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                state = status(management, args.tls)
                samples.append(
                    {
                        "event_count": state["event_count"],
                        "event_bytes": state["event_bytes"],
                        "pending_required": state["pending_required"],
                        "rss_kib": rss_kib(server.pid),
                        "fds": fd_count(server.pid),
                        "tasks": state["runtime_tasks"],
                        "cpu_percent": float(
                            subprocess.check_output(
                                ["ps", "-o", "%cpu=", "-p", str(server.pid)], text=True
                            ).strip()
                        ),
                        "loadgen_cpu_percent": float(
                            subprocess.check_output(
                                ["ps", "-o", "%cpu=", "-p", str(load.pid)], text=True
                            ).strip()
                        ),
                        "sink_cpu_percent": float(
                            subprocess.check_output(
                                ["ps", "-o", "%cpu=", "-p", str(sink.pid)], text=True
                            ).strip()
                        ),
                    }
                )
                time.sleep(1)
            load_out, load_err = load.communicate(timeout=5)
            profile_stderr = ""
            if profiler is not None:
                _, profile_stderr = profiler.communicate(timeout=args.sample_seconds + 10)
            server_cpu_seconds = process_cpu_seconds(server.pid) - server_cpu_before
            metrics = management_get(management, "/api/v1/metrics", args.tls)
            server_cpu_seconds = cpu_seconds(server.pid)
            server.send_signal(signal.SIGTERM)
            server.wait(timeout=30)
            sink.send_signal(signal.SIGTERM)
            sink_out, sink_err = sink.communicate(timeout=10)
            result = {
                "rate_requested": args.rate,
                "duration_seconds": args.duration,
                "connections": args.connections,
                "qos": args.qos,
                "payload_bytes": args.payload_bytes,
                "warmup_seconds": args.warmup,
                "cooldown_seconds": args.cooldown,
                "sink_mode": args.sink_mode,
                "tls": args.tls,
                "sink_delay_ms": args.sink_delay_ms,
                "load": last_json(load_out, "final"),
                "sink": last_json(sink_out),
                "metrics": metrics,
                "samples": samples,
                "server_exit": server.returncode,
                "server_cpu_seconds": server_cpu_seconds,
                "load_stderr": load_err[-512:],
                "sink_stderr": sink_err[-512:],
                "profile_stderr": profile_stderr[-512:],
            }
            print(json.dumps(result, sort_keys=True))
        finally:
            for process in (load, server, sink):
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
