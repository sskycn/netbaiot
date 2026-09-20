#!/usr/bin/env python3
"""Run a bounded real MQTT -> EventAccepted -> confirmed webhook measurement."""

import argparse
import json
import os
import signal
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, required=True)
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--sink-delay-ms", type=float, default=0)
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
        "auth_cache_max_entries": maximum,
        "auth_cache_max_bytes": 16 * 1024 * 1024,
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
                    "tls": None,
                    "delivery_url": f"http://127.0.0.1:{sink_port}/events",
                    "auth_provider_url": None,
                    "spool_directory": os.path.join(temporary, "spool"),
                    "device_configs": [],
                },
                output,
            )
        with open(load_config, "w", encoding="utf-8") as output:
            json.dump(
                {
                    "transport": "mqtt",
                    "address": f"127.0.0.1:{mqtt}",
                    "connections": args.connections,
                    "tenant_width": args.connections + 1,
                    "ramp_per_sec": args.connections,
                    "warmup_secs": 2,
                    "duration_secs": args.duration,
                    "cooldown_secs": 2,
                    "publish_rate": args.rate,
                    "payload_bytes": 256,
                    "qos": 1,
                    "subscribe": False,
                    "report_every_secs": 5,
                },
                output,
            )
        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = "ab" * 32
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
                    time.sleep(.05)
            load = subprocess.Popen(
                [os.path.join(ROOT, "target/release/netbaiot-loadgen"), load_config],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            samples = []
            while load.poll() is None:
                state = status(management, False)
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
                    }
                )
                time.sleep(1)
            load_out, load_err = load.communicate(timeout=5)
            server.send_signal(signal.SIGTERM)
            server.wait(timeout=30)
            sink.send_signal(signal.SIGTERM)
            sink_out, sink_err = sink.communicate(timeout=10)
            result = {
                "rate_requested": args.rate,
                "duration_seconds": args.duration,
                "connections": args.connections,
                "sink_delay_ms": args.sink_delay_ms,
                "load": last_json(load_out, "final"),
                "sink": last_json(sink_out),
                "samples": samples,
                "server_exit": server.returncode,
                "load_stderr": load_err[-512:],
                "sink_stderr": sink_err[-512:],
            }
            print(json.dumps(result, sort_keys=True))
        finally:
            for process in (load, server, sink):
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
