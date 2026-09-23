#!/usr/bin/env python3
"""Measure server RSS/FD/task deltas while the Rust load generator holds idle sessions."""

import argparse
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import ssl


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
SECRET = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def rss_kib(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip())


def fd_count(pid):
    output = subprocess.check_output(["lsof", "-n", "-p", str(pid)], text=True)
    return max(0, len(output.splitlines()) - 1)


def status(port, tls):
    stream = socket.create_connection(("127.0.0.1", port), timeout=3)
    if tls:
        stream = ssl._create_unverified_context().wrap_socket(stream, server_hostname="localhost")
    request = (
        "GET /api/v1/status HTTP/1.1\r\n"
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
        raise RuntimeError(f"status response: {response[:256]!r}")
    return json.loads(body)


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--transport", choices=("mqtt", "tcp"), required=True)
    parser.add_argument("--connections", type=int, required=True)
    parser.add_argument("--tls", action="store_true")
    parser.add_argument("--persistent", action="store_true")
    parser.add_argument("--subscribe", action="store_true")
    parser.add_argument("--disconnected", action="store_true")
    parser.add_argument("--hold-seconds", type=float, default=12)
    parser.add_argument("--server", default=os.path.join(ROOT, "target/release/netbaiot-server"))
    parser.add_argument("--loadgen", default=os.path.join(ROOT, "target/release/netbaiot-loadgen"))
    args = parser.parse_args()
    if not 1 <= args.connections <= 10000:
        raise SystemExit("connections must be 1..10000")
    if args.disconnected and (args.transport != "mqtt" or not args.persistent):
        raise SystemExit("--disconnected requires --transport mqtt --persistent")

    device_ingress, management = [free_port() for _ in range(2)]
    stream_port = device_ingress
    maximum = max(128, args.connections + 32)
    limits = {
        "max_connections": maximum,
        "max_connections_per_ip": maximum,
        "max_connections_per_tenant": maximum,
        "max_devices": maximum,
        "max_devices_per_tenant": maximum,
        "max_persistent_sessions": maximum,
        "max_persistent_sessions_per_tenant": maximum,
        "max_subscriptions_per_tenant": maximum * 2,
        "max_subscriptions": maximum * 2,
        "auth_cache_max_entries": maximum,
        "auth_cache_max_bytes": 64 * 1024 * 1024,
        "rate_entries": maximum,
        "requests_per_second": maximum,
        "requests_per_ip_second": maximum,
        "messages_per_device_second": maximum,
        "messages_per_tenant_second": maximum * 3,
        "global_connection_logical_bytes": maximum * 524288,
        # Scaling the admitted session/connection counts also scales the validated
        # worst-case NBMQ recovery image. Keep the benchmark configuration valid;
        # this is a ceiling and is not allocated by the probe.
        "mqtt_recovery_max_bytes": 512 * 1024 * 1024,
    }
    logical_bytes_per_connection = 524288
    if maximum * logical_bytes_per_connection > 2**32 - 1:
        logical_bytes_per_connection = 65536
        limits["connection_memory_reservation"] = 65536
        limits["global_connection_logical_bytes"] = maximum * 65536
    config = {
        "device_ingress": f"127.0.0.1:{device_ingress}",
        "management_http": f"127.0.0.1:{management}",
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
        "delivery_url": None,
        "auth_provider_url": None,
        "spool_directory": "spool",

    }
    workload = {
        "transport": args.transport,
        "address": f"127.0.0.1:{stream_port}",
        "connections": args.connections,
        "tenant_width": args.connections + 1,
        "ramp_per_sec": min(1500, args.connections),
        "warmup_secs": 2,
        "duration_secs": args.hold_seconds,
        "cooldown_secs": 1,
        "publish_rate": 0,
        "subscribe": False,
        "mqtt_clean_session": not args.persistent,
        "clean_disconnect": True,
    }
    if args.tls:
        workload["tls_ca"] = os.path.join(ROOT, "tests/fixtures/localhost-cert.pem")
    if args.subscribe:
        workload["subscribe"] = True
    with tempfile.TemporaryDirectory(prefix="netbaiot-memory-") as temporary:
        config["spool_directory"] = os.path.join(temporary, "spool")
        server_config = os.path.join(temporary, "server.json")
        load_config = os.path.join(temporary, "load.json")
        with open(server_config, "w", encoding="utf-8") as output:
            json.dump(config, output)
        with open(load_config, "w", encoding="utf-8") as output:
            json.dump(workload, output)
        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = "ab" * 32
        environment["NO_PROXY"] = "127.0.0.1,localhost"
        environment["no_proxy"] = "127.0.0.1,localhost"
        server = subprocess.Popen(
            [args.server, server_config],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            env=environment,
        )
        load = None
        try:
            deadline = time.time() + 10
            while True:
                try:
                    base_status = status(management, args.tls)
                    break
                except Exception:
                    if server.poll() is not None or time.time() >= deadline:
                        details = ""
                        if server.poll() is not None and server.stderr is not None:
                            details = server.stderr.read()
                        raise RuntimeError("server did not become ready: " + details)
                    time.sleep(0.05)
            base_rss = rss_kib(server.pid)
            base_fds = fd_count(server.pid)
            load = subprocess.Popen([args.loadgen, load_config], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            if args.disconnected:
                load.communicate(timeout=30 + args.connections / 1000)
                time.sleep(1)
            else:
                time.sleep(4 + args.connections / min(1500, args.connections))
            loaded_status = status(management, args.tls)
            loaded_rss = rss_kib(server.pid)
            loaded_fds = fd_count(server.pid)
            active_connections = loaded_status.get("active_connections", {})
            if isinstance(active_connections, dict):
                active_connection_count = sum(active_connections.values())
            else:
                active_connection_count = sum(active_connections)
            result = {
                "transport": args.transport,
                "tls": args.tls,
                "persistent": args.persistent,
                "subscribed": args.subscribe,
                "measurement_state": "disconnected" if args.disconnected else "active",
                "connections_requested": args.connections,
                # Management leases are excluded from device connection counts.
                "connections_active": active_connection_count,
                "rss_base_kib": base_rss,
                "rss_loaded_kib": loaded_rss,
                "rss_delta_kib": loaded_rss - base_rss,
                "rss_delta_bytes_per_connection": round((loaded_rss - base_rss) * 1024 / args.connections, 1),
                "fd_base": base_fds,
                "fd_loaded": loaded_fds,
                "runtime_tasks_base": base_status["runtime_tasks"],
                "runtime_tasks_loaded": loaded_status["runtime_tasks"],
                "tasks_delta_per_connection": round(
                    (loaded_status["runtime_tasks"] - base_status["runtime_tasks"]) / args.connections, 3
                ),
                "logical_bytes_per_connection": logical_bytes_per_connection,
            }
            print(json.dumps(result, sort_keys=True))
            if load.poll() is None:
                load.send_signal(signal.SIGINT)
                load.communicate(timeout=10)
        finally:
            if load is not None and load.poll() is None:
                load.kill()
                load.wait()
            if server.poll() is None:
                server.send_signal(signal.SIGTERM)
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


if __name__ == "__main__":
    main()
