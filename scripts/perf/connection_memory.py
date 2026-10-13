#!/usr/bin/env python3
"""Measure server RSS/FD/task deltas while the Rust load generator holds idle sessions."""

import argparse
import json
import math
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
    return free_ports(1)[0]


def free_ports(count):
    """Keep test reservations open together so returned ports are distinct."""
    if not 1 <= count <= 16:
        raise ValueError("test port reservation count must be between 1 and 16")
    sockets = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sockets.append(sock)
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


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


def active_connection_count(snapshot):
    active = snapshot.get("active_connections", {})
    counts = active.values() if isinstance(active, dict) else active
    return sum(counts)


def parse_load_final(stdout):
    """Use the final cumulative counters, never a requested population or sample."""
    final = None
    for line in stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("event") == "final":
            final = record.get("stats")
    return final


def validate_population(requested, active, disconnected, final, returncode):
    """Validate the held-socket sample and the completed unique-client workload."""
    errors = []
    expected_active = 0 if disconnected else requested
    if active != expected_active:
        errors.append(f"sample has {active} active connections; expected {expected_active}")
    if returncode != 0:
        errors.append(f"load generator exited with {returncode}")
    if not isinstance(final, dict):
        errors.append("load generator did not emit final counters")
        return None, errors
    counters = final.get("counters", {})
    established = counters.get("connected", 0)
    if established != requested:
        errors.append(f"load generator established {established} connections; expected {requested}")
    if counters.get("connect_attempts", 0) != requested:
        errors.append("connection attempts do not match the unique requested population")
    if counters.get("disconnected", 0) != established:
        errors.append("not all established connections completed their owned shutdown")
    for name in ["client_errors", "connect_rejected", "forced_tasks", "panicked_tasks", "reconnects"]:
        if counters.get(name, 0):
            errors.append(f"load generator reported {name}={counters[name]}")
    if final.get("error_samples"):
        errors.append("load generator reported error samples")
    # No public broker gauge reports retained MQTT session count. For the
    # disconnected probe this is the successfully established unique persistent
    # population, not a claim about a directly measured broker collection length.
    return established if disconnected else active, errors


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
    if not math.isfinite(args.hold_seconds) or args.hold_seconds <= 0:
        raise SystemExit("hold-seconds must be finite and positive")
    if args.disconnected and (args.transport != "mqtt" or not args.persistent):
        raise SystemExit("--disconnected requires --transport mqtt --persistent")

    device_ingress, management = free_ports(2)
    stream_port = device_ingress
    maximum = max(128, args.connections + 32)
    limits = {
        "max_connections": maximum,
        "max_device_connections_per_protocol": maximum,
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
                load_stdout, load_stderr = load.communicate(timeout=args.hold_seconds + 30 + args.connections / 1000)
                time.sleep(1)
            else:
                time.sleep(4 + args.connections / min(1500, args.connections))
            loaded_status = status(management, args.tls)
            loaded_rss = rss_kib(server.pid)
            loaded_fds = fd_count(server.pid)
            active = active_connection_count(loaded_status)
            if not args.disconnected:
                # Keep the RSS snapshot from the held-connection phase, then wait
                # for normal completion so authentication failures and incomplete
                # connection establishment cannot be hidden by SIGINT.
                load_stdout, load_stderr = load.communicate(timeout=args.hold_seconds + 30 + args.connections / 1000)
            load_final = parse_load_final(load_stdout)
            denominator, validation_errors = validate_population(
                args.connections, active, args.disconnected, load_final, load.returncode
            )
            valid = not validation_errors
            rss_per_population = (
                round((loaded_rss - base_rss) * 1024 / denominator, 1)
                if valid and denominator
                else None
            )
            tasks_per_population = (
                round((loaded_status["runtime_tasks"] - base_status["runtime_tasks"]) / denominator, 3)
                if valid and denominator
                else None
            )
            result = {
                "transport": args.transport,
                "tls": args.tls,
                "persistent": args.persistent,
                "subscribed": args.subscribe,
                "measurement_state": "disconnected" if args.disconnected else "active",
                "connections_requested": args.connections,
                # Management leases are excluded from device connection counts.
                "connections_active": active,
                "connections_established": (load_final or {}).get("counters", {}).get("connected", 0),
                "population_denominator": denominator,
                "population_kind": "established_persistent_sessions" if args.disconnected else "active_connections",
                "measurement_valid": valid,
                "validation_errors": validation_errors,
                "load_returncode": load.returncode,
                "load_final": load_final,
                "load_stderr": load_stderr,
                "rss_base_kib": base_rss,
                "rss_loaded_kib": loaded_rss,
                "rss_delta_kib": loaded_rss - base_rss,
                "rss_delta_bytes_per_connection": None if args.disconnected else rss_per_population,
                "rss_delta_bytes_per_established_persistent_session": rss_per_population if args.disconnected else None,
                "fd_base": base_fds,
                "fd_loaded": loaded_fds,
                "runtime_tasks_base": base_status["runtime_tasks"],
                "runtime_tasks_loaded": loaded_status["runtime_tasks"],
                "tasks_delta_per_connection": None if args.disconnected else tasks_per_population,
                "tasks_delta_per_established_persistent_session": tasks_per_population if args.disconnected else None,
                "logical_bytes_per_connection": logical_bytes_per_connection,
            }
            print(json.dumps(result, sort_keys=True))
            if not valid:
                raise SystemExit("connection population validation failed; diagnostic JSON retained above")
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
