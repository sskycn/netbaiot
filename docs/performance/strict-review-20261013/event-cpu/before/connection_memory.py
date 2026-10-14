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
from pathlib import Path
import hashlib
import platform
from linux_measurement import process as linux_process, cpu_percent, tcp_listener_memory


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
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], universal_newlines=True).strip())


def fd_count(pid):
    if os.path.isdir('/proc/%d/fd' % pid):
        with os.scandir('/proc/%d/fd' % pid) as entries:
            return sum(1 for _ in entries)
    output = subprocess.check_output(["lsof", "-n", "-p", str(pid)], universal_newlines=True)
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
        if isinstance(record, dict) and record.get("event") == "final":
            final = record.get("stats")
    return final


def finish_load(load, timeout):
    try:
        return load.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        load.kill()
        stdout, stderr = load.communicate(timeout=10)
        return stdout, stderr + "\nload generator exceeded its owned completion deadline\n"


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
    parser.add_argument("--mqtt-version", type=int, choices=(4, 5), default=4)
    parser.add_argument("--ramp-per-second", type=int, default=1500)
    parser.add_argument("--allocation-profile", action="store_true")
    parser.add_argument("--density", action="store_true", help="use the compact Rust holder")
    parser.add_argument("--ping-seconds", type=int, default=0)
    parser.add_argument("--require-density-limits", action="store_true",
                        help="reject a stale/pre-optimization executable")
    parser.add_argument("--server", default=os.path.join(ROOT, "target/release/netbaiot-server"))
    parser.add_argument("--loadgen", default=os.path.join(ROOT, "target/release/netbaiot-loadgen"))
    args = parser.parse_args()
    if not 1 <= args.connections <= 1_000_000:
        raise SystemExit("connections must be 1..1000000")
    if not 1 <= args.ramp_per_second <= 100000:
        raise SystemExit("ramp-per-second must be 1..100000")
    # Self-contained probes provision explicit unique static identities. The
    # server's hostile-config bound still applies; use the distributed loadgen
    # against an externally provisioned/auth-provider server for larger stages.
    if args.connections > 25000:
        raise SystemExit("self-contained static credential probe is bounded to 25000; "
                         "use netbaiot-loadgen against a provisioned server for 50K..1M")
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
    defaults = json.loads(subprocess.check_output([args.server, '--print-default-limits'], universal_newlines=True))
    if args.require_density_limits and (
        defaults.get('connection_memory_reservation') != 16384
        or 'max_connection_bytes_per_tenant' not in defaults
        or 'max_tls_handshakes' not in defaults
    ):
        raise SystemExit('executable does not have the optimized connection limits; rebuild it')
    def binary_digest(path):
        digest = hashlib.sha256()
        with open(path, 'rb') as source:
            for block in iter(lambda: source.read(1024 * 1024), b''):
                digest.update(block)
        return digest.hexdigest()
    provenance = {'platform': platform.platform(), 'server_sha256': binary_digest(args.server),
                  'loadgen_sha256': binary_digest(args.loadgen),
                  'server_default_connection_reservation': defaults['connection_memory_reservation']}
    logical_bytes_per_connection = defaults['connection_memory_reservation']
    limits['connection_memory_reservation'] = logical_bytes_per_connection
    limits['global_connection_logical_bytes'] = maximum * (logical_bytes_per_connection + 65540)
    if 'max_connection_bytes_per_tenant' in defaults:
        limits['max_connection_bytes_per_tenant'] = limits['global_connection_logical_bytes']
        limits['mqtt_recovery_max_bytes'] = max(512 * 1024 * 1024, maximum * 4096)
        limits['global_mqtt_session_bytes'] = max(134217728, maximum * 4096)
    if maximum * logical_bytes_per_connection > 2**32 - 1:
        if 'max_connection_bytes_per_tenant' not in defaults:
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
        "ramp_per_sec": min(args.ramp_per_second, args.connections),
        "warmup_secs": 2,
        "duration_secs": args.hold_seconds,
        "cooldown_secs": 1,
        "publish_rate": 0,
        "subscribe": False,
        "mqtt_clean_session": not args.persistent,
        "clean_disconnect": True,
        "mqtt_version": args.mqtt_version,
    }
    if args.tls:
        workload["tls_ca"] = os.path.join(ROOT, "tests/fixtures/localhost-cert.pem")
    if args.subscribe:
        workload["subscribe"] = True
    if args.density:
        workload = {'address': workload['address'], 'transport': args.transport,
                    'connections': args.connections, 'ramp_per_sec': workload['ramp_per_sec'],
                    'hold_secs': math.ceil(args.hold_seconds), 'mqtt_version': args.mqtt_version,
                    'persistent': args.persistent, 'subscribe': args.subscribe,
                    'ping_secs': args.ping_seconds,
                    'tls_ca': workload.get('tls_ca')}
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
        if args.allocation_profile:
            environment['NETBAIOT_ALLOCATION_PROFILE'] = '1'
        server_log = open(os.path.join(temporary, 'server.log'), 'w+', encoding='utf-8')
        server = subprocess.Popen(
            [args.server, server_config],
            stdout=subprocess.DEVNULL,
            stderr=server_log,
            universal_newlines=True,
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
                        if server.poll() is not None:
                            server_log.seek(0)
                            details = server_log.read(65536)
                        raise RuntimeError("server did not become ready: " + details)
                    time.sleep(0.05)
            if args.allocation_profile:
                # The first instrumentation tick may precede config/provider
                # initialization. Baseline must include the ready idle server.
                time.sleep(1.2)
            base_rss = rss_kib(server.pid)
            base_fds = fd_count(server.pid)
            base_linux = linux_process(server.pid)
            def allocation_sample():
                server_log.flush()
                if os.fstat(server_log.fileno()).st_size > 16 * 1024 * 1024:
                    raise RuntimeError('server profiling log exceeded 16 MiB')
                server_log.seek(0)
                latest = None
                for line in server_log:
                    try:
                        row = json.loads(line)
                        if row.get('event') == 'allocation_profile':
                            latest = row
                    except (json.JSONDecodeError, AttributeError):
                        pass
                return latest
            base_heap = allocation_sample()
            load = subprocess.Popen([args.loadgen, *(['--density'] if args.density else []), load_config], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            if args.disconnected:
                load_stdout, load_stderr = finish_load(load, args.hold_seconds + 30 + args.connections / 1000)
                time.sleep(1)
            else:
                time.sleep(4 + args.connections / min(args.ramp_per_second, args.connections))
            loaded_status = status(management, args.tls)
            loaded_rss = rss_kib(server.pid)
            loaded_fds = fd_count(server.pid)
            loaded_linux = linux_process(server.pid)
            client_linux = linux_process(load.pid) if not args.disconnected else None
            loaded_heap = allocation_sample()
            kernel_socket_sample = tcp_listener_memory(device_ingress) if loaded_linux else None
            stable_samples = []
            if not args.disconnected:
                for _ in range(3):
                    time.sleep(1)
                    stable_samples.append({'active': active_connection_count(status(management, args.tls)),
                                           'linux': linux_process(server.pid),
                                           'rss_kib': rss_kib(server.pid)})
            active = active_connection_count(loaded_status)
            if not args.disconnected:
                # Keep the RSS snapshot from the held-connection phase, then wait
                # for normal completion so authentication failures and incomplete
                # connection establishment cannot be hidden by SIGINT.
                load_stdout, load_stderr = finish_load(load, args.hold_seconds + 30 + args.connections / 1000)
            load_final = parse_load_final(load_stdout)
            denominator, validation_errors = validate_population(
                args.connections, active, args.disconnected, load_final, load.returncode
            )
            valid = not validation_errors
            if any(sample['active'] != args.connections for sample in stable_samples):
                validation_errors.append('connection population changed during stable samples')
                valid = False
            heap_per_connection = None
            if valid and denominator and base_heap and loaded_heap:
                heap_per_connection = (loaded_heap['requested_live_bytes'] - base_heap['requested_live_bytes']) / denominator
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
                "provenance": provenance,
                "transport": args.transport,
                "tls": args.tls,
                "mqtt_version": args.mqtt_version,
                "compact_density_client": args.density,
                "ping_seconds": args.ping_seconds if args.density else 10,
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
                "global_connection_logical_byte_ceiling": limits['global_connection_logical_bytes'],
                "requested_heap_base": base_heap,
                "requested_heap_loaded": loaded_heap,
                "requested_heap_delta_bytes_per_connection": heap_per_connection,
                "linux_base": base_linux,
                "linux_loaded": loaded_linux,
                "linux_client_loaded": client_linux,
                "linux_listener_socket_memory": kernel_socket_sample,
                "linux_stable_samples": stable_samples,
                "server_cpu_percent_ramp": cpu_percent(base_linux, loaded_linux),
                "server_cpu_percent_stable": cpu_percent(loaded_linux, stable_samples[-1]['linux']) if stable_samples else None,
                "kernel_memory_attribution": 'shared namespace/cgroup includes local client; not server-only',
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
            server_log.close()


if __name__ == "__main__":
    main()
