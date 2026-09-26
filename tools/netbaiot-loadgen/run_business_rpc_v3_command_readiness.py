#!/usr/bin/env python3
"""Run a disposable local mTLS V3 Auth/Event/Command mixed workload."""

import argparse
import hashlib
import json
import os
import platform
import signal
import socket
import ssl
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures"
ADMIN_SECRET = "a" * 64
V3_LIMITS = {
    "max_frame_payload_bytes": 8192,
    "max_concurrent_streams": 256,
    "initial_stream_window_bytes": 262144,
    "initial_connection_window_bytes": 4194304,
    "heartbeat_ms": 5000,
}


def fingerprint(name):
    pem = (FIXTURES / name).read_text()
    der = ssl.PEM_cert_to_DER_cert(pem)
    return hashlib.sha256(der).hexdigest()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def metrics(port):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v1/metrics",
        headers={"Authorization": f"Bearer {ADMIN_SECRET}"},
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        request, timeout=2
    ) as response:
        return response.read().decode()


def status(port):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v1/status",
        headers={"Authorization": f"Bearer {ADMIN_SECRET}"},
    )
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
        request, timeout=2
    ) as response:
        return json.load(response)


def metric(body, name):
    prefix = f"netbaiot_{name} "
    return next((int(line[len(prefix) :]) for line in body.splitlines() if line.startswith(prefix)), None)


def sysctl(name):
    try:
        return subprocess.check_output(["sysctl", "-n", name], text=True).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def identity(name, role, provide, call, provider=None, sink=None):
    return {
        "certificate_sha256": fingerprint(name),
        "principal_id": role,
        "role": role,
        "provider_id": provider,
        "sink_id": sink,
        "provide_methods": provide,
        "call_methods": call,
        "global": False,
        "tenants": ["demo"],
        "expires_at_ms": None,
    }


def tls_client(cert, key):
    return {
        "mode": "mtls",
        "server_name": "localhost",
        "ca_pem": str(FIXTURES / "localhost-cert.pem"),
        "certificate_pem": str(FIXTURES / cert),
        "private_key_pem": str(FIXTURES / key),
    }


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    profile = json.loads(args.profile.read_text())
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    device_port, admin_port, business_port = free_port(), free_port(), free_port()
    if len({device_port, admin_port, business_port}) != 3:
        raise RuntimeError("port reservation collision")

    gateway = json.loads((ROOT / "configs" / "development.json").read_text())
    gateway.update(
        device_ingress=f"127.0.0.1:{device_port}",
        management_http=f"127.0.0.1:{admin_port}",
        business_tcp=f"127.0.0.1:{business_port}",
        device_auth="business_rpc",
        event_delivery="business_rpc",
        spool_directory=str(output / "spool"),
        credentials=[],
    )
    # The local workload uses one loopback IP and keeps all command devices
    # connected. Raise only the connection quotas needed for this topology.
    device_connections = profile["command_device_count"] + 1 + int(profile["tcp_command_device"])
    loopback_headroom = max(96, device_connections + 8)
    gateway["limits"].update(
        max_connections_per_ip=loopback_headroom,
        max_connections_per_tenant=loopback_headroom,
        requests_per_ip_second=loopback_headroom,
    )
    gateway["business_rpc"] = {
        "version": 2,
        "v3": V3_LIMITS,
        "v3_send_ahead": None,
        "v3_experiment_socket_send_buffer_bytes": None,
        "tls": {
            "certificate": str(FIXTURES / "localhost-cert.pem"),
            "private_key": str(FIXTURES / "localhost-key.pem"),
            "client_ca": str(FIXTURES / "business-rpc-test-client-cas.pem"),
            "require_client_certificate": True,
        },
        "identities": [
            identity(
                "management-client.pem",
                "multiplexed",
                ["device.authenticate", "device.resolve_verifier"],
                ["auth.sync", "auth.invalidate"],
                provider="primary",
                sink="tcp-rpc",
            ),
            identity(
                "business-command-client.pem",
                "commands",
                [],
                ["device.command.send"],
            ),
        ],
        "development_token_env": None,
        "development_role": None,
        "allow_v1": False,
        "max_connections": 8,
        "auth_max_inflight": 32,
        "max_auth_control_offline_ms": 30000,
    }
    (output / "gateway-config.json").write_text(json.dumps(gateway, indent=2) + "\n")
    load = {
        "scenario": "soak",
        "business_address": f"127.0.0.1:{business_port}",
        "device_address": f"127.0.0.1:{device_port}",
        "udp_address": f"127.0.0.1:{device_port}",
        "management_url": f"http://127.0.0.1:{admin_port}",
        "gateway_pid": None,
        "duration_secs": profile["duration_secs"],
        "auth_concurrency": profile["auth_concurrency"],
        "auth_rate": profile.get("auth_rate", 0),
        "event_rate": profile["event_rate"],
        "command_rate": profile["command_rate"],
        "command_device_count": profile["command_device_count"],
        "tcp_command_device": profile["tcp_command_device"],
        "event_ack_delay_ms": profile["event_ack_delay_ms"],
        "event_reconnect_every_secs": profile["event_reconnect_every_secs"],
        "auth_reconnect_every_secs": profile.get("auth_reconnect_every_secs", 0),
        "invalidate_every_secs": profile["invalidate_every_secs"],
        "verifier_rate": profile["verifier_rate"],
        "reconnect_cycles": 0,
        "reconnect_pause_ms": 0,
        "sample_period_ms": 1000,
        "snapshot_every_secs": 5,
        "warmup_secs": 5,
        "recovery_secs": 5,
        "business_transport": tls_client("management-client.pem", "management-client-key.pem"),
        "command_transport": tls_client(
            "business-command-client.pem", "business-command-client-key.pem"
        ),
        "topology": profile.get("topology", "v3"),
        "network_profile": "local_mtls",
        "event_payload_bytes": 1024,
    }
    server_bin = ROOT / "target" / "release" / "netbaiot-server"
    load_bin = ROOT / "target" / "release" / "business_rpc"
    if not server_bin.exists() or not load_bin.exists():
        raise RuntimeError("build release binaries first: cargo build --locked --release -p netbaiot-server -p netbaiot-loadgen --bins")
    environment = os.environ.copy()
    environment["NETBAIOT_ADMIN_SECRET"] = ADMIN_SECRET
    start = time.time()
    with (output / "gateway.log").open("wb") as gateway_log:
        server = subprocess.Popen(
            [str(server_bin), str(output / "gateway-config.json")],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=gateway_log,
        )
        try:
            for _ in range(100):
                if server.poll() is not None:
                    raise RuntimeError(f"gateway exited during startup: {server.returncode}")
                try:
                    metrics(admin_port)
                    break
                except Exception:
                    time.sleep(0.1)
            else:
                raise RuntimeError("gateway did not become ready")
            load["gateway_pid"] = server.pid
            (output / "loadgen-config.json").write_text(json.dumps(load, indent=2) + "\n")
            with (output / "loadgen.json").open("wb") as raw, (output / "loadgen.log").open("wb") as log:
                result = subprocess.run(
                    [str(load_bin), str(output / "loadgen-config.json")],
                    cwd=ROOT,
                    env=environment,
                    stdout=raw,
                    stderr=log,
                    timeout=profile["duration_secs"] + 120,
                    check=False,
                )
            final_metrics = metrics(admin_port)
            (output / "final-metrics.txt").write_text(final_metrics)
            final_status = status(admin_port)
            (output / "final-status.json").write_text(json.dumps(final_status, indent=2) + "\n")
            raw_result = json.loads((output / "loadgen.json").read_text()) if result.returncode == 0 else None
            counts = raw_result.get("counts", {}) if raw_result else {}
            gauges = {
                name: metric(final_metrics, name)
                for name in (
                    "business_rpc_v3_active_connections",
                    "business_rpc_v3_active_streams",
                    "business_rpc_v3_queued_bytes",
                    "business_rpc_v3_reassembly_reserved_bytes",
                    "business_rpc_pending",
                    "business_rpc_pending_bytes",
                    "command_dedup_entries",
                    "command_dedup_inflight",
                    "command_dedup_accepted",
                )
            }
            gateway_counts = {
                name: metric(final_metrics, name)
                for name in (
                    "connections_rejected_total",
                    "events_accepted_total",
                    "sink_acks_total",
                    "sink_retries_total",
                )
            }
            checks = {
                "loadgen_exit": result.returncode == 0,
                "gateway_alive": server.poll() is None,
                "real_commands_delivered": counts.get("command_device_deliveries", 0) > 0,
                "mqtt_commands_delivered": counts.get("mqtt_command_device_deliveries", 0) > 0,
                "tcp_commands_delivered": counts.get("tcp_command_device_deliveries", 0) > 0,
                "no_duplicate_device_delivery": counts.get("command_device_duplicates") == 0,
                "no_delivery_after_unavailable": counts.get("command_delivery_after_unavailable") == 0,
                "device_application_acks": counts.get("command_device_acks") == counts.get("command_device_deliveries"),
                "all_accepted_commands_delivered": counts.get("command_accepted", 0) > 0 and counts.get("command_accepted") == counts.get("command_device_deliveries"),
                "all_command_ack_events_acked": counts.get("command_ack_event_unique", 0) > 0 and counts.get("command_ack_event_unique") == counts.get("command_ack_event_acks") == counts.get("command_accepted"),
                "all_accepted_events_acked": gateway_counts["events_accepted_total"] is not None and gateway_counts["events_accepted_total"] == gateway_counts["sink_acks_total"],
                "no_command_errors": counts.get("command_errors") == 0,
                "no_ingress_rejections": not profile.get("require_zero_ingress_rejections", False) or gateway_counts["connections_rejected_total"] == 0,
                "no_publish_errors": not profile.get("require_zero_publish_errors", False) or counts.get("publish_errors") == 0,
                "connections_released": gauges["business_rpc_v3_active_connections"] == 0,
                "streams_released": gauges["business_rpc_v3_active_streams"] == 0,
                "queued_bytes_released": gauges["business_rpc_v3_queued_bytes"] == 0,
                "reassembly_released": gauges["business_rpc_v3_reassembly_reserved_bytes"] == 0,
                "rpc_pending_released": gauges["business_rpc_pending"] == 0 and gauges["business_rpc_pending_bytes"] == 0,
                "required_events_drained": final_status.get("pending_required") == 0,
                "dedup_inflight_released": gauges["command_dedup_inflight"] == 0,
                "dedup_bounded": gauges["command_dedup_entries"] is not None and gauges["command_dedup_entries"] <= 4096,
            }
            summary = {
                "start_time_unix": start,
                "duration_secs": profile["duration_secs"],
                "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                "branch": subprocess.check_output(["git", "branch", "--show-current"], cwd=ROOT, text=True).strip(),
                "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
                "environment": {
                    "os": platform.platform(),
                    "kernel": platform.release(),
                    "architecture": platform.machine(),
                    "cpu": sysctl("machdep.cpu.brand_string") or platform.processor(),
                    "ram_bytes": sysctl("hw.memsize"),
                    "rust": subprocess.check_output(["rustc", "--version"], text=True).strip(),
                    "cargo": subprocess.check_output(["cargo", "--version"], text=True).strip(),
                    "profile": "release",
                    "business_transport": "mtls",
                    "business_rpc_version": 3,
                },
                "config": profile,
                "limits_config": {
                    "source": "gateway defaults; see gateway-config.json",
                    "command_ttl_ms": 300000,
                    "command_dedup_ttl_ms": 300000,
                    "command_dedup_max_entries": 4096,
                },
                "v3_limits": V3_LIMITS,
                "random_seed": None,
                "gateway_config": "gateway-config.json",
                "loadgen_config": "loadgen-config.json",
                "loadgen_exit_code": result.returncode,
                "counts": counts,
                "gateway_counts": gateway_counts,
                "gauges": gauges,
                "final_status": final_status,
                "checks": checks,
                "pass": all(checks.values()),
            }
            (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(json.dumps(summary, indent=2))
            return 0 if summary["pass"] else 1
        finally:
            if server.poll() is None:
                server.send_signal(signal.SIGTERM)
                try:
                    server.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


if __name__ == "__main__":
    sys.exit(run())
