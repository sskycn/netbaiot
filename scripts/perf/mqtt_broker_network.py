#!/usr/bin/env python3
"""Repeatable local MQTT persistent-session network probe.

This is an opt-in measurement, not a capacity test or a CI gate. The server
and load generator share one host, and each run uses fresh temporary state.
"""

import argparse
import json
import os
import socket
import subprocess
import tempfile
import time

from connection_memory import SECRET, free_port, rss_kib, status
from event_load import management_get


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))


def credential(index, tenant_width):
    return {
        "credential_id": f"a{index}",
        "secret_hex": SECRET,
        "identity": {
            "device_key": {
                "tenant_id": f"t{index // tenant_width}",
                "product_id": "p",
                "device_id": f"d{index}",
            },
            "credential_version": 1,
            "auth_generation": 1,
            "codec_id": "netbaiot-json",
            "codec_version": 1,
            "permissions": {"publish": True, "commands": True},
        },
    }


def metric_subset(body):
    wanted = (
        "netbaiot_broker_lock_wait_us_",
        "netbaiot_broker_lock_hold_us_",
        "netbaiot_mqtt_connect_",
        "netbaiot_mqtt_subscriptions_",
        "netbaiot_mqtt_publishes_",
        "netbaiot_mqtt_pubacks_",
        "netbaiot_connections_rejected_",
        "netbaiot_queue_rejects_",
        "netbaiot_timeouts_",
    )
    result = {}
    for line in body.splitlines():
        if line.startswith(wanted) and not line.startswith("#"):
            name, value = line.rsplit(" ", 1)
            result[name] = float(value)
    return result


def last_final(output):
    for line in reversed(output.splitlines()):
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            continue
        if item.get("event") == "final":
            return item
    return None


def mqtt_packet(first, body):
    length = len(body)
    encoded = bytearray()
    while True:
        digit = length % 128
        length //= 128
        encoded.append(digit | (128 if length else 0))
        if not length:
            break
    return bytes([first]) + bytes(encoded) + body


def mqtt_text(value):
    value = value.encode()
    return len(value).to_bytes(2, "big") + value


def recv_exact(stream, size):
    output = bytearray()
    while len(output) < size:
        part = stream.recv(size - len(output))
        if not part:
            raise RuntimeError("sentinel MQTT socket closed")
        output.extend(part)
    return bytes(output)


def broker_sentinel(port, device_id, tenant_width):
    """Keep one unrelated subscription present so publishes enter Broker::route."""
    stream = socket.create_connection(("127.0.0.1", port), timeout=5)
    stream.settimeout(5)
    connect = (mqtt_text("MQTT") + bytes([4, 0xC2, 1, 44])
               + mqtt_text("scale-sentinel") + mqtt_text(f"a{device_id}")
               + mqtt_text(SECRET))
    stream.sendall(mqtt_packet(0x10, connect))
    if recv_exact(stream, 4) != bytes([0x20, 0x02, 0, 0]):
        raise RuntimeError("sentinel MQTT CONNECT failed")
    subscribe = (bytes([0, 1]) + mqtt_text(
        f"v1/t/t{device_id // tenant_width}/p/p/d/d{device_id}/down"
    ) + bytes([0]))
    stream.sendall(mqtt_packet(0x82, subscribe))
    if recv_exact(stream, 5) != bytes([0x90, 0x03, 0, 1, 0]):
        raise RuntimeError("sentinel MQTT SUBSCRIBE failed")
    return stream


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--connections", type=int, required=True)
    parser.add_argument("--tenant-width", type=int, default=0)
    parser.add_argument("--rate", type=float, default=0.0)
    parser.add_argument("--subscribe", action="store_true")
    parser.add_argument("--broker-subscriber", action="store_true")
    parser.add_argument("--qos2-fraction", type=float, default=0.0)
    parser.add_argument("--reconnect-every", type=float, default=0.0)
    parser.add_argument("--duration", type=float, default=12.0)
    parser.add_argument("--ramp", type=float, default=500.0)
    parser.add_argument("--server", default=os.path.join(ROOT, "target/release/netbaiot-server"))
    parser.add_argument("--loadgen", default=os.path.join(ROOT, "target/release/netbaiot-loadgen"))
    args = parser.parse_args()
    if not 1 <= args.connections <= 50000 or not 0 <= args.qos2_fraction <= 1:
        parser.error("connections must be 1..50000 and qos2 fraction 0..1")
    tenant_width = args.tenant_width or args.connections + 1
    ingress, management = free_port(), free_port()
    maximum = args.connections + 64
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
        "auth_cache_max_bytes": 128 * 1024 * 1024,
        "rate_entries": maximum,
        "requests_per_second": 1000000,
        "requests_per_ip_second": 1000000,
        "messages_per_device_second": 1000000,
        "messages_per_tenant_second": 1000000,
        "connection_memory_reservation": 65536,
        "global_connection_logical_bytes": maximum * 65536,
        "max_ingress": 128,
        "max_ingress_per_tenant": 128,
        "max_ingress_per_device": 4,
        "mqtt_recovery_max_bytes": 512 * 1024 * 1024,
    }
    with tempfile.TemporaryDirectory(prefix="netbaiot-mqtt-scale-") as temporary:
        server_config = os.path.join(temporary, "server.json")
        with open(server_config, "w", encoding="utf-8") as output:
            json.dump(
                {
                    "device_ingress": f"127.0.0.1:{ingress}",
                    "management_http": f"127.0.0.1:{management}",
                    "business_tcp": None,
                    "development": True,
                    "limits": limits,
                    "credentials": [credential(i, tenant_width) for i in range(args.connections + 1)],
                    "tls": None,
                    "delivery_url": None,
                    "auth_provider_url": None,
                    "spool_directory": os.path.join(temporary, "spool"),
                },
                output,
                separators=(",", ":"),
            )
        qos2_connections = round(args.connections * args.qos2_fraction)
        groups = [(0, args.connections - qos2_connections, 1)]
        if qos2_connections:
            groups.append((args.connections - qos2_connections, qos2_connections, 2))
        configs = []
        for offset, count, qos in groups:
            if count == 0:
                continue
            path = os.path.join(temporary, f"load-{qos}.json")
            with open(path, "w", encoding="utf-8") as output:
                json.dump(
                    {
                        "transport": "mqtt",
                        "address": f"127.0.0.1:{ingress}",
                        "connections": count,
                        "offset": offset,
                        "tenant_width": tenant_width,
                        "ramp_per_sec": args.ramp * count / args.connections,
                        "warmup_secs": 3,
                        "duration_secs": args.duration,
                        "cooldown_secs": 2,
                        "publish_rate": args.rate * count / args.connections,
                        "payload_bytes": 256,
                        "qos": qos,
                        "subscribe": args.subscribe,
                        "mqtt_clean_session": False,
                        "reconnect_every_secs": args.reconnect_every,
                        "report_every_secs": 2,
                    },
                    output,
                )
            configs.append((qos, path))
        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = "ab" * 32
        environment["NETBAIOT_PERF_LOCK_METRICS"] = "1"
        environment["NO_PROXY"] = "127.0.0.1,localhost"
        environment["no_proxy"] = "127.0.0.1,localhost"
        server = subprocess.Popen(
            [args.server, server_config], stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE, text=True, env=environment,
        )
        loads = []
        sentinel = None
        try:
            deadline = time.monotonic() + 30
            while True:
                try:
                    status(management, False)
                    break
                except Exception:
                    if server.poll() is not None or time.monotonic() >= deadline:
                        raise RuntimeError("server failed to become ready")
                    time.sleep(0.1)
            baseline = metric_subset(management_get(management, "/api/v1/metrics"))
            if args.broker_subscriber:
                sentinel = broker_sentinel(ingress, args.connections, tenant_width)
            for qos, path in configs:
                loads.append((qos, subprocess.Popen(
                    [args.loadgen, path], stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE, text=True,
                )))
            results = []
            for qos, process in loads:
                stdout, stderr = process.communicate(
                    timeout=args.connections / args.ramp + args.duration + 50
                )
                results.append({"qos": qos, "exit": process.returncode,
                                "final": last_final(stdout), "error": stderr[-1000:]})
            after = metric_subset(management_get(management, "/api/v1/metrics"))
            try:
                rss = rss_kib(server.pid)
            except Exception:
                rss = None
            print(json.dumps({
                "connections": args.connections, "tenant_width": tenant_width,
                "rate": args.rate, "subscribe": args.subscribe,
                "broker_subscriber": args.broker_subscriber,
                "qos2_fraction": args.qos2_fraction,
                "reconnect_every": args.reconnect_every, "duration": args.duration,
                "config_bytes": os.path.getsize(server_config), "rss_kib": rss,
                "loadgen": results, "metrics_before": baseline,
                "metrics_after": after,
            }, separators=(",", ":")))
        finally:
            if sentinel is not None:
                sentinel.close()
            for _, process in loads:
                if process.poll() is None:
                    process.terminate()
                    process.communicate(timeout=10)
            if server.poll() is None:
                server.terminate()
            try:
                server.communicate(timeout=20)
            except subprocess.TimeoutExpired:
                server.kill()
                server.communicate()


if __name__ == "__main__":
    main()
