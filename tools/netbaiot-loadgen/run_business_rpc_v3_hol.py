#!/usr/bin/env python3
"""Run fresh local gateways for V2 single, V2 dual, and V3 frame-size HOL samples.

This user-space proxy limits a byte stream; it does not emulate TCP packet loss.
The script writes loadgen JSON, metrics, and logs so each result is reviewable.
Optional capture stores V3 frame headers only, without tokens or Event bodies.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request


ROOT = Path(__file__).resolve().parents[2]
GATEWAY = ROOT / "target/release/netbaiot-server"
LOADGEN = ROOT / "target/release/business_rpc"
PROXY = ROOT / "tools/netbaiot-loadgen/business_stream_proxy.py"
MODES = ("multiplexed", "dual", "v3-4096", "v3-8192", "v3-16384")


def free_ports(count):
    reserved = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reserved.append(sock)
        return [sock.getsockname()[1] for sock in reserved]
    finally:
        for sock in reserved:
            sock.close()


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


def run_mode(mode, args):
    duration = args.duration_secs
    output = args.output
    event_payload_bytes = args.event_payload_bytes
    stream_window_bytes = args.stream_window_bytes
    disconnect_after_secs = args.disconnect_after_secs
    device, management, business, proxy_port = free_ports(4)
    frame_size = int(mode.split("-")[1]) if mode.startswith("v3-") else 8192
    with tempfile.TemporaryDirectory(prefix=f"netbaiot-hol-{mode}-") as temporary:
        temporary = Path(temporary)
        gateway_config = json.loads((ROOT / "configs/development.json").read_text())
        gateway_config.update({
            "device_ingress": f"127.0.0.1:{device}",
            "management_http": f"127.0.0.1:{management}",
            "business_tcp": f"127.0.0.1:{business}",
            "spool_directory": str(temporary / "spool"),
            "device_auth": "business_rpc",
            "event_delivery": "business_rpc",
            "business_rpc": {
                "version": 2,
                "v3": {
                    "max_frame_payload_bytes": frame_size,
                    "max_concurrent_streams": 256,
                    "initial_stream_window_bytes": 262144,
                    "initial_connection_window_bytes": 4194304,
                    "heartbeat_ms": 5000,
                },
                "v3_send_ahead": ({
                    "stream_bytes": args.send_ahead_stream_bytes,
                    "connection_bytes": args.send_ahead_connection_bytes,
                } if args.send_ahead_stream_bytes is not None else None),
                "v3_experiment_socket_send_buffer_bytes": args.socket_send_buffer_bytes,
                "tls": None,
                "development_token_env": "NETBAIOT_BUSINESS_RPC_TOKEN",
            },
        })
        if args.auth_unique_devices:
            # The SDK keeps a bounded persistent MQTT subscription per identity.
            # Raise those experiment capacities equally for all compared modes so
            # a 60-second fresh-identity run measures RPC, not the default 128
            # tenant subscription ceiling.
            gateway_config["limits"].update({
                "max_subscriptions": 2048,
                "max_subscriptions_per_tenant": 2048,
                "max_persistent_sessions_per_tenant": 2048,
                "max_devices": 2048,
                "max_devices_per_tenant": 2048,
            })
        gateway_path = temporary / "gateway.json"
        gateway_path.write_text(json.dumps(gateway_config))
        load_config = {
            "scenario": "multiplexed",
            "business_address": f"127.0.0.1:{business if args.no_proxy else proxy_port}",
            "topology": "v3" if mode.startswith("v3-") else mode,
            "frame_payload_bytes": frame_size if mode.startswith("v3-") else None,
            "stream_window_bytes": stream_window_bytes if mode.startswith("v3-") else None,
            "network_profile": ("loopback" if args.no_proxy else "user_proxy"),
            "device_address": f"127.0.0.1:{device}",
            "udp_address": f"127.0.0.1:{device}",
            "management_url": f"http://127.0.0.1:{management}",
            "duration_secs": duration,
            "auth_concurrency": args.auth_concurrency,
            "auth_unique_devices": args.auth_unique_devices,
            "auth_handler_delay_ms": 0,
            "event_rate": args.event_rate,
            "event_payload_bytes": event_payload_bytes,
            "event_ack_delay_ms": 0,
            "event_reconnect_every_secs": 0,
            "auth_reconnect_every_secs": 0,
            "invalidate_every_secs": 0,
            "verifier_rate": 0,
            "reconnect_cycles": 10,
            "reconnect_pause_ms": 0,
            "sample_period_ms": 250,
            "warmup_secs": min(args.warmup_secs, duration // 2),
            "recovery_secs": args.recovery_secs,
        }
        load_path = temporary / "load.json"
        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = "a" * 64
        environment["NETBAIOT_BUSINESS_RPC_TOKEN"] = "v3-hol-local-test-token"
        if args.trace_gateway or args.trace_socket:
            environment["RUST_LOG"] = ("info,netbaiot_transports::business_rpc=debug"
                                       if args.trace_socket else
                                       "info,netbaiot_transports::business_rpc::v3=debug")
        if args.trace_socket:
            environment["NETBAIOT_V3_SOCKET_TRACE"] = "1"
        (output / f"{mode}-experiment.json").write_text(json.dumps({
            "git_sha": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "gateway_config": gateway_config["business_rpc"],
            "load_config": load_config,
            "proxy": None if args.no_proxy else {
                "delay_ms_per_read_per_direction": args.delay_ms,
                "bytes_per_second_per_direction": args.bytes_per_second,
                "read_limit_bytes": 16 * 1024,
            },
            "os": os.uname().sysname,
            "arch": os.uname().machine,
        }, indent=2))
        with (output / f"{mode}-gateway.log").open("w") as gateway_log, \
             (output / f"{mode}-proxy.log").open("w") as proxy_log:
            gateway = subprocess.Popen([str(GATEWAY), str(gateway_path)], cwd=ROOT,
                                       env=environment, stdout=gateway_log, stderr=subprocess.STDOUT)
            proxy_args = ["python3", str(PROXY), "--listen", f"127.0.0.1:{proxy_port}",
                          "--target", f"127.0.0.1:{business}", "--delay-ms", str(args.delay_ms),
                          "--bytes-per-second", str(args.bytes_per_second)]
            if disconnect_after_secs is not None:
                proxy_args.extend(["--disconnect-after-secs", str(disconnect_after_secs)])
            if os.environ.get("NETBAIOT_HOL_CAPTURE"):
                proxy_args.extend(["--capture-prefix", str(output / mode)])
            proxy = None if args.no_proxy else subprocess.Popen(
                proxy_args, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=proxy_log)
            try:
                load_config["gateway_pid"] = gateway.pid
                load_path.write_text(json.dumps(load_config))
                time.sleep(0.4)
                if gateway.poll() is not None or (proxy is not None and proxy.poll() is not None):
                    raise RuntimeError(f"{mode}: gateway or proxy exited before load")
                completed = subprocess.run([str(LOADGEN), str(load_path)], cwd=ROOT,
                                           env=environment, text=True, capture_output=True,
                                           timeout=duration + 60)
                (output / f"{mode}.json").write_text(completed.stdout)
                (output / f"{mode}-loadgen.log").write_text(completed.stderr)
                if completed.returncode:
                    raise RuntimeError(f"{mode}: loadgen exit {completed.returncode}; inspect logs")
                result = json.loads(completed.stdout)
                request = urllib.request.Request(
                    f"http://127.0.0.1:{management}/api/v1/metrics",
                    headers={"Authorization": f"Bearer {environment['NETBAIOT_ADMIN_SECRET']}"},
                )
                try:
                    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
                    with opener.open(request, timeout=2) as response:
                        (output / f"{mode}-metrics.txt").write_bytes(response.read())
                except OSError as error:
                    (output / f"{mode}-metrics-error.txt").write_text(str(error))
                counts = result["counts"]
                print(f"{mode}: auth={counts['success']}/{counts['requests']} "
                      f"acks={counts['event_acks']} p95={counts['latency_ms']['p95']}ms "
                      f"p99={counts['latency_ms']['p99']}ms", flush=True)
            finally:
                if proxy is not None:
                    stop(proxy)
                stop(gateway)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-secs", type=int, default=10)
    parser.add_argument("--event-payload-bytes", type=int, default=16384)
    parser.add_argument("--event-rate", type=int, default=1)
    parser.add_argument("--auth-concurrency", type=int, default=4)
    parser.add_argument("--auth-unique-devices", action="store_true")
    parser.add_argument("--delay-ms", type=float, default=25)
    parser.add_argument("--bytes-per-second", type=int, default=32768)
    parser.add_argument("--no-proxy", action="store_true")
    parser.add_argument("--trace-gateway", action="store_true")
    parser.add_argument("--trace-socket", action="store_true")
    parser.add_argument("--warmup-secs", type=int, default=2)
    parser.add_argument("--recovery-secs", type=int, default=2)
    parser.add_argument("--send-ahead-stream-bytes", type=int)
    parser.add_argument("--send-ahead-connection-bytes", type=int)
    parser.add_argument("--socket-send-buffer-bytes", type=int)
    parser.add_argument("--stream-window-bytes", type=int)
    parser.add_argument("--disconnect-after-secs", type=float)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--modes", nargs="+", choices=MODES, default=MODES)
    args = parser.parse_args()
    if not 2 <= args.duration_secs <= 120:
        parser.error("duration must be 2–120 seconds")
    if not 1 <= args.event_payload_bytes <= 16_384:
        parser.error("event payload must be 1–16384 bytes for the current codec")
    if args.stream_window_bytes is not None and not 4096 <= args.stream_window_bytes <= 4 * 1024 * 1024:
        parser.error("stream window must be 4096–4194304 bytes")
    if (args.send_ahead_stream_bytes is None) != (args.send_ahead_connection_bytes is None):
        parser.error("both send-ahead limits are required together")
    if args.send_ahead_stream_bytes is not None and not (4096 <= args.send_ahead_stream_bytes <= args.send_ahead_connection_bytes <= 16 * 1024 * 1024):
        parser.error("invalid send-ahead limits")
    if args.socket_send_buffer_bytes is not None and not 4096 <= args.socket_send_buffer_bytes <= 4 * 1024 * 1024:
        parser.error("invalid socket send buffer size")
    if not 0 <= args.event_rate <= 10000 or not 1 <= args.auth_concurrency <= 256:
        parser.error("invalid workload rate or concurrency")
    if args.delay_ms < 0 or args.bytes_per_second < 0:
        parser.error("invalid proxy parameters")
    if args.disconnect_after_secs is not None and not 0 < args.disconnect_after_secs < args.duration_secs:
        parser.error("disconnect time must be within the workload duration")
    if not GATEWAY.is_file() or not LOADGEN.is_file():
        parser.error("build release gateway and loadgen first")
    args.output.mkdir(parents=True, exist_ok=True)
    for mode in args.modes:
        if mode.startswith("v3-") and args.stream_window_bytes is not None:
            if args.stream_window_bytes < int(mode.split("-")[1]):
                parser.error("stream window must be at least max frame payload")
        if mode.startswith("v3-") and args.send_ahead_stream_bytes is not None:
            if args.send_ahead_stream_bytes < int(mode.split("-")[1]):
                parser.error("send-ahead stream limit must be at least frame payload")
        run_mode(mode, args)


if __name__ == "__main__":
    main()
