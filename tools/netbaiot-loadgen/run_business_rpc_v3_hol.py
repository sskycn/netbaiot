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


def run_mode(mode, duration, output, event_payload_bytes, stream_window_bytes, disconnect_after_secs):
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
                    "max_frame_payload_bytes": 16384,
                    "max_concurrent_streams": 256,
                    "initial_stream_window_bytes": 262144,
                    "initial_connection_window_bytes": 4194304,
                    "heartbeat_ms": 5000,
                },
                "tls": None,
                "development_token_env": "NETBAIOT_BUSINESS_RPC_TOKEN",
            },
        })
        gateway_path = temporary / "gateway.json"
        gateway_path.write_text(json.dumps(gateway_config))
        load_config = {
            "scenario": "multiplexed",
            "business_address": f"127.0.0.1:{proxy_port}",
            "topology": "v3" if mode.startswith("v3-") else mode,
            "frame_payload_bytes": frame_size if mode.startswith("v3-") else None,
            "stream_window_bytes": stream_window_bytes if mode.startswith("v3-") else None,
            "network_profile": "user_proxy_25ms_each_direction_32768Bps",
            "device_address": f"127.0.0.1:{device}",
            "udp_address": f"127.0.0.1:{device}",
            "management_url": f"http://127.0.0.1:{management}",
            "duration_secs": duration,
            "auth_concurrency": 4,
            "auth_handler_delay_ms": 0,
            "event_rate": 1,
            "event_payload_bytes": event_payload_bytes,
            "event_ack_delay_ms": 0,
            "event_reconnect_every_secs": 0,
            "auth_reconnect_every_secs": 0,
            "invalidate_every_secs": 0,
            "verifier_rate": 0,
            "reconnect_cycles": 10,
            "reconnect_pause_ms": 0,
            "sample_period_ms": 250,
            "warmup_secs": min(2, duration // 2),
            "recovery_secs": 2,
        }
        load_path = temporary / "load.json"
        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = "a" * 64
        environment["NETBAIOT_BUSINESS_RPC_TOKEN"] = "v3-hol-local-test-token"
        with (output / f"{mode}-gateway.log").open("w") as gateway_log, \
             (output / f"{mode}-proxy.log").open("w") as proxy_log:
            gateway = subprocess.Popen([str(GATEWAY), str(gateway_path)], cwd=ROOT,
                                       env=environment, stdout=gateway_log, stderr=subprocess.STDOUT)
            proxy_args = ["python3", str(PROXY), "--listen", f"127.0.0.1:{proxy_port}",
                          "--target", f"127.0.0.1:{business}", "--delay-ms", "25",
                          "--bytes-per-second", "32768"]
            if disconnect_after_secs is not None:
                proxy_args.extend(["--disconnect-after-secs", str(disconnect_after_secs)])
            if os.environ.get("NETBAIOT_HOL_CAPTURE"):
                proxy_args.extend(["--capture-prefix", str(output / mode)])
            proxy = subprocess.Popen(proxy_args, cwd=ROOT,
                                     stdout=subprocess.DEVNULL, stderr=proxy_log)
            try:
                load_config["gateway_pid"] = gateway.pid
                load_path.write_text(json.dumps(load_config))
                time.sleep(0.4)
                if gateway.poll() is not None or proxy.poll() is not None:
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
                stop(proxy)
                stop(gateway)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-secs", type=int, default=10)
    parser.add_argument("--event-payload-bytes", type=int, default=16384)
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
    if args.disconnect_after_secs is not None and not 0 < args.disconnect_after_secs < args.duration_secs:
        parser.error("disconnect time must be within the workload duration")
    if not GATEWAY.is_file() or not LOADGEN.is_file():
        parser.error("build release gateway and loadgen first")
    args.output.mkdir(parents=True, exist_ok=True)
    for mode in args.modes:
        if mode.startswith("v3-") and args.stream_window_bytes is not None:
            if args.stream_window_bytes < int(mode.split("-")[1]):
                parser.error("stream window must be at least max frame payload")
        run_mode(mode, args.duration_secs, args.output, args.event_payload_bytes,
                 args.stream_window_bytes, args.disconnect_after_secs)


if __name__ == "__main__":
    main()
