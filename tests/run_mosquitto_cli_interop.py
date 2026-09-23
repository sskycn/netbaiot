#!/usr/bin/env python3
"""Start the embedded broker on ephemeral loopback ports and run the CLI matrix."""

import json
import os
import pathlib
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request


ROOT = pathlib.Path(__file__).resolve().parents[1]
ADMIN = "d" * 64


def wait_ready(address):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(
        f"http://{address}/api/v1/ready",
        headers={"Authorization": f"Bearer {ADMIN}"},
    )
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        try:
            with opener.open(request, timeout=0.2) as response:
                if response.status == 200:
                    return
        except (OSError, urllib.error.URLError):
            time.sleep(0.03)
    raise AssertionError(f"server did not become ready on {address}")


def main():
    subprocess.run(["cargo", "build", "-p", "netbaiot-server"], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix="netbaiot-mosquitto-") as temporary:
        config = json.loads((ROOT / "configs/development.json").read_text())
        tcp_reservations = [socket.socket() for _ in range(2)]
        udp_reservation = socket.socket(type=socket.SOCK_DGRAM)
        for listener in tcp_reservations:
            listener.bind(("127.0.0.1", 0))
        udp_reservation.bind(tcp_reservations[0].getsockname())
        for field, listener in zip(
            ("device_ingress", "management_http"), tcp_reservations
        ):
            config[field] = f"127.0.0.1:{listener.getsockname()[1]}"
        config["spool_directory"] = str(pathlib.Path(temporary) / "spool")
        config_path = pathlib.Path(temporary) / "config.json"
        config_path.write_text(json.dumps(config))
        for listener in [*tcp_reservations, udp_reservation]:
            listener.close()
        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = ADMIN
        server = subprocess.Popen(
            [str(ROOT / "target/debug/netbaiot-server"), str(config_path)],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            # A raw TCP readiness probe on the MQTT listener is an invalid pre-CONNECT
            # connection. Use the authenticated management readiness contract instead.
            wait_ready(config["management_http"])
            port = config["device_ingress"].split(":")[1]
            subprocess.run(
                [
                    "python3",
                    str(ROOT / "tests/mosquitto_cli_interop.py"),
                    "--port",
                    port,
                ],
                cwd=ROOT,
                check=True,
            )
            server.terminate()
            assert server.wait(timeout=8) == 0
        finally:
            if server.poll() is None:
                server.kill()
                server.wait(timeout=3)
            diagnostics = server.stderr.read()
            if diagnostics:
                print(diagnostics, flush=True)


if __name__ == "__main__":
    main()
