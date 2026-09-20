#!/usr/bin/env python3
"""Start the embedded broker on ephemeral loopback ports and run the CLI matrix."""

import json
import os
import pathlib
import socket
import subprocess
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]
ADMIN = "d" * 64


def tcp_address():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{listener.getsockname()[1]}"


def udp_address():
    with socket.socket(type=socket.SOCK_DGRAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{listener.getsockname()[1]}"


def wait_port(address):
    host, port = address.split(":")
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=0.2):
                return
        except OSError:
            time.sleep(0.03)
    raise AssertionError(f"server did not listen on {address}")


def main():
    subprocess.run(["cargo", "build", "-p", "netbaiot-server"], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix="netbaiot-mosquitto-") as temporary:
        config = json.loads((ROOT / "configs/development.json").read_text())
        config["device_http"] = tcp_address()
        config["management_http"] = tcp_address()
        config["mqtt"] = tcp_address()
        config["tcp"] = tcp_address()
        config["udp"] = udp_address()
        config["spool_directory"] = str(pathlib.Path(temporary) / "spool")
        config_path = pathlib.Path(temporary) / "config.json"
        config_path.write_text(json.dumps(config))
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
            wait_port(config["mqtt"])
            port = config["mqtt"].split(":")[1]
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
