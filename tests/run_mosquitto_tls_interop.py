#!/usr/bin/env python3
"""Run the MQTT 3.1.1 Mosquitto CLI matrix through verified TLS."""

import json
import os
import pathlib
import socket
import subprocess
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parents[1]
CERTIFICATE = ROOT / "tests/fixtures/localhost-cert.pem"
PRIVATE_KEY = ROOT / "tests/fixtures/localhost-key.pem"
PASSWORD = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
TOPIC = "v1/t/demo/p/sensor/d/device-1/up"


def free_address():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{listener.getsockname()[1]}"


def wait_port(address, process):
    host, port = address.split(":")
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(process.stderr.read())
        try:
            with socket.create_connection((host, int(port)), timeout=0.2):
                return
        except OSError:
            time.sleep(0.03)
    raise AssertionError(f"server did not listen on {address}")


def main():
    subprocess.run(["cargo", "build", "-p", "netbaiot-server"], cwd=ROOT, check=True)
    with tempfile.TemporaryDirectory(prefix="netbaiot-mosquitto-tls-") as temporary:
        config = json.loads((ROOT / "configs/development.json").read_text())
        config["device_http"] = free_address()
        config["management_http"] = free_address()
        config["mqtt"] = free_address()
        config["tcp"] = free_address()
        config["udp"] = free_address()
        config["spool_directory"] = str(pathlib.Path(temporary) / "spool")
        config["tls"] = {
            "certificate": str(CERTIFICATE),
            "private_key": str(PRIVATE_KEY),
        }
        config_path = pathlib.Path(temporary) / "config.json"
        config_path.write_text(json.dumps(config))
        environment = os.environ.copy()
        environment["NETBAIOT_ADMIN_SECRET"] = "d" * 64
        server = subprocess.Popen(
            [str(ROOT / "target/debug/netbaiot-server"), str(config_path)],
            cwd=ROOT,
            env=environment,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            wait_port(config["mqtt"], server)
            port = config["mqtt"].split(":")[1]
            base = [
                "/usr/local/bin/mosquitto_pub",
                "-h",
                "127.0.0.1",
                "-p",
                port,
                "-V",
                "mqttv311",
                "-d",
                "-u",
                "demo-device",
                "-P",
                PASSWORD,
                "-i",
                "mosq-tls",
                "-t",
                TOPIC,
                "-q",
                "1",
                "-m",
                '{"schema_version":1,"source_message_id":"mosq-tls","kind":"heartbeat","data":{"sequence":1}}',
            ]
            trusted = subprocess.run(
                [*base, "--cafile", str(CERTIFICATE)],
                capture_output=True,
                text=True,
                timeout=8,
            )
            assert trusted.returncode == 0, (trusted.stdout, trusted.stderr)
            untrusted = subprocess.run(base, capture_output=True, text=True, timeout=8)
            assert untrusted.returncode != 0, "self-signed server certificate unexpectedly trusted"
            print(
                json.dumps(
                    {
                        "protocol": "mqttv311",
                        "trusted_ca_and_hostname_verification": "pass",
                        "untrusted_ca_rejected": "pass",
                    },
                    indent=2,
                )
            )
            server.terminate()
            assert server.wait(timeout=8) == 0
        finally:
            if server.poll() is None:
                server.kill()
                server.wait(timeout=3)
            diagnostics = server.stderr.read() if server.stderr else ""
            if diagnostics:
                print(diagnostics)


if __name__ == "__main__":
    main()
