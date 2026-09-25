#!/usr/bin/env python3
"""External broker smoke for the MQTT Device Profile SDK example."""

import os
from pathlib import Path
import select
import shutil
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "target/debug/examples/device_mqtt"
CERT = ROOT / "tests/fixtures/localhost-cert.pem"
KEY = ROOT / "tests/fixtures/localhost-key.pem"
MOSQUITTO = shutil.which("mosquitto") or next(
    (str(path) for path in (Path("/usr/local/sbin/mosquitto"),
                            Path("/usr/sbin/mosquitto"),
                            Path("/opt/homebrew/sbin/mosquitto")) if path.exists()),
    None,
)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run_broker(tls):
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="netbaiot-mosquitto-profile-") as tmp:
        config = Path(tmp) / "mosquitto.conf"
        lines = [f"listener {port} 127.0.0.1", "allow_anonymous true", "persistence false", "log_type error"]
        if tls:
            lines += [f"certfile {CERT}", f"keyfile {KEY}", "require_certificate false"]
        config.write_text("\n".join(lines) + "\n")
        with (Path(tmp) / "mosquitto.log").open("w") as log:
            broker = subprocess.Popen([MOSQUITTO, "-c", str(config)], stdout=log, stderr=log)
            try:
                for _ in range(100):
                    try:
                        with socket.create_connection(("127.0.0.1", port), 0.1):
                            break
                    except OSError:
                        time.sleep(0.02)
                else:
                    raise RuntimeError(f"Mosquitto did not start: {Path(tmp, 'mosquitto.log').read_text()}")
                for version in ("311", "5"):
                    env = dict(os.environ)
                    env.update(
                        NETBAIOT_MQTT_ENDPOINT=f"{'mqtts' if tls else 'mqtt'}://127.0.0.1:{port}",
                        NETBAIOT_DEVICE_CREDENTIAL_ID="interop",
                        NETBAIOT_DEVICE_SECRET="test-secret",
                        NETBAIOT_MQTT_VERSION=version,
                    )
                    if tls:
                        env["NETBAIOT_MQTT_CA_PEM"] = str(CERT)
                    subprocess.run([str(EXAMPLE)], env=env, cwd=ROOT, check=True, timeout=15)
                    print(f"Mosquitto profile {version} {'TLS' if tls else 'TCP'} passed")
                    if not tls and version == "311":
                        env["NETBAIOT_PROBE_MODE"] = "puback"
                        probe = subprocess.run([str(EXAMPLE)], env=env, cwd=ROOT,
                                               check=True, timeout=20, capture_output=True, text=True)
                        print(probe.stdout.strip())
                        env.pop("NETBAIOT_PROBE_MODE")
                    if tls and version == "311":
                        env.pop("NETBAIOT_MQTT_CA_PEM")
                        failure = subprocess.run([str(EXAMPLE)], env=env, cwd=ROOT, capture_output=True, text=True, timeout=15)
                        assert failure.returncode != 0 and "Unauthenticated" in failure.stderr, failure.stderr
                        print("Mosquitto untrusted CA rejected")
            finally:
                broker.terminate()
                broker.wait(timeout=5)


def read_line(pipe, timeout):
    deadline = time.monotonic() + timeout
    result = bytearray()
    while time.monotonic() < deadline:
        if not select.select([pipe], [], [], max(0, deadline - time.monotonic()))[0]:
            break
        byte = os.read(pipe.fileno(), 1)
        if byte == b"\n":
            return result.decode()
        if not byte:
            break
        result.extend(byte)
    raise TimeoutError(f"client output stopped after {result!r}")


def run_persistent_reconnect(version, cycles=5):
    port = free_port()
    with tempfile.TemporaryDirectory(prefix="netbaiot-mosquitto-reconnect-") as tmp:
        config = Path(tmp) / "mosquitto.conf"
        config.write_text(
            f"listener {port} 127.0.0.1\nallow_anonymous true\n"
            f"persistence true\npersistence_location {tmp}/\nlog_type error\n"
        )
        log = (Path(tmp) / "mosquitto.log").open("w")
        command = [MOSQUITTO, "-c", str(config)]
        broker = subprocess.Popen(command, stdout=log, stderr=log)
        child = None
        try:
            for _ in range(100):
                try:
                    with socket.create_connection(("127.0.0.1", port), 0.1):
                        break
                except OSError:
                    time.sleep(0.02)
            else:
                raise RuntimeError("Mosquitto did not start")
            env = dict(os.environ)
            env.update(
                NETBAIOT_MQTT_ENDPOINT=f"mqtt://127.0.0.1:{port}",
                NETBAIOT_DEVICE_CREDENTIAL_ID="interop",
                NETBAIOT_DEVICE_SECRET="test-secret",
                NETBAIOT_MQTT_VERSION=version,
                NETBAIOT_RECONNECT_CHECK="1",
                NETBAIOT_RECONNECT_CYCLES=str(cycles),
            )
            child = subprocess.Popen(
                [str(EXAMPLE)], env=env, cwd=ROOT, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, bufsize=1,
            )
            rss_samples = []
            for _ in range(cycles):
                assert read_line(child.stdout, 15) == "READY_FOR_RESTART"
                rss_samples.append(int(subprocess.check_output(
                    ["ps", "-o", "rss=", "-p", str(child.pid)], text=True).strip()))
                broker.terminate()
                broker.wait(timeout=5)
                broker = subprocess.Popen(command, stdout=log, stderr=log)
                assert read_line(child.stdout, 15) == "RECONNECTED"
            out, err = child.communicate(timeout=25)
            assert child.returncode == 0, (out, err)
            print(f"Mosquitto profile {version} persistent reconnect {cycles} cycles passed; "
                  f"RSS KiB={rss_samples}")
        finally:
            if child and child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            if broker.poll() is None:
                broker.terminate()
                broker.wait(timeout=5)
            log.close()


def run_tls_certificate_failures():
    def openssl(*args):
        result = subprocess.run(["openssl", *args], capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())

    with tempfile.TemporaryDirectory(prefix="netbaiot-mosquitto-negative-tls-") as tmp:
        base = Path(tmp)
        (base / "newcerts").mkdir()
        (base / "index.txt").write_text("")
        (base / "serial").write_text("01\n")
        openssl_config = base / "openssl.cnf"
        openssl_config.write_text(f"""
[req]
distinguished_name = request_dn
[request_dn]
commonName = Common Name
[ca]
default_ca = local_ca
[local_ca]
dir = {tmp}
database = $dir/index.txt
new_certs_dir = $dir/newcerts
certificate = $dir/ca.pem
private_key = $dir/ca.key
serial = $dir/serial
default_md = sha256
default_days = 365
policy = test_policy
unique_subject = no
[test_policy]
commonName = supplied
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
[server_wrong]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:wrong.invalid
[server_expired]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = IP:127.0.0.1
""")
        openssl("req", "-new", "-x509", "-newkey", "rsa:2048",
                "-nodes", "-keyout", str(base / "ca.key"), "-out", str(base / "ca.pem"),
                "-days", "3650", "-subj", "/CN=Device Profile Test CA",
                "-config", str(openssl_config), "-extensions", "v3_ca")
        for kind in ("wrong", "expired"):
            key = base / f"{kind}.key"
            cert = base / f"{kind}.pem"
            csr = base / f"{kind}.csr"
            openssl("req", "-new", "-newkey", "rsa:2048", "-nodes",
                    "-keyout", str(key), "-out", str(csr), "-subj", f"/CN={kind}")
            command = ["openssl", "ca", "-batch", "-config", str(openssl_config),
                       "-extensions", f"server_{kind}", "-in", str(csr), "-out", str(cert)]
            if kind == "expired":
                command += ["-startdate", "20200101000000Z", "-enddate", "20200102000000Z"]
            openssl(*command[1:])
            port = free_port()
            config = base / f"mosquitto-{kind}.conf"
            config.write_text(f"listener {port} 127.0.0.1\nallow_anonymous true\n"
                              f"certfile {cert}\nkeyfile {key}\nlog_type error\n")
            with (base / f"mosquitto-{kind}.log").open("w") as log:
                broker = subprocess.Popen(
                    [MOSQUITTO, "-c", str(config)],
                    stdout=log, stderr=log)
                try:
                    for _ in range(100):
                        try:
                            with socket.create_connection(("127.0.0.1", port), 0.1):
                                break
                        except OSError:
                            time.sleep(0.02)
                    else:
                        raise RuntimeError(f"Mosquitto {kind} TLS listener did not start")
                    env = dict(os.environ)
                    env.update(NETBAIOT_MQTT_ENDPOINT=f"mqtts://127.0.0.1:{port}",
                               NETBAIOT_DEVICE_CREDENTIAL_ID="interop",
                               NETBAIOT_DEVICE_SECRET="test-secret",
                               NETBAIOT_MQTT_CA_PEM=str(base / "ca.pem"))
                    failure = subprocess.run([str(EXAMPLE)], env=env, cwd=ROOT,
                                             capture_output=True, text=True, timeout=15)
                    assert failure.returncode != 0 and "Unauthenticated" in failure.stderr, failure.stderr
                    print(f"Mosquitto {kind} TLS certificate rejected")
                finally:
                    broker.terminate()
                    broker.wait(timeout=5)


if __name__ == "__main__":
    if MOSQUITTO is None:
        raise SystemExit("Mosquitto executable is required")
    if not EXAMPLE.exists():
        raise SystemExit("build the example first: cargo build --locked -p netbaiot-device-sdk --example device_mqtt")
    run_broker(False)
    run_broker(True)
    run_persistent_reconnect("311")
    run_persistent_reconnect("5")
    run_tls_certificate_failures()
