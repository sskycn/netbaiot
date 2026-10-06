#!/usr/bin/env python3
"""Execute the extracted binary package's documented MQTT -> webhook demo."""

import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from release_package import TARGETS, validate_archive


def wait_for(process, predicate, seconds, message):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"demo exited with {process.returncode}: {message}")
        if predicate():
            return
        time.sleep(0.1)
    raise RuntimeError(f"timeout: {message}")


def stop_group(process):
    # The shell owns the server and webhook. TERM allows its trap to drain the
    # server before stopping the sink; KILL is only a bounded failure cleanup.
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            raise RuntimeError("demo did not stop within 30 seconds")
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def run_demo(root):
    if os.name == "nt":
        raise RuntimeError("Bash archive smoke must run on Linux/macOS")
    publisher = shutil.which("mosquitto_pub")
    if publisher is None:
        raise RuntimeError("mosquitto_pub is required")
    for kind, port in [(socket.SOCK_STREAM, 8080), (socket.SOCK_DGRAM, 8080),
                       (socket.SOCK_STREAM, 9090), (socket.SOCK_STREAM, 18080)]:
        with socket.socket(socket.AF_INET, kind) as sock:
            sock.bind(("127.0.0.1", port))
    with tempfile.TemporaryDirectory(prefix="netbaiot-smoke-") as temporary:
        working = Path(temporary)
        guard = working / "bin"
        guard.mkdir()
        # Prove that the downloaded archive does not fall back to Cargo/Rust.
        (guard / "cargo").write_text("#!/bin/sh\necho 'unexpected Cargo invocation' >&2\nexit 99\n")
        (guard / "cargo").chmod(0o755)
        environment = os.environ.copy()
        environment["PATH"] = f"{guard}{os.pathsep}{environment.get('PATH', '')}"
        environment["TMPDIR"] = str(working)
        environment["RUST_LOG"] = "info"
        # The preferred first-use path invokes only the extracted Rust binary.
        cli = root / "netbaiot"
        for arguments in (["version"], ["config", "check", "--config", "configs/tutorial.json"], ["demo", "--once"]):
            native_environment = environment.copy()
            native_environment["PATH"] = ""  # no Python, Mosquitto or Cargo fallback
            native = subprocess.run([str(cli), *arguments], cwd=root, env=native_environment,
                check=True, capture_output=True, text=True, timeout=30)
            if arguments[0] == "demo":
                for stage in ("Gateway started", "Demo device authenticated", "Heartbeat EventAccepted", "Business sink acknowledged", "Shutdown completed"):
                    if stage not in native.stdout:
                        raise RuntimeError(f"native demo missing stage: {stage}")
                if list(working.glob("netbaiot-demo-*")):
                    raise RuntimeError("native demo left temporary recovery storage")
        native_environment = environment.copy()
        native_environment["PATH"] = ""
        schema = subprocess.run([str(cli), "config", "schema"], cwd=root, env=native_environment,
            check=True, capture_output=True, text=True, timeout=30)
        if json.loads(schema.stdout) != json.loads((root / "docs/schema/netbaiot-config.schema.json").read_text()):
            raise RuntimeError("packaged CLI schema does not match packaged schema file")
        project = working / "native-project"
        subprocess.run([str(cli), "init", str(project)], cwd=root, env=native_environment,
            check=True, capture_output=True, text=True, timeout=30)
        config_path = project / "netbaiot.json"
        config = json.loads(config_path.read_text())
        config["device_ingress"] = "127.0.0.1:0"
        config["management_http"] = "127.0.0.1:0"
        config_path.write_text(json.dumps(config))
        for command in (["config", "check"], ["doctor"]):
            subprocess.run([str(cli), *command, "--config", str(config_path)], cwd=root,
                env=native_environment, check=True, capture_output=True, text=True, timeout=30)
        production = working / "production-project"
        subprocess.run([str(cli), "init", "--production", str(production)], cwd=root,
            env=native_environment, check=True, capture_output=True, text=True, timeout=30)
        production_config = json.loads((production / "netbaiot.json").read_text())
        if production_config["credentials"] or production_config["development"]:
            raise RuntimeError("production skeleton contains development credentials")
        print("Archive native CLI PASS: version, config check, demo --once, init, doctor, schema, production skeleton, empty PATH")
        log = working / "demo.log"
        with log.open("w") as output:
            process = subprocess.Popen(["bash", "scripts/demo/start.sh"], cwd=root,
                env=environment, stdin=subprocess.DEVNULL, stdout=output,
                stderr=subprocess.STDOUT, start_new_session=True)
            try:
                def ready():
                    request = urllib.request.Request("http://127.0.0.1:9090/api/v1/ready",
                        headers={"Authorization": "Bearer " + "ab" * 32})
                    try:
                        with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(
                                request, timeout=0.5) as response:
                            return response.status == 200 and json.load(response).get("ready") is True
                    except (OSError, urllib.error.URLError):
                        return False
                wait_for(process, ready, 20, "management readiness")
                subprocess.run([publisher, "-h", "127.0.0.1", "-p", "8080", "-V", "mqttv311",
                    "-u", "demo-device", "-P", "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
                    "-i", "quickstart", "-t", "v1/t/demo/p/sensor/d/device-1/up", "-q", "1",
                    "-m", '{"schema_version":1,"source_message_id":"demo:1","kind":"heartbeat","data":{"sequence":1}}'],
                    check=True, capture_output=True, timeout=10)
                def event_received():
                    for line in log.read_text().splitlines():
                        if not line.startswith("{"):
                            continue
                        row = json.loads(line)
                        event = row.get("event", {})
                        if event.get("source_message_id") == "demo:1":
                            if (not event.get("event_id") or event.get("tenant_id") != "demo"
                                    or event.get("product_id") != "sensor" or event.get("device_id") != "device-1"
                                    or event.get("event_type") != "heartbeat"
                                    or event.get("payload", {}).get("data", {}).get("sequence") != 1):
                                raise RuntimeError("incorrect normalized DeviceEvent")
                            return True
                    return False
                wait_for(process, event_received, 10, "normalized webhook DeviceEvent")
                # Exercise the other examples actually promised by Quick Start.
                for example in ("device_tcp.py", "device_udp.py"):
                    subprocess.run([sys.executable, f"examples/{example}", "--address", "127.0.0.1:8080"],
                        cwd=root, check=True, capture_output=True, timeout=15)
                process.terminate()
                process.wait(timeout=30)
                if process.returncode != 143 or "shutdown complete" not in log.read_text():
                    raise RuntimeError(f"missing graceful shutdown; demo exit {process.returncode}")
                if list(working.glob("netbaiot-demo.*")):
                    raise RuntimeError("demo did not remove temporary config/spool")
            except BaseException:
                print(log.read_text()[-8000:], file=sys.stderr)
                raise
            finally:
                stop_group(process)
    print("Archive smoke PASS: packaged Bash demo, no Cargo, MQTT QoS1, DeviceEvent webhook, TCP/UDP, graceful stop")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    target = next((target for target in TARGETS if args.archive.name.endswith(f"-{target}.tar.gz")), None)
    if target is None or not args.archive.name.startswith("netbaiot-v"):
        parser.error("expected a named NetbaIoT platform archive")
    tag = args.archive.name[len("netbaiot-"):-len(f"-{target}.tar.gz")]
    prefix = validate_archive(args.archive, tag, target)
    with tempfile.TemporaryDirectory(prefix="netbaiot-extracted-") as temporary:
        extracted = Path(temporary)
        with tarfile.open(args.archive, "r:gz") as bundle:
            # validate_archive already rejects traversal, links, special files,
            # duplicate members, excessive counts and uncompressed sizes.
            for member in bundle:
                path = extracted / member.name
                if member.isdir():
                    path.mkdir(parents=True, exist_ok=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    with bundle.extractfile(member) as source, path.open("wb") as destination:
                        shutil.copyfileobj(source, destination)
                    path.chmod(member.mode & 0o777)
        run_demo(extracted / prefix)


if __name__ == "__main__":
    main()
