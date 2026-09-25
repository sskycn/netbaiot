#!/usr/bin/env python3
"""Local, reproducible Device Profile process RSS and queue-pressure probe.

This is a measurement aid, not a production capacity benchmark. The fake broker
intentionally withholds PUBACK in the saturated case.
"""

import os
from pathlib import Path
import select
import socket
import subprocess
import threading

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "target/debug/examples/device_mqtt"


def packet(stream):
    first = stream.recv(1)
    if not first:
        return None
    remaining = 0
    multiplier = 1
    for _ in range(4):
        part = stream.recv(1)
        if not part:
            return None
        remaining += (part[0] & 127) * multiplier
        if not part[0] & 128:
            break
        multiplier *= 128
    if remaining > 131_072:
        raise ValueError("unexpected oversized packet")
    body = bytearray()
    while len(body) < remaining:
        chunk = stream.recv(remaining - len(body))
        if not chunk:
            return None
        body.extend(chunk)
    return first[0], body


def broker(listener, stop):
    listener.settimeout(0.2)
    try:
        while not stop.is_set():
            try:
                stream, _ = listener.accept()
            except socket.timeout:
                continue
            with stream:
                stream.settimeout(5)
                if packet(stream) is None:
                    continue
                stream.sendall(bytes.fromhex("20 02 00 00"))
                subscribed = packet(stream)
                if subscribed is None or subscribed[0] != 0x82:
                    continue
                packet_id = subscribed[1][:2]
                stream.sendall(b"\x90\x03" + packet_id + b"\x01")
                try:
                    while packet(stream) is not None:
                        pass
                except socket.timeout:
                    pass
    finally:
        listener.close()


def rss_kib(pid):
    return int(subprocess.check_output(["ps", "-o", "rss=", "-p", str(pid)], text=True).strip())


def thread_count(pid):
    task = Path(f"/proc/{pid}/task")
    if task.is_dir():
        return len(list(task.iterdir()))
    output = subprocess.check_output(["ps", "-M", "-p", str(pid)], text=True)
    return max(0, len(output.splitlines()) - 1)


def probe(mode):
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    stop = threading.Event()
    worker = threading.Thread(target=broker, args=(listener, stop), daemon=True)
    worker.start()
    env = dict(os.environ)
    env.update(
        NETBAIOT_MQTT_ENDPOINT=f"mqtt://127.0.0.1:{listener.getsockname()[1]}",
        NETBAIOT_DEVICE_CREDENTIAL_ID="measurement",
        NETBAIOT_DEVICE_SECRET="measurement-secret",
        NETBAIOT_PROBE_MODE=mode,
    )
    child = subprocess.Popen([str(EXAMPLE)], env=env, cwd=ROOT,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert select.select([child.stdout], [], [], 10)[0], "probe did not connect"
        marker = child.stdout.readline().strip()
        assert marker.startswith(f"PROBE_READY {mode} "), marker
        rss = rss_kib(child.pid)
        threads = thread_count(child.pid)
        out, err = child.communicate(timeout=10)
        assert child.returncode == 0, (out, err)
        return marker, rss, threads
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        stop.set()
        worker.join(timeout=2)


if __name__ == "__main__":
    if not EXAMPLE.exists():
        raise SystemExit("build the example first")
    for mode in ("idle", "saturated"):
        marker, rss, threads = probe(mode)
        print(f"{marker}: RSS={rss} KiB, OS threads={threads}")
