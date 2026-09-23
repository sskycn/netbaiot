"""Small dependency-free MQTT 3.1.1 raw client and isolated broker runners."""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field


ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER = ROOT / "target/debug/netbaiot-server"
MOSQUITTO = pathlib.Path(shutil.which("mosquitto") or "/usr/local/sbin/mosquitto")
USERNAME_A = "demo-device"
USERNAME_B = "demo-device-b"
PASSWORD = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
ROOT_A = "v1/t/demo/p/sensor/d/device-1"
ROOT_B = "v1/t/demo/p/sensor/d/device-2"
TOPIC_A = f"{ROOT_A}/up"
TOPIC_B = f"{ROOT_B}/up"


def remaining(value: int) -> bytes:
    output = bytearray()
    while True:
        encoded = value % 128
        value //= 128
        if value:
            encoded |= 0x80
        output.append(encoded)
        if not value:
            return bytes(output)


def frame(first: int, body: bytes = b"") -> bytes:
    return bytes([first]) + remaining(len(body)) + body


def binary(value: bytes) -> bytes:
    if len(value) > 65535:
        raise ValueError("MQTT binary field is too long")
    return len(value).to_bytes(2, "big") + value


def connect(
    client_id: bytes,
    *,
    clean: bool = True,
    username: bytes | None = USERNAME_A.encode(),
    password: bytes | None = PASSWORD.encode(),
    keepalive: int = 30,
    protocol: bytes = b"MQTT",
    level: int = 4,
    reserved: bool = False,
    will: tuple[bytes, bytes, int, bool] | None = None,
    flags_override: int | None = None,
    tail: bytes = b"",
) -> bytes:
    flags = int(clean) << 1
    payload = bytearray(binary(client_id))
    if will is not None:
        will_topic, will_payload, will_qos, will_retain = will
        flags |= 0x04 | (will_qos << 3) | (int(will_retain) << 5)
        payload.extend(binary(will_topic))
        payload.extend(binary(will_payload))
    if username is not None:
        flags |= 0x80
        payload.extend(binary(username))
    if password is not None:
        flags |= 0x40
        payload.extend(binary(password))
    if reserved:
        flags |= 0x01
    if flags_override is not None:
        flags = flags_override
    body = binary(protocol) + bytes([level, flags]) + keepalive.to_bytes(2, "big")
    return frame(0x10, body + bytes(payload) + tail)


def publish(
    topic: str,
    payload: bytes,
    *,
    qos: int = 0,
    packet_id: int | None = None,
    retain: bool = False,
    dup: bool = False,
) -> bytes:
    body = bytearray(binary(topic.encode()))
    if qos:
        if packet_id is None:
            raise ValueError("packet identifier required")
        body.extend(packet_id.to_bytes(2, "big"))
    body.extend(payload)
    first = 0x30 | (int(dup) << 3) | (qos << 1) | int(retain)
    return frame(first, bytes(body))


def subscribe(packet_id: int, filters: list[tuple[str, int]]) -> bytes:
    body = bytearray(packet_id.to_bytes(2, "big"))
    for topic_filter, qos in filters:
        body.extend(binary(topic_filter.encode()))
        body.append(qos)
    return frame(0x82, bytes(body))


def unsubscribe(packet_id: int, filters: list[str]) -> bytes:
    body = bytearray(packet_id.to_bytes(2, "big"))
    for topic_filter in filters:
        body.extend(binary(topic_filter.encode()))
    return frame(0xA2, bytes(body))


def parse_publish(first: int, body: bytes) -> dict[str, object]:
    topic_length = int.from_bytes(body[:2], "big")
    topic_end = 2 + topic_length
    qos = (first >> 1) & 0x03
    packet_id = None
    payload_start = topic_end
    if qos:
        packet_id = int.from_bytes(body[topic_end : topic_end + 2], "big")
        payload_start += 2
    return {
        "topic": body[2:topic_end].decode(),
        "payload": body[payload_start:],
        "qos": qos,
        "packet_id": packet_id,
        "dup": bool(first & 0x08),
        "retain": bool(first & 0x01),
    }


@dataclass
class RawClient:
    host: str
    port: int
    timeout: float = 2.0
    sock: socket.socket = field(init=False)
    evidence: list[dict[str, object]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        self.sock.settimeout(self.timeout)

    def send(self, data: bytes, *, fragments: list[int] | None = None) -> None:
        wire = "<redacted-connect>" if data and data[0] >> 4 == 1 else data.hex()
        self.evidence.append(
            {"direction": "client_to_server", "hex": wire, "at": time.monotonic()}
        )
        if not fragments:
            self.sock.sendall(data)
            return
        offset = 0
        for length in fragments:
            self.sock.sendall(data[offset : offset + length])
            offset += length
        if offset < len(data):
            self.sock.sendall(data[offset:])

    def recv(self) -> tuple[int, bytes]:
        first = self._exact(1)[0]
        multiplier = 1
        length = 0
        encoded = bytearray()
        for _ in range(4):
            byte = self._exact(1)[0]
            encoded.append(byte)
            length += (byte & 0x7F) * multiplier
            if byte & 0x80 == 0:
                body = self._exact(length)
                self.evidence.append(
                    {
                        "direction": "server_to_client",
                        "first": first,
                        "body_hex": body.hex(),
                        "at": time.monotonic(),
                    }
                )
                return first, body
            multiplier *= 128
        raise AssertionError("server emitted malformed Remaining Length")

    def _exact(self, length: int) -> bytes:
        output = bytearray()
        while len(output) < length:
            chunk = self.sock.recv(length - len(output))
            if not chunk:
                raise EOFError("connection closed")
            output.extend(chunk)
        return bytes(output)

    def expect_closed(self, timeout: float = 2.0) -> None:
        self.sock.settimeout(timeout)
        try:
            data = self.sock.recv(1)
        except (ConnectionResetError, BrokenPipeError):
            return
        assert data == b"", f"expected close, received {data.hex()}"

    def close(self, mqtt_disconnect: bool = False) -> None:
        if mqtt_disconnect:
            try:
                self.send(b"\xe0\x00")
                self.sock.shutdown(socket.SHUT_WR)
                self.sock.settimeout(0.5)
                while self.sock.recv(256):
                    pass
            except OSError:
                pass
        self.sock.close()


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def wait_port(port: int, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr else ""
            raise AssertionError(f"broker exited during startup: {stderr}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.03)
    raise AssertionError(f"broker did not listen on {port}")


@dataclass
class BrokerProcess:
    process: subprocess.Popen[str]
    port: int
    root: pathlib.Path
    config_path: pathlib.Path | None = None

    def stop(self, *, expect_success: bool = True) -> None:
        if self.process.poll() is None:
            self.process.terminate()
        code = self.process.wait(timeout=10)
        if expect_success:
            assert code == 0, self.process.stderr.read() if self.process.stderr else code


def netbaiot_config(root: pathlib.Path, port: int) -> pathlib.Path:
    config = json.loads((ROOT / "configs/development.json").read_text())
    config["management_http"] = f"127.0.0.1:{free_port()}"
    config["device_ingress"] = f"127.0.0.1:{port}"
    config["spool_directory"] = str(root / "spool")
    config["limits"] = {
        "connect_timeout_ms": 1000,
        "packet_read_timeout_ms": 500,
        "idle_timeout_ms": 3000,
        "requests_per_second": 10000,
        "requests_per_ip_second": 10000,
        "messages_per_device_second": 10000,
        "messages_per_tenant_second": 10000,
    }
    second = json.loads(json.dumps(config["credentials"][0]))
    second["credential_id"] = USERNAME_B
    second["identity"]["device_key"]["device_id"] = "device-2"
    config["credentials"].append(second)
    path = root / "netbaiot.json"
    path.write_text(json.dumps(config))
    return path


def start_netbaiot(root: pathlib.Path, *, port: int | None = None) -> BrokerProcess:
    port = port or free_port()
    config_path = netbaiot_config(root, port)
    return start_netbaiot_config(root, config_path, port)


def start_netbaiot_config(
    root: pathlib.Path, config_path: pathlib.Path, port: int
) -> BrokerProcess:
    environment = os.environ.copy()
    environment["NETBAIOT_ADMIN_SECRET"] = "d" * 64
    process = subprocess.Popen(
        [str(SERVER), str(config_path)],
        cwd=ROOT,
        env=environment,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    wait_port(port, process)
    return BrokerProcess(process, port, root, config_path)


def start_mosquitto(root: pathlib.Path, *, port: int | None = None) -> BrokerProcess:
    if not MOSQUITTO.exists():
        raise FileNotFoundError(MOSQUITTO)
    port = port or free_port()
    config_path = root / "mosquitto.conf"
    config_path.write_text(
        "\n".join(
            [
                f"listener {port} 127.0.0.1",
                "allow_anonymous true",
                "persistence false",
                "log_type error",
            ]
        )
        + "\n"
    )
    process = subprocess.Popen(
        [str(MOSQUITTO), "-c", str(config_path)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    wait_port(port, process)
    return BrokerProcess(process, port, root, config_path)


def temporary_root(prefix: str) -> tempfile.TemporaryDirectory[str]:
    return tempfile.TemporaryDirectory(prefix=prefix)
