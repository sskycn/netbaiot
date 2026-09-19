#!/usr/bin/env python3
"""Actual server + PostgreSQL + HTTP business sink + MQTT command smoke test.

Run after cargo build -p netbaiot-server, with DATABASE_URL pointing at a fresh
throwaway database. Uses only Python's standard library. Owns and stops every
process/thread/socket it creates. Device/admin keys are test-only.
"""
import http.client
import http.server
import json
import os
import pathlib
import signal
import socket
import struct
import subprocess
import tempfile
import threading
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
SECRET = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
ADMIN = "ab" * 32
received = []


class Sink(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        size = int(self.headers["Content-Length"])
        assert 0 < size <= 65536
        message = json.loads(self.rfile.read(size))
        assert self.headers["Idempotency-Key"] == message["message_id"]
        assert len(received) < 16
        received.append(message)
        self.send_response(204)
        self.end_headers()

    def log_message(self, *_):
        pass


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(port, path, value, token):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        connection.request("POST", path, json.dumps(value), {
            "Authorization": "Bearer " + token, "Content-Type": "application/json"
        })
        response = connection.getresponse()
        return response.status, json.loads(response.read())
    finally:
        connection.close()


def variable(n):
    out = bytearray()
    while True:
        b, n = n % 128, n // 128
        out.append(b | (128 if n else 0))
        if not n:
            return bytes(out)


def packet(first, body):
    return bytes([first]) + variable(len(body)) + body


def text(value):
    value = value.encode()
    return struct.pack("!H", len(value)) + value


def exact(s, n):
    out = bytearray()
    while len(out) < n:
        chunk = s.recv(n - len(out))
        if not chunk:
            raise EOFError("MQTT connection closed")
        out.extend(chunk)
    return bytes(out)


def read_packet(s):
    first, length, mul = exact(s, 1)[0], 0, 1
    for _ in range(4):
        byte = exact(s, 1)[0]
        length += (byte & 127) * mul
        if not byte & 128:
            assert length <= 65536
            return first, exact(s, length)
        mul *= 128
    raise AssertionError("invalid MQTT remaining length")


def main():
    assert os.environ.get("DATABASE_URL"), "DATABASE_URL must name a throwaway test database"
    sink = http.server.HTTPServer(("127.0.0.1", 0), Sink)
    thread = threading.Thread(target=sink.serve_forever)
    thread.start()
    server = None
    try:
        with tempfile.TemporaryDirectory(prefix="netbaiot-smoke-") as tmp:
            config = json.loads((ROOT / "configs/development.json").read_text())
            ports = {name: free_port() for name in ("http", "mqtt", "tcp", "udp")}
            assert len(set(ports.values())) == 4
            config.update({name: f"127.0.0.1:{port}" for name, port in ports.items()})
            config.update(development=False, delivery_url=f"http://127.0.0.1:{sink.server_port}/ingress")
            path = pathlib.Path(tmp) / "config.json"
            path.write_text(json.dumps(config))
            with (pathlib.Path(tmp) / "server.log").open("w+") as log:
                env = dict(os.environ, NETBAIOT_ADMIN_SECRET=ADMIN, RUST_LOG="warn")
                # The loopback test sink must not use a host's configured HTTP proxy.
                env["NO_PROXY"] = env["no_proxy"] = "127.0.0.1,localhost"
                server = subprocess.Popen([str(ROOT / "target/debug/netbaiot-server"), str(path)],
                                          stdout=log, stderr=log, env=env)
                for _ in range(100):
                    if server.poll() is not None:
                        log.seek(0)
                        raise AssertionError(log.read())
                    try:
                        with socket.create_connection(("127.0.0.1", ports["mqtt"]), timeout=.1):
                            break
                    except OSError:
                        time.sleep(.05)
                else:
                    raise AssertionError("server startup timed out")
                message = {"schema_version": 1, "source_message_id": "smoke:http:1",
                           "kind": "telemetry", "data": {"temperature": 25.3}}
                status, receipt = request(ports["http"], "/v1/device/messages", message, "demo-device:" + SECRET)
                assert status == 202 and receipt["boundary"] == "durable"
                status, duplicate = request(ports["http"], "/v1/device/messages", message, "demo-device:" + SECRET)
                assert status == 202 and duplicate["duplicate"] and duplicate["message_id"] == receipt["message_id"]
                prefix = "v1/t/demo/p/sensor/d/device-1/"
                with socket.create_connection(("127.0.0.1", ports["mqtt"]), timeout=3) as mqtt:
                    connect = text("MQTT") + b"\x04\xc2\x00\x1e" + text("demo-device") + text("demo-device") + text(SECRET)
                    mqtt.sendall(packet(0x10, connect))
                    assert read_packet(mqtt) == (0x20, b"\x00\x00")
                    mqtt.sendall(packet(0x82, b"\x00\x01" + text(prefix + "down") + b"\x01" + text(prefix + "up_ack") + b"\x00"))
                    assert read_packet(mqtt) == (0x90, b"\x00\x01\x01\x00")
                    message["source_message_id"] = "smoke:mqtt:1"
                    mqtt.sendall(packet(0x32, text(prefix + "up") + b"\x00\x02" + json.dumps(message).encode()))
                    assert read_packet(mqtt) == (0x40, b"\x00\x02")
                    first, body = read_packet(mqtt)
                    assert first == 0x30
                    topic_length = struct.unpack("!H", body[:2])[0]
                    assert json.loads(body[2 + topic_length:])["boundary"] == "durable"
                    command = {"command_id": str(uuid.uuid4()), "device": config["credentials"][0]["identity"]["device_key"],
                               "expires_at": int(time.time() * 1000) + 60000,
                               "payload": {"name": "set_led", "arguments": {"on": True}}}
                    status, record = request(ports["http"], "/v1/admin/commands", command, ADMIN)
                    assert status == 202 and record["execution"] == "unknown"
                    first, body = read_packet(mqtt)
                    assert first == 0x32
                    topic_length = struct.unpack("!H", body[:2])[0]
                    position = 2 + topic_length
                    mqtt.sendall(packet(0x40, body[position:position + 2]))
                    assert json.loads(body[position + 2:])["command_id"] == command["command_id"]
                    ack = {"schema_version": 1, "source_message_id": "smoke:ack:1", "kind": "command_ack",
                           "data": {"command_id": command["command_id"], "execution": "succeeded"}}
                    mqtt.sendall(packet(0x32, text(prefix + "down_ack") + b"\x00\x03" + json.dumps(ack).encode()))
                    assert read_packet(mqtt) == (0x40, b"\x00\x03")
                    assert read_packet(mqtt)[0] == 0x30
                    mqtt.sendall(b"\xe0\x00")
                for _ in range(100):
                    if len(received) >= 3:
                        break
                    time.sleep(.02)
                assert len(received) == 3, "outbox did not deliver all three unique messages"
                assert sum(m["payload"]["kind"] == "command_ack" for m in received) == 1
                server.send_signal(signal.SIGTERM)
                assert server.wait(timeout=5) == 0
                print("PASS: durable HTTP/MQTT acceptance, deduplication, outbox HTTP delivery, admin command, MQTT PUBACK, execution ACK, SIGTERM shutdown")
    finally:
        if server is not None and server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=2)
        sink.shutdown()
        sink.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


if __name__ == "__main__":
    main()
