#!/usr/bin/env python3
"""Extra MQTT regression cases for sskycn/netbaiot.

Audit baseline: e9c9b06658ed811b1261c0cd40bf07a8df485ee6.
These are proposed regression tests, NOT tests already run against the Rust broker.
Each case starts an isolated LOCAL broker using the repository's existing fixtures
and synthetic demo credentials. Do not adapt them to a production listener.

Usage:
  cargo build --locked -p netbaiot-server
  python3 mqtt_protocol_regressions.py --repo /path/to/netbaiot --output results.json

The binary must be rebuilt from the checkout being tested. Set
NETBAIOT_TEST_SERVER to use an isolated binary. The output records
the binary hash and source-diff hashes; they identify artifacts but cannot prove
which source produced a preexisting binary without a witnessed build.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time
import traceback
from types import ModuleType
from typing import Callable

AUDIT_SHA = "e9c9b06658ed811b1261c0cd40bf07a8df485ee6"
C: ModuleType
V: ModuleType
R: ModuleType


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


class Case:
    def __init__(self, broker) -> None:
        self.broker = broker
        self.port = broker.port
        self.clients: list[object] = []

    def client(self):
        client = C.RawClient("127.0.0.1", self.port)
        self.clients.append(client)
        return client

    def v5(self, name: str, **options):
        client = self.client()
        body = V.connect(client, name, **options)
        return client, body

    def v311(self, name: str, *, clean: bool = True):
        client = self.client()
        client.send(C.connect(name.encode(), clean=clean))
        first, body = client.recv()
        require(first == 0x20 and len(body) == 2 and body[1] == 0,
                f"3.1.1 CONNECT rejected: {(first, body)!r}")
        return client

    def close(self) -> None:
        for client in reversed(self.clients):
            try:
                client.close(True)
            except (OSError, EOFError):
                pass

    def evidence(self) -> list[dict]:
        return [{"connection_index": i, "packets": client.evidence}
                for i, client in enumerate(self.clients)]


def ack(packet: tuple[int, bytes], kind: int, packet_id: int,
        *, success: bool = True) -> int:
    first, body = packet
    require(first == kind and len(body) >= 2 and
            int.from_bytes(body[:2], "big") == packet_id,
            f"Expected ACK {kind:#04x}/{packet_id}, got {packet!r}")
    reason = body[2] if len(body) > 2 else 0
    if len(body) > 2:
        require(len(body) >= 4, f"Missing MQTT 5 ACK Property Length: {packet!r}")
        property_length, at = variable(body, 3)
        require(at + property_length == len(body), f"Bad MQTT 5 ACK properties: {packet!r}")
    if success:
        require(reason < 0x80, f"Unexpected negative ACK: {packet!r}")
    return reason


def publish5(client, packet_id: int, payload: bytes, *, qos: int = 2,
             properties: bytes = b"", dup: bool = False) -> None:
    body = (C.binary(C.TOPIC_A.encode()) + packet_id.to_bytes(2, "big")
            + C.remaining(len(properties)) + properties + payload)
    client.send(C.frame(0x30 | (qos << 1) | (int(dup) << 3), body))


def variable(data: bytes, at: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 28, 7):
        if at >= len(data):
            raise ValueError("truncated Variable Byte Integer")
        byte = data[at]
        at += 1
        value |= (byte & 0x7f) << shift
        if not byte & 0x80:
            return value, at
    raise ValueError("invalid Variable Byte Integer")


def disconnect_reason(packet: tuple[int, bytes]) -> int:
    first, body = packet
    require(first == 0xe0, f"Expected DISCONNECT, got {packet!r}")
    if not body:
        return 0  # Compact normal DISCONNECT.
    require(len(body) >= 2, f"Missing DISCONNECT Property Length: {packet!r}")
    property_length, at = variable(body, 1)
    require(at + property_length == len(body), f"Bad DISCONNECT properties: {packet!r}")
    return body[0]


def connack_properties(body: bytes) -> dict[int, object]:
    length, at = variable(body, 2)
    end = at + length
    require(end == len(body), "bad CONNACK property length")
    output: dict[int, object] = {}
    byte_ids = {0x24, 0x25, 0x28, 0x29, 0x2a}
    short_ids = {0x13, 0x21, 0x22}
    int_ids = {0x11, 0x27}
    string_ids = {0x12, 0x15, 0x1a, 0x1c, 0x1f}
    while at < end:
        key, at = variable(body, at)
        if key in byte_ids:
            size = 1
        elif key in short_ids:
            size = 2
        elif key in int_ids:
            size = 4
        elif key in string_ids or key == 0x16:
            require(at + 2 <= end, "truncated string/binary length")
            size = int.from_bytes(body[at:at + 2], "big")
            at += 2
        elif key == 0x26:
            pair = []
            for _ in range(2):
                require(at + 2 <= end, "truncated User Property")
                size = int.from_bytes(body[at:at + 2], "big")
                at += 2
                require(at + size <= end, "truncated User Property value")
                pair.append(body[at:at + size].decode())
                at += size
            output.setdefault(key, []).append(pair)
            continue
        else:
            raise ValueError(f"unsupported CONNACK property in test parser: {key:#x}")
        require(at + size <= end, "truncated CONNACK property")
        raw = body[at:at + size]
        at += size
        output[key] = raw.decode() if key in string_ids else (
            raw if key == 0x16 else int.from_bytes(raw, "big"))
    return output


def assigned_id_collision(case: Case) -> None:
    # Fresh fixture: first authenticated session consumes Sessions generation 1.
    # A client is allowed to choose this literal identifier itself.
    old, _ = case.v5("generated-2", clean=False, expiry=60)
    V.subscribe(old, 1)
    old.close(True)
    new, body = case.v5("", clean=True, expiry=60)
    assigned = connack_properties(body).get(0x12)
    require(isinstance(assigned, str) and bool(assigned), "Assigned Client ID missing")
    require(assigned != "generated-2",
            "Assigned Client ID collided with another still-stored session")
    new.close(True)
    resumed, body = case.v5("generated-2", clean=False, expiry=60)
    require(body[0] == 1, "Anonymous CONNECT destroyed the preexisting session")


def will_no_local_takeover(case: Case) -> None:
    old, _ = case.v5("audit-no-local", clean=False, expiry=60,
                     will=(C.TOPIC_A, R.event(8101), 0), will_retain=False)
    V.subscribe(old, 1, options=0x05)  # QoS1 + No Local
    new, body = case.v5("audit-no-local", clean=False, expiry=60)
    require(body[0] == 1, "Expected the same MQTT session to resume")
    require(old.recv() == (0xe0, b"\x8e\0"), "Missing takeover DISCONNECT")
    old.expect_closed()
    V.no_packet(new, timeout=0.6)
    new.send(C.frame(0xc0))
    require(new.recv() == (0xd0, b""), "Resumed connection is not usable")


def v311_started_expiry(case: Case) -> None:
    publisher, _ = case.v5("audit-expiring-retained")
    V.publish(publisher, 8102, 7, retain=True, expiry=3)
    ack(publisher.recv(), 0x40, 7)
    publisher.close(True)
    subscriber = case.v311("audit-v311-subscriber", clean=False)
    subscriber.send(C.subscribe(1, [(C.TOPIC_A, 1)]))
    received = [subscriber.recv(), subscriber.recv()]
    require(any(first == 0x90 for first, _ in received), "Missing SUBACK")
    publications = [(first, body) for first, body in received if first >> 4 == 3]
    require(len(publications) == 1, "Missing retained PUBLISH before expiry")
    message = C.parse_publish(*publications[0])
    require(message["qos"] == 1, "Expected a QoS1 subscriber delivery")
    packet_id = message["packet_id"]
    # MQTT keepalive is 30 seconds, so this is not an idle-timeout test.
    time.sleep(4.2)  # Message expiry plus a maintenance tick.
    subscriber.send(C.frame(0x40, packet_id.to_bytes(2, "big")))
    subscriber.send(C.frame(0xc0))
    require(subscriber.recv() == (0xd0, b""),
            "ACK of an already-transferred message was rejected after expiry")


def receive_maximum_resume(case: Case) -> None:
    old, body = case.v5("audit-receive-window", clean=False, expiry=60)
    maximum = int(connack_properties(body).get(0x21, 65535))
    if not 1 <= maximum <= 128:
        raise RuntimeError(f"This bounded fixture test requires Receive Maximum <=128; got {maximum}")
    for packet_id in range(1, maximum + 1):
        publish5(old, packet_id, R.event(8200 + packet_id))
        ack(old.recv(), 0x50, packet_id)
    # Treat captured PUBRECs as lost to the modeled sender before its restart.
    # Deliberately keep the server at AwaitPubrel; no PUBREL has been sent.
    old.close(True)
    resumed, body = case.v5("audit-receive-window", clean=False, expiry=60)
    require(body[0] == 1, "The pending QoS2 session did not resume")
    for packet_id in range(1, maximum + 1):
        publish5(resumed, packet_id, R.event(8200 + packet_id), dup=True)
        ack(resumed.recv(), 0x50, packet_id)
    # This is deliberately an over-quota sender. The 0x93 assertion verifies
    # this broker's bounded receive policy, not a receiver-side MQTT MUST.
    publish5(resumed, maximum + 1, R.event(8400), qos=1)
    packet = resumed.recv()
    require(disconnect_reason(packet) == 0x93,
            f"Expected Receive Maximum exceeded, got {packet!r}")


def v311_duplicate_packet_id(case: Case) -> None:
    before = V.events_accepted(case.broker)
    client = case.v311("audit-v311-duplicate", clean=False)
    client.send(C.publish(C.TOPIC_A, R.event(8501), qos=2, packet_id=7))
    ack(client.recv(), 0x50, 7)
    # Receiver robustness obligation; the changed payload models a bad sender.
    client.send(C.publish(C.TOPIC_A, R.event(8502), qos=2, packet_id=7, dup=True))
    ack(client.recv(), 0x50, 7)
    client.send(C.frame(0x62, b"\0\x07"))
    ack(client.recv(), 0x70, 7)
    require(V.events_accepted(case.broker) == before + 1,
            "Changed retransmission caused duplicate business acceptance")


def payload_format_reason(case: Case) -> None:
    before = V.events_accepted(case.broker)
    client, _ = case.v5("audit-payload-format")
    publish5(client, 7, b"\xff", properties=b"\x01\x01")
    first, body = client.recv()
    if first == 0xe0:
        require(disconnect_reason((first, body)) == 0x99,
                f"Wrong DISCONNECT reason for invalid UTF-8 payload: {body!r}")
        return
    reason = ack((first, body), 0x50, 7, success=False)
    require(reason == 0x99,
            f"Unused packet ID with invalid payload should not get {reason:#04x}")
    # A rejected flow must not prevent reuse of its identifier for a new message.
    publish5(client, 7, R.event(8601))
    ack(client.recv(), 0x50, 7)
    client.send(C.frame(0x62, b"\0\x07"))
    ack(client.recv(), 0x70, 7)
    require(V.events_accepted(case.broker) == before + 1,
            "Rejected payload format consumed or duplicated business admission")


def qos2_business_validation(case: Case) -> None:
    before = V.events_accepted(case.broker)
    client, _ = case.v5("audit-codec-rejection", clean=False, expiry=60)
    publish5(client, 7, b"{")  # MQTT-valid bytes; invalid netbaiot-json payload.
    first, body = client.recv()
    if first == 0xe0:
        require(V.events_accepted(case.broker) == before,
                "Invalid business payload crossed EventAccepted")
        return  # Rejecting before a successful PUBREC does not accept ownership.
    reason = ack((first, body), 0x50, 7, success=False)
    if reason >= 0x80:
        require(reason != 0x91, "Packet ID was unused; rejection reason is misleading")
        require(V.events_accepted(case.broker) == before,
                "Invalid business payload crossed EventAccepted")
        publish5(client, 7, R.event(8602))
        ack(client.recv(), 0x50, 7)
        client.send(C.frame(0x62, b"\0\x07"))
        ack(client.recv(), 0x70, 7)
        require(V.events_accepted(case.broker) == before + 1,
                "Rejected business payload left a poisoned Packet Identifier")
        return
    client.send(C.frame(0x62, b"\0\x07"))
    packet = client.recv()
    ack(packet, 0x70, 7)  # A successful PUBREC must not lead to a poison reconnect loop.


TESTS: dict[str, Callable[[Case], None]] = {
    "assigned_id_collision": assigned_id_collision,
    "will_no_local_takeover": will_no_local_takeover,
    "v311_started_expiry": v311_started_expiry,
    "receive_maximum_resume": receive_maximum_resume,
    "v311_duplicate_packet_id": v311_duplicate_packet_id,
    "payload_format_reason": payload_format_reason,
    "qos2_business_validation": qos2_business_validation,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--repo", type=pathlib.Path, required=True)
    parser.add_argument("--only", choices=sorted(TESTS))
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("mqtt-audit-results.json"))
    args = parser.parse_args()
    repo = args.repo.resolve()
    test_directory = repo / "tests/mqtt_conformance"
    if not (test_directory / "common.py").is_file():
        parser.error("--repo must point to the netbaiot checkout containing its conformance fixtures")
    binary_path = pathlib.Path(os.environ.get(
        "NETBAIOT_TEST_SERVER", str(repo / "target/debug/netbaiot-server")
    )).resolve()
    if not binary_path.is_file():
        parser.error("Build the tested checkout first: cargo build --locked -p netbaiot-server")
    sys.path.insert(0, str(test_directory))
    global C, V, R
    C = importlib.import_module("common")
    R = importlib.import_module("run")
    V = importlib.import_module("v5_smoke")
    head_result = subprocess.run(["git", "-C", str(repo), "rev-parse", "HEAD"],
                                 text=True, capture_output=True, check=False)
    head = head_result.stdout.strip() if head_result.returncode == 0 else "unknown"
    binary_sha256 = hashlib.sha256(binary_path.read_bytes()).hexdigest()
    diff = subprocess.run(["git", "-C", str(repo), "diff", "HEAD", "--binary"],
                          capture_output=True, check=False)
    diff_sha256 = hashlib.sha256(diff.stdout).hexdigest() if diff.returncode == 0 else None
    untracked = subprocess.run(["git", "-C", str(repo), "ls-files", "--others",
                                "--exclude-standard", "-z"], capture_output=True, check=False)
    untracked_sha256 = None
    if untracked.returncode == 0:
        digest = hashlib.sha256()
        for raw_path in sorted(path for path in untracked.stdout.split(b"\0") if path):
            path = repo / raw_path.decode("utf-8")
            if path.is_file():
                digest.update(raw_path + b"\0")
                digest.update(hashlib.sha256(path.read_bytes()).digest())
        untracked_sha256 = digest.hexdigest()
    print(f"Audit baseline: {AUDIT_SHA}\nCheckout HEAD:  {head}")
    print("Only isolated loopback test processes are started. Rebuild the binary before testing.")
    results = []
    selected = {args.only: TESTS[args.only]} if args.only else TESTS
    for name, test in selected.items():
        row = {"case": name, "result": "ERROR", "details": "", "connections": []}
        start = time.monotonic()
        case = None
        broker = None
        with tempfile.TemporaryDirectory(prefix="netbaiot-protocol-audit-") as directory:
            try:
                broker = C.start_netbaiot(pathlib.Path(directory))
                case = Case(broker)
                test(case)
                row["result"] = "PASS"
            except (AssertionError, EOFError, OSError) as error:
                row["result"] = "FAIL" if case is not None else "ERROR"
                row["details"] = f"{type(error).__name__}: {error}"
                row["traceback"] = traceback.format_exc(limit=4)
            except Exception as error:
                row["details"] = f"{type(error).__name__}: {error}"
                row["traceback"] = traceback.format_exc(limit=4)
            finally:
                if case is not None:
                    case.close()
                    row["connections"] = case.evidence()
                if broker is not None:
                    try:
                        broker.stop()
                    except Exception as error:
                        row["cleanup_error"] = f"{type(error).__name__}: {error}"
                        row["result"] = "ERROR"
                        if broker.process.poll() is None:
                            broker.process.kill()
                            broker.process.wait(timeout=5)
        row["duration_seconds"] = round(time.monotonic() - start, 3)
        results.append(row)
        print(f"{row['result']:5s} {name}: {row['details']}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"audit_baseline": AUDIT_SHA,
                                      "checkout_head": head,
                                      "binary_path": str(binary_path),
                                      "binary_sha256": binary_sha256,
                                      "tracked_diff_sha256": diff_sha256,
                                      "untracked_manifest_sha256": untracked_sha256,
                                      "results": results}, ensure_ascii=False, indent=2))
    print(f"Evidence: {args.output.resolve()}")
    return 0 if all(row["result"] == "PASS" for row in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
