#!/usr/bin/env python3
"""Run reproducible raw MQTT 3.1.1 and optional Mosquitto differential checks."""

from __future__ import annotations

import argparse
import json
import pathlib
import socket
import subprocess
import sys
import time
import traceback

from common import (
    MOSQUITTO,
    PASSWORD,
    ROOT,
    ROOT_A,
    ROOT_B,
    TOPIC_A,
    TOPIC_B,
    USERNAME_A,
    USERNAME_B,
    RawClient,
    binary,
    connect,
    frame,
    parse_publish,
    publish,
    start_mosquitto,
    start_netbaiot,
    start_netbaiot_config,
    subscribe,
    temporary_root,
    unsubscribe,
)


def event(sequence: int) -> bytes:
    return json.dumps(
        {
            "schema_version": 1,
            "source_message_id": f"conformance-{sequence}",
            "kind": "heartbeat",
            "data": {"sequence": sequence},
        },
        separators=(",", ":"),
    ).encode()


def connected(
    port: int,
    client_id: str,
    *,
    clean: bool = True,
    username: str = USERNAME_A,
    password: str = PASSWORD,
    keepalive: int = 30,
    will: tuple[bytes, bytes, int, bool] | None = None,
) -> tuple[RawClient, bytes]:
    client = RawClient("127.0.0.1", port)
    client.send(
        connect(
            client_id.encode(),
            clean=clean,
            username=username.encode(),
            password=password.encode(),
            keepalive=keepalive,
            will=will,
        )
    )
    first, body = client.recv()
    assert first == 0x20 and body[1] == 0, (first, body)
    return client, body


def no_packet(client: RawClient, timeout: float = 0.2) -> None:
    client.sock.settimeout(timeout)
    try:
        packet = client.recv()
    except (TimeoutError, socket.timeout):
        client.sock.settimeout(client.timeout)
        return
    raise AssertionError(f"unexpected packet {packet}")


class Results:
    def __init__(self, implementation: str, only: str | None = None) -> None:
        self.implementation = implementation
        self.only = only
        self.items: list[dict[str, object]] = []

    def run(self, test_id: str, category: str, requirements: str, callback) -> None:
        if self.only is not None and test_id != self.only:
            return
        started = time.monotonic()
        try:
            details = callback() or "completed"
            result = "PASS"
        except Exception as error:  # keep the entire release-gate matrix observable
            details = f"{type(error).__name__}: {error}; {traceback.format_exc(limit=2).strip()}"
            result = "FAIL"
        self.items.append(
            {
                "test_id": test_id,
                "category": category,
                "spec_requirement": requirements,
                "implementation": self.implementation,
                "result": result,
                "duration_ms": round((time.monotonic() - started) * 1000, 3),
                "details": details,
            }
        )


def raw_netbaiot(port: int, results: Results) -> None:
    def connect_valid() -> str:
        client, body = connected(port, "connect-valid")
        assert body == b"\x00\x00"
        client.close(True)
        return "accepted; Session Present=0"

    results.run("CONNECT-001", "CONNECT", "MQTT-3.1.4-4; MQTT-3.2.0-1", connect_valid)

    def connect_version() -> str:
        client = RawClient("127.0.0.1", port)
        client.send(connect(b"wrong-level", level=3))
        assert client.recv() == (0x20, b"\x00\x01")
        client.expect_closed()
        return "CONNACK=1 then close"

    results.run("CONNECT-002", "CONNECT", "MQTT-3.1.2-2", connect_version)

    def malformed_connect_flags() -> str:
        vectors = [
            connect(b"reserved", reserved=True),
            connect(b"password-only", flags_override=0x42),
            connect(b"bad-will", flags_override=0x1E),
        ]
        for wire in vectors:
            client = RawClient("127.0.0.1", port)
            client.send(wire)
            client.expect_closed()
        return "reserved/password-without-username/WillQoS=3 closed"

    results.run(
        "CONNECT-003",
        "CONNECT",
        "MQTT-3.1.2-3; MQTT-3.1.2-14; MQTT-3.1.2-22; MQTT-3.1.4-1",
        malformed_connect_flags,
    )

    def connect_auth() -> str:
        client = RawClient("127.0.0.1", port)
        client.send(connect(b"bad-auth", password=b"incorrect"))
        assert client.recv() == (0x20, b"\x00\x04")
        client.expect_closed()
        return "bad password CONNACK=4, Session Present=0"

    results.run("CONNACK-001", "CONNACK", "MQTT-3.2.2-4; MQTT-3.2.2-5", connect_auth)

    def client_id_matrix() -> str:
        rejected = RawClient("127.0.0.1", port)
        rejected.send(connect(b"", clean=False))
        assert rejected.recv() == (0x20, b"\x00\x02")
        rejected.expect_closed()
        accepted, body = connected(port, "")
        assert body == b"\x00\x00"
        accepted.close(True)
        return "zero ClientId rejected for CleanSession=0 and generated for CleanSession=1"

    results.run(
        "CONNECT-004",
        "CONNECT",
        "MQTT-3.1.3-6; MQTT-3.1.3-8; MQTT-3.1.3-9",
        client_id_matrix,
    )

    def only_one_connect() -> str:
        client, _ = connected(port, "double-connect")
        client.send(connect(b"double-connect"))
        client.expect_closed()
        return "second CONNECT closed"

    results.run("CONNECT-005", "CONNECT", "MQTT-3.1.0-2", only_one_connect)

    def first_packet_connect() -> str:
        for wire in [b"\xc0\x00", frame(0x30, binary(TOPIC_A.encode())), b"\x82\x02\x00\x01", b"\xe0\x00"]:
            client = RawClient("127.0.0.1", port)
            client.send(wire)
            client.expect_closed()
        return "PINGREQ/PUBLISH/SUBSCRIBE/DISCONNECT before CONNECT closed"

    results.run("SEQUENCE-001", "sequence", "MQTT-3.1.0-1", first_packet_connect)

    def header_flags() -> str:
        for first, body in [
            (0x41, b"\x00\x01"),
            (0x51, b"\x00\x01"),
            (0x60, b"\x00\x01"),
            (0x71, b"\x00\x01"),
            (0x80, b"\x00\x01"),
            (0xA0, b"\x00\x01"),
            (0xC1, b""),
            (0xE1, b""),
        ]:
            client, _ = connected(port, f"flags-{first}")
            client.send(frame(first, body))
            client.expect_closed()
        return "all client-sendable packet flag families rejected invalid fixed flags"

    results.run("HEADER-001", "fixed-header", "MQTT-2.2.2-1; MQTT-2.2.2-2", header_flags)

    def fragmentation() -> str:
        wire = connect(b"fragmented")
        client = RawClient("127.0.0.1", port)
        client.send(wire, fragments=[1] * len(wire))
        assert client.recv() == (0x20, b"\x00\x00")
        client.send(b"\xc0\x00\xc0\x00")
        assert client.recv() == (0xD0, b"")
        assert client.recv() == (0xD0, b"")
        client.close(True)
        return "bytewise CONNECT and two coalesced PINGREQ packets"

    results.run("FRAMING-001", "framing", "MQTT-7.1.1-1", fragmentation)

    def remaining_malformed() -> str:
        for wire in [b"\x10\x80\x80\x80\x80", b"\x10\x80\x80\x80\x80\x00", b"\x10\xff\xff\x7f"]:
            client = RawClient("127.0.0.1", port)
            client.send(wire)
            client.expect_closed()
        return "unterminated/fifth/over-product-limit Remaining Length closed"

    results.run("REMAINING-001", "remaining-length", "MQTT-4.8.0-1", remaining_malformed)

    def invalid_utf8() -> str:
        for client_id in [b"bad\x00id", b"\xed\xa0\x80"]:
            client = RawClient("127.0.0.1", port)
            client.send(connect(client_id))
            client.expect_closed()
        return "NUL and surrogate encoding closed"

    results.run("UTF8-001", "UTF-8", "MQTT-1.5.3-1; MQTT-1.5.3-2", invalid_utf8)

    def session_present() -> str:
        first, body = connected(port, "session-present", clean=False)
        assert body == b"\x00\x00"
        first.close(True)
        second, body = connected(port, "session-present", clean=False)
        assert body == b"\x01\x00"
        second.close(True)
        clean, body = connected(port, "session-present", clean=True)
        assert body == b"\x00\x00"
        clean.close(True)
        fresh, body = connected(port, "session-present", clean=False)
        assert body == b"\x00\x00"
        fresh.close(True)
        return "0,new; 1,resume; 0,CleanSession=1; 0,post-reset"

    results.run(
        "SESSION-001",
        "session",
        "MQTT-3.1.2-4; MQTT-3.1.2-6; MQTT-3.2.2-1; MQTT-3.2.2-2; MQTT-3.2.2-3",
        session_present,
    )

    def subscribe_matrix() -> str:
        client, _ = connected(port, "subscribe-matrix", clean=False)
        filters = [(f"{ROOT_A}/up", 0), (f"{ROOT_A}/+", 1), (f"{ROOT_A}/#", 2)]
        client.send(subscribe(10, filters))
        assert client.recv() == (0x90, b"\x00\x0a\x00\x01\x02")
        client.send(subscribe(11, [(TOPIC_B, 1)]))
        assert client.recv() == (0x90, b"\x00\x0b\x80")
        client.close(True)
        return "SUBACK order 0/1/2 and per-filter ACL failure 0x80"

    results.run(
        "SUB-001",
        "subscription",
        "MQTT-3.8.4-1; MQTT-3.8.4-2; MQTT-3.8.4-4; MQTT-3.8.4-5; MQTT-3.9.3-1",
        subscribe_matrix,
    )

    def subscribe_invalid() -> str:
        vectors = [subscribe(0, [(TOPIC_A, 1)]), subscribe(1, [(f"{ROOT_A}/a+", 1)]), frame(0x82, b"\x00\x01")]
        for wire in vectors:
            client, _ = connected(port, f"sub-invalid-{len(wire)}-{wire[-1] if wire else 0}")
            client.send(wire)
            client.expect_closed()
        return "packet ID 0, invalid filter, and empty payload closed"

    results.run("SUB-002", "subscription", "MQTT-2.3.1-1; MQTT-3.8.3-3; MQTT-3-8.3-4", subscribe_invalid)

    def unsubscribe_matrix() -> str:
        client, _ = connected(port, "unsubscribe-matrix", clean=False)
        client.send(subscribe(1, [(TOPIC_A, 1), (f"{ROOT_A}/#", 1)]))
        assert client.recv()[0] == 0x90
        client.send(unsubscribe(2, [TOPIC_A, f"{ROOT_A}/missing"]))
        assert client.recv() == (0xB0, b"\x00\x02")
        client.send(unsubscribe(3, [f"{ROOT_A}/#"]))
        assert client.recv() == (0xB0, b"\x00\x03")
        client.close(True)
        return "existing/non-existing/multiple filters acknowledged with same ID"

    results.run(
        "UNSUB-001",
        "unsubscribe",
        "MQTT-3.10.4-1; MQTT-3.10.4-4; MQTT-3.10.4-5; MQTT-3.10.4-6",
        unsubscribe_matrix,
    )

    def qos0() -> str:
        client, _ = connected(port, "qos0")
        client.send(subscribe(1, [(TOPIC_A, 2)]))
        assert client.recv()[0] == 0x90
        payload = event(100)
        client.send(publish(TOPIC_A, payload, qos=0))
        first, body = client.recv()
        routed = parse_publish(first, body)
        assert routed["qos"] == 0 and routed["payload"] == payload
        client.close(True)
        return "no PUBACK; subscriber received QoS0"

    results.run("QOS0-001", "QoS0", "MQTT-3.3.4-1; MQTT-4.3.1-1", qos0)

    def qos1() -> str:
        client, _ = connected(port, "qos1")
        client.send(subscribe(1, [(TOPIC_A, 2)]))
        assert client.recv()[0] == 0x90
        payload = event(101)
        client.send(publish(TOPIC_A, payload, qos=1, packet_id=42))
        assert client.recv() == (0x40, b"\x00\x2a")
        first, body = client.recv()
        routed = parse_publish(first, body)
        assert routed["qos"] == 1 and routed["payload"] == payload
        client.send(frame(0x40, int(routed["packet_id"]).to_bytes(2, "big")))
        client.close(True)
        return "PUBACK preserved id=42; outgoing QoS=min(1,2)"

    results.run("QOS1-001", "QoS1", "MQTT-2.3.1-6; MQTT-4.3.2-2", qos1)

    def qos1_duplicate_new() -> str:
        client, _ = connected(port, "qos1-dup")
        client.send(subscribe(1, [(TOPIC_A, 1)]))
        assert client.recv()[0] == 0x90
        wire = publish(TOPIC_A, event(102), qos=1, packet_id=50)
        client.send(wire)
        assert client.recv() == (0x40, b"\x00\x32")
        first, body = client.recv()
        first_delivery = parse_publish(first, body)
        client.send(frame(0x40, int(first_delivery["packet_id"]).to_bytes(2, "big")))
        client.send(publish(TOPIC_A, event(103), qos=1, packet_id=50, dup=True))
        assert client.recv() == (0x40, b"\x00\x32")
        _, body = client.recv()
        assert parse_publish(0x32, body)["payload"] == event(103)
        client.close(True)
        return "same incoming id after PUBACK treated as a new publication"

    results.run("QOS1-DUP-001", "QoS1", "MQTT-4.3.2-2", qos1_duplicate_new)

    def qos2_duplicates() -> str:
        client, _ = connected(port, "qos2-duplicates", clean=False)
        client.send(subscribe(1, [(TOPIC_A, 2)]))
        assert client.recv()[0] == 0x90
        payload = event(104)
        client.send(publish(TOPIC_A, payload, qos=2, packet_id=60))
        assert client.recv() == (0x50, b"\x00\x3c")
        client.send(publish(TOPIC_A, payload, qos=2, packet_id=60, dup=True))
        assert client.recv() == (0x50, b"\x00\x3c")
        client.send(frame(0x62, b"\x00\x3c"))
        assert client.recv() == (0x70, b"\x00\x3c")
        first, body = client.recv()
        routed = parse_publish(first, body)
        assert routed["qos"] == 2 and routed["payload"] == payload
        outbound_id = int(routed["packet_id"])
        client.send(frame(0x50, outbound_id.to_bytes(2, "big")))
        assert client.recv() == (0x62, outbound_id.to_bytes(2, "big"))
        client.send(frame(0x70, outbound_id.to_bytes(2, "big")))
        client.send(frame(0x62, b"\x00\x3c"))
        assert client.recv() == (0x70, b"\x00\x3c")
        no_packet(client)
        client.close(True)
        return "duplicate PUBLISH=>PUBREC; duplicate PUBREL=>PUBCOMP; one routed delivery"

    results.run(
        "QOS2-IN-001",
        "QoS2",
        "MQTT-4.3.3-2",
        qos2_duplicates,
    )

    def retained_matrix() -> str:
        writer, _ = connected(port, "retained-writer")
        writer.send(publish(TOPIC_A, event(110), qos=1, packet_id=1, retain=True))
        assert writer.recv() == (0x40, b"\x00\x01")
        writer.send(subscribe(2, [(f"{ROOT_A}/#", 2)]))
        assert writer.recv()[0] == 0x90
        first, body = writer.recv()
        retained = parse_publish(first, body)
        assert retained["retain"] and retained["payload"] == event(110)
        writer.send(frame(0x40, int(retained["packet_id"]).to_bytes(2, "big")))
        writer.send(publish(TOPIC_A, event(111), qos=1, packet_id=2, retain=True))
        assert writer.recv() == (0x40, b"\x00\x02")
        first, body = writer.recv()
        live = parse_publish(first, body)
        assert not live["retain"] and live["payload"] == event(111)
        writer.send(frame(0x40, int(live["packet_id"]).to_bytes(2, "big")))
        writer.send(publish(TOPIC_A, b"", qos=1, packet_id=3, retain=True))
        assert writer.recv() == (0x40, b"\x00\x03")
        _, body = writer.recv()
        deletion = parse_publish(0x32, body)
        assert deletion["payload"] == b"" and not deletion["retain"]
        writer.send(frame(0x40, int(deletion["packet_id"]).to_bytes(2, "big")))
        writer.close(True)
        fresh, _ = connected(port, "retained-empty")
        fresh.send(subscribe(4, [(TOPIC_A, 1)]))
        assert fresh.recv()[0] == 0x90
        no_packet(fresh)
        fresh.close(True)
        return "create/replay(RETAIN=1), replace/live(RETAIN=0), zero-delete/no future replay"

    results.run(
        "RETAIN-001",
        "retained",
        "MQTT-3.3.1-5; MQTT-3.3.1-6; MQTT-3.3.1-8; MQTT-3.3.1-9; MQTT-3.3.1-10; MQTT-3.3.1-11",
        retained_matrix,
    )

    def resubscribe_replays() -> str:
        writer, _ = connected(port, "resub-writer")
        writer.send(publish(TOPIC_A, event(112), qos=1, packet_id=1, retain=True))
        assert writer.recv()[0] == 0x40
        for packet_id, qos in [(1, 0), (2, 2)]:
            writer.send(subscribe(packet_id, [(TOPIC_A, qos)]))
            assert writer.recv() == (0x90, packet_id.to_bytes(2, "big") + bytes([qos]))
            first, body = writer.recv()
            replay = parse_publish(first, body)
            assert replay["retain"] and replay["qos"] == min(1, qos)
            if replay["packet_id"]:
                writer.send(frame(0x40, int(replay["packet_id"]).to_bytes(2, "big")))
        writer.send(publish(TOPIC_A, b"", qos=1, packet_id=2, retain=True))
        assert writer.recv()[0] == 0x40
        _, body = writer.recv()
        deletion = parse_publish(0x32, body)
        writer.send(frame(0x40, int(deletion["packet_id"]).to_bytes(2, "big")))
        writer.close(True)
        return "identical filter replacement re-sent retained message and changed max QoS"

    results.run("SUB-RESUB-001", "subscription", "MQTT-3.8.4-3", resubscribe_replays)

    def offline_qos() -> str:
        persistent, body = connected(port, "offline", clean=False)
        assert body == b"\x00\x00"
        persistent.send(subscribe(1, [(TOPIC_A, 2)]))
        assert persistent.recv()[0] == 0x90
        persistent.close(True)
        publisher, _ = connected(port, "offline-publisher")
        publisher.send(publish(TOPIC_A, event(120), qos=1, packet_id=1))
        assert publisher.recv()[0] == 0x40
        publisher.send(publish(TOPIC_A, event(121), qos=2, packet_id=2))
        assert publisher.recv() == (0x50, b"\x00\x02")
        publisher.send(frame(0x62, b"\x00\x02"))
        assert publisher.recv() == (0x70, b"\x00\x02")
        resumed, body = connected(port, "offline", clean=False)
        assert body == b"\x01\x00"
        observed = []
        for _ in range(2):
            first, packet_body = resumed.recv()
            message = parse_publish(first, packet_body)
            observed.append((message["payload"], message["qos"]))
            packet_id = int(message["packet_id"])
            if message["qos"] == 1:
                resumed.send(frame(0x40, packet_id.to_bytes(2, "big")))
            else:
                resumed.send(frame(0x50, packet_id.to_bytes(2, "big")))
                assert resumed.recv() == (0x62, packet_id.to_bytes(2, "big"))
                resumed.send(frame(0x70, packet_id.to_bytes(2, "big")))
        assert observed == [(event(120), 1), (event(121), 2)], observed
        publisher.close(True)
        resumed.close(True)
        return "ordered offline QoS1/QoS2 and Session Present=1"

    results.run(
        "SESSION-OFFLINE-001",
        "session",
        "MQTT-3.1.2-5; MQTT-4.4.0-1; MQTT-4.6.0-1",
        offline_qos,
    )

    def will_abnormal_normal() -> str:
        payload = event(130)
        abrupt, _ = connected(
            port,
            "will-abrupt",
            will=(TOPIC_A.encode(), payload, 1, True),
        )
        abrupt.close(False)
        time.sleep(0.05)
        reader, _ = connected(port, "will-reader")
        reader.send(subscribe(1, [(TOPIC_A, 2)]))
        assert reader.recv()[0] == 0x90
        first, body = reader.recv()
        delivered = parse_publish(first, body)
        assert delivered["payload"] == payload and delivered["retain"]
        reader.send(frame(0x40, int(delivered["packet_id"]).to_bytes(2, "big")))
        reader.send(publish(TOPIC_A, b"", qos=1, packet_id=5, retain=True))
        assert reader.recv()[0] == 0x40
        _, body = reader.recv()
        deletion = parse_publish(0x32, body)
        reader.send(frame(0x40, int(deletion["packet_id"]).to_bytes(2, "big")))
        normal, _ = connected(
            port,
            "will-normal",
            will=(TOPIC_A.encode(), event(131), 2, True),
        )
        normal.close(True)
        verify, _ = connected(port, "will-normal-verify")
        verify.send(subscribe(1, [(TOPIC_A, 2)]))
        assert verify.recv()[0] == 0x90
        no_packet(verify)
        reader.close(True)
        verify.close(True)
        return "EOF published retained Will; DISCONNECT removed Will without publication"

    results.run(
        "WILL-001",
        "Will",
        "MQTT-3.1.2-8; MQTT-3.1.2-10; MQTT-3.1.2-17; MQTT-3.14.4-3",
        will_abnormal_normal,
    )

    def duplicate_client_will() -> str:
        payload = event(132)
        old, _ = connected(
            port,
            "takeover",
            clean=False,
            will=(TOPIC_A.encode(), payload, 1, False),
        )
        old.send(subscribe(1, [(TOPIC_A, 1)]))
        assert old.recv()[0] == 0x90
        replacement, body = connected(port, "takeover", clean=False)
        assert body == b"\x01\x00"
        first, packet_body = replacement.recv()
        delivery = parse_publish(first, packet_body)
        assert delivery["payload"] == payload and not delivery["retain"]
        replacement.send(frame(0x40, int(delivery["packet_id"]).to_bytes(2, "big")))
        old.expect_closed()
        replacement.close(True)
        return "old connection closed and its Will delivered once to replacement session"

    results.run("WILL-TAKEOVER-001", "Will", "MQTT-3.1.4-2; MQTT-3.1.2-8", duplicate_client_will)

    def ping_keepalive() -> str:
        ping, _ = connected(port, "ping", keepalive=0)
        ping.send(b"\xc0\x00")
        assert ping.recv() == (0xD0, b"")
        ping.close(True)
        idle, _ = connected(port, "keepalive", keepalive=1)
        started = time.monotonic()
        idle.expect_closed(timeout=2.5)
        elapsed = time.monotonic() - started
        assert 1.2 <= elapsed <= 2.4, elapsed
        return f"PINGRESP and KeepAlive close at {elapsed:.3f}s"

    results.run("KEEPALIVE-001", "Keep Alive", "MQTT-3.1.2-24; MQTT-3.12.4-1", ping_keepalive)

    def slowloris() -> str:
        client = RawClient("127.0.0.1", port)
        client.send(b"\x10")
        started = time.monotonic()
        client.expect_closed(timeout=1.5)
        elapsed = time.monotonic() - started
        assert 0.35 <= elapsed <= 1.4, elapsed
        return f"incomplete packet closed at {elapsed:.3f}s by whole-packet deadline"

    results.run("SLOWLORIS-001", "resource", "MQTT-4.8.0-2", slowloris)

    def illegal_sequences() -> str:
        packets = [
            frame(0x40, b"\x00\x63"),
            frame(0x50, b"\x00\x63"),
            frame(0x70, b"\x00\x63"),
            frame(0x20, b"\x00\x00"),
            frame(0x90, b"\x00\x01\x00"),
            frame(0xB0, b"\x00\x01"),
            b"\xd0\x00",
        ]
        for index, wire in enumerate(packets):
            client, _ = connected(port, f"illegal-{index}")
            client.send(wire)
            client.expect_closed()
        pubrel, _ = connected(port, "unknown-pubrel")
        pubrel.send(frame(0x62, b"\x00\x63"))
        assert pubrel.recv() == (0x70, b"\x00\x63")
        pubrel.close(True)
        return "unknown PUBREL received PUBCOMP; other unknown/server-only packets closed"

    results.run("SEQUENCE-002", "sequence", "MQTT-4.8.0-1", illegal_sequences)

    def acl_matrix() -> str:
        for index, topic in enumerate([TOPIC_B, "v1/t/other/p/sensor/d/device-1/up"]):
            client, _ = connected(port, f"acl-publish-{index}")
            client.send(publish(topic, event(140 + index), qos=1, packet_id=1, retain=True))
            client.expect_closed()
        subscriber, _ = connected(port, "acl-subscribe")
        subscriber.send(subscribe(1, [(TOPIC_B, 1), (f"v1/t/other/#", 1)]))
        assert subscriber.recv() == (0x90, b"\x00\x01\x80\x80")
        other, _ = connected(port, "acl-other", username=USERNAME_B)
        other.send(subscribe(1, [(TOPIC_B, 1)]))
        assert other.recv() == (0x90, b"\x00\x01\x01")
        no_packet(other)
        subscriber.close(True)
        other.close(True)
        return "cross-device/tenant publish closed before retained effect; subscriptions 0x80"

    results.run("SECURITY-ACL-001", "security", "NetbaIoT product profile", acl_matrix)

    def cross_identity_session() -> str:
        first, _ = connected(port, "shared-id", clean=False)
        first.send(subscribe(1, [(TOPIC_A, 1)]))
        assert first.recv()[0] == 0x90
        first.close(True)
        second, body = connected(port, "shared-id", clean=False, username=USERNAME_B)
        assert body == b"\x00\x00"
        second.send(subscribe(1, [(TOPIC_B, 1)]))
        assert second.recv()[0] == 0x90
        resumed, body = connected(port, "shared-id", clean=False)
        assert body == b"\x01\x00"
        second.close(True)
        resumed.close(True)
        return "same ClientId under different authenticated DeviceKey did not resume or clear state"

    results.run("SECURITY-SESSION-001", "security", "NetbaIoT product profile", cross_identity_session)


def planned_shutdown_will(root: pathlib.Path, broker, results: Results) -> None:
    def check() -> str:
        payload = event(150)
        client, _ = connected(
            broker.port,
            "planned-will",
            clean=False,
            will=(TOPIC_A.encode(), payload, 1, True),
        )
        broker.stop()
        try:
            client.close()
        except OSError:
            pass
        restarted = start_netbaiot_config(root, broker.config_path, broker.port)
        try:
            reader, _ = connected(restarted.port, "planned-will-reader")
            reader.send(subscribe(1, [(TOPIC_A, 1)]))
            assert reader.recv()[0] == 0x90
            first, body = reader.recv()
            retained = parse_publish(first, body)
            assert retained["payload"] == payload and retained["retain"]
            reader.close(True)
        finally:
            restarted.stop()
        return "server shutdown published retained Will before snapshot; restart replayed it"

    results.run("WILL-SHUTDOWN-001", "Will", "MQTT-3.1.2-8", check)


def differential_vectors(netbaiot_port: int, mosquitto_port: int, results: Results) -> None:
    def open_client(port: int, client_id: str, *, clean: bool, credentials: bool, keepalive: int = 30, will=None):
        client = RawClient("127.0.0.1", port)
        client.send(
            connect(
                client_id.encode(),
                clean=clean,
                username=USERNAME_A.encode() if credentials else None,
                password=PASSWORD.encode() if credentials else None,
                keepalive=keepalive,
                will=will,
            )
        )
        connack = client.recv()
        assert connack[0] == 0x20 and connack[1][1] == 0, connack
        return client, connack

    def exchange(port: int, wire: bytes, *, credentials: bool) -> tuple[int, bytes] | str:
        client = RawClient("127.0.0.1", port)
        if credentials:
            client.send(connect(b"diff", clean=True))
        else:
            client.send(connect(b"diff", clean=True, username=None, password=None))
        connack = client.recv()
        if wire:
            client.send(wire)
            try:
                reply: tuple[int, bytes] | str = client.recv()
            except (TimeoutError, socket.timeout):
                reply = "NO_RESPONSE"
            except EOFError:
                reply = "CLOSED"
        else:
            reply = connack
        client.close()
        return reply

    vectors = [
        ("DIFF-CONNECT-001", "CONNECT", b"", (0x20, b"\x00\x00")),
        ("DIFF-PING-001", "PING", b"\xc0\x00", (0xD0, b"")),
        ("DIFF-FLAGS-001", "malformed-flags", b"\xc1\x00", "CLOSED"),
        (
            "DIFF-UNKNOWN-PUBACK-001",
            "unknown-id",
            frame(0x40, b"\x00\x63"),
            ("CLOSED", "NO_RESPONSE"),
        ),
    ]
    for test_id, category, wire, expected in vectors:
        def callback(wire=wire, expected=expected) -> str:
            netbaiot = exchange(netbaiot_port, wire, credentials=True)
            mosquitto = exchange(mosquitto_port, wire, credentials=False)
            if isinstance(expected, tuple) and expected and isinstance(expected[0], str):
                assert (netbaiot, mosquitto) == expected, ((netbaiot, mosquitto), expected)
                classification = "; IMPLEMENTATION_DEFINED_ALLOWED"
            else:
                assert netbaiot == expected, (netbaiot, expected)
                assert mosquitto == expected, (mosquitto, expected)
                classification = ""
            return f"NetbaIoT={netbaiot!r}; Mosquitto={mosquitto!r}{classification}"

        results.run(test_id, f"differential/{category}", "observable MQTT 3.1.1 behavior", callback)

    def compare(name: str, category: str, callback) -> None:
        def check() -> str:
            netbaiot = callback(netbaiot_port, True)
            mosquitto = callback(mosquitto_port, False)
            assert netbaiot == mosquitto, (netbaiot, mosquitto)
            return f"NetbaIoT={netbaiot!r}; Mosquitto={mosquitto!r}"

        results.run(name, f"differential/{category}", "observable MQTT 3.1.1 behavior", check)

    def session_present(port: int, credentials: bool):
        first, first_ack = open_client(port, "diff-session", clean=False, credentials=credentials)
        first.close(True)
        second, second_ack = open_client(port, "diff-session", clean=False, credentials=credentials)
        second.close(True)
        reset, reset_ack = open_client(port, "diff-session", clean=True, credentials=credentials)
        reset.close(True)
        fresh, fresh_ack = open_client(port, "diff-session", clean=False, credentials=credentials)
        fresh.close(True)
        return tuple(packet[1][0] for packet in (first_ack, second_ack, reset_ack, fresh_ack))

    compare("DIFF-SESSION-001", "session", session_present)

    def subscribe_resubscribe_unsubscribe(port: int, credentials: bool):
        client, _ = open_client(port, "diff-sub", clean=True, credentials=credentials)
        client.send(subscribe(1, [(TOPIC_A, 0), (f"{ROOT_A}/+", 1), (f"{ROOT_A}/#", 2)]))
        first = client.recv()
        client.send(subscribe(2, [(TOPIC_A, 2)]))
        second = client.recv()
        client.send(unsubscribe(3, [TOPIC_A, f"{ROOT_A}/+"]))
        third = client.recv()
        client.close(True)
        return first, second, third

    compare("DIFF-SUB-001", "subscribe-resubscribe-unsubscribe", subscribe_resubscribe_unsubscribe)

    def qos_flow(port: int, credentials: bool):
        client, _ = open_client(port, "diff-qos", clean=True, credentials=credentials)
        client.send(subscribe(1, [(TOPIC_A, 2)]))
        assert client.recv() == (0x90, b"\x00\x01\x02")
        observed = []
        for qos, packet_id in ((0, None), (1, 10), (2, 11)):
            payload = event(200 + qos)
            client.send(publish(TOPIC_A, payload, qos=qos, packet_id=packet_id))
            if qos == 1:
                packets = [client.recv(), client.recv()]
                assert (0x40, b"\x00\x0a") in packets
                routed = next(packet for packet in packets if packet[0] >> 4 == 3)
            elif qos == 2:
                assert client.recv() == (0x50, b"\x00\x0b")
                client.send(frame(0x62, b"\x00\x0b"))
                packets = [client.recv(), client.recv()]
                assert (0x70, b"\x00\x0b") in packets
                routed = next(packet for packet in packets if packet[0] >> 4 == 3)
            else:
                routed = client.recv()
            message = parse_publish(*routed)
            observed.append((message["payload"], message["qos"]))
            outbound_id = message["packet_id"]
            if message["qos"] == 1:
                client.send(frame(0x40, int(outbound_id).to_bytes(2, "big")))
            elif message["qos"] == 2:
                client.send(frame(0x50, int(outbound_id).to_bytes(2, "big")))
                assert client.recv() == (0x62, int(outbound_id).to_bytes(2, "big"))
                client.send(frame(0x70, int(outbound_id).to_bytes(2, "big")))
        client.close(True)
        return observed

    compare("DIFF-QOS-001", "qos0-qos1-qos2", qos_flow)

    def retained(port: int, credentials: bool):
        writer, _ = open_client(port, "diff-retain-writer", clean=True, credentials=credentials)
        retained_payload = event(210)
        writer.send(publish(TOPIC_A, retained_payload, qos=1, packet_id=1, retain=True))
        assert writer.recv() == (0x40, b"\x00\x01")
        writer.close(True)
        reader, _ = open_client(port, "diff-retain-reader", clean=True, credentials=credentials)
        reader.send(subscribe(1, [(f"{ROOT_A}/#", 1)]))
        assert reader.recv() == (0x90, b"\x00\x01\x01")
        replay = parse_publish(*reader.recv())
        reader.send(frame(0x40, int(replay["packet_id"]).to_bytes(2, "big")))
        reader.send(publish(TOPIC_A, b"", qos=1, packet_id=2, retain=True))
        packets = [reader.recv(), reader.recv()]
        assert (0x40, b"\x00\x02") in packets
        deletion = parse_publish(*next(packet for packet in packets if packet[0] >> 4 == 3))
        reader.send(frame(0x40, int(deletion["packet_id"]).to_bytes(2, "big")))
        reader.close(True)
        return replay["payload"] == retained_payload, replay["qos"], replay["retain"]

    compare("DIFF-RETAIN-001", "retained", retained)

    def will(port: int, credentials: bool):
        payload = event(220)
        writer, _ = open_client(
            port,
            "diff-will-writer",
            clean=True,
            credentials=credentials,
            will=(TOPIC_A.encode(), payload, 1, True),
        )
        writer.close(False)
        time.sleep(0.05)
        reader, _ = open_client(port, "diff-will-reader", clean=True, credentials=credentials)
        reader.send(subscribe(1, [(TOPIC_A, 1)]))
        assert reader.recv() == (0x90, b"\x00\x01\x01")
        replay = parse_publish(*reader.recv())
        reader.send(frame(0x40, int(replay["packet_id"]).to_bytes(2, "big")))
        reader.send(publish(TOPIC_A, b"", qos=1, packet_id=2, retain=True))
        packets = [reader.recv(), reader.recv()]
        assert (0x40, b"\x00\x02") in packets
        deletion = parse_publish(*next(packet for packet in packets if packet[0] >> 4 == 3))
        reader.send(frame(0x40, int(deletion["packet_id"]).to_bytes(2, "big")))
        reader.close(True)
        return replay["payload"] == payload, replay["qos"], replay["retain"]

    compare("DIFF-WILL-001", "will", will)

    def duplicate_client(port: int, credentials: bool):
        old, _ = open_client(port, "diff-duplicate", clean=False, credentials=credentials)
        replacement, connack = open_client(port, "diff-duplicate", clean=False, credentials=credentials)
        old.expect_closed()
        replacement.close(True)
        return connack[1][0], "old-closed"

    compare("DIFF-DUPLICATE-CLIENT-001", "duplicate-client", duplicate_client)

    def keepalive(port: int, credentials: bool):
        client, _ = open_client(port, "diff-keepalive", clean=True, credentials=credentials, keepalive=1)
        started = time.monotonic()
        client.expect_closed(timeout=2.6)
        elapsed = time.monotonic() - started
        assert 1.2 <= elapsed <= 2.5, elapsed
        return "closed-at-1.5x"

    compare("DIFF-KEEPALIVE-001", "keepalive", keepalive)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--netbaiot-only", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--only")
    args = parser.parse_args()
    if not args.no_build:
        subprocess.run(["cargo", "build", "-p", "netbaiot-server"], cwd=ROOT, check=True)

    output = ROOT / "target/mqtt-conformance/results.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    results = Results("NetbaIoT", args.only)
    with temporary_root("netbaiot-conformance-") as root_name:
        root = pathlib.Path(root_name)
        broker = start_netbaiot(root)
        try:
            raw_netbaiot(broker.port, results)
        finally:
            if broker.process.poll() is None:
                broker.stop()
            if any(item["result"] == "FAIL" for item in results.items) and broker.process.stderr:
                diagnostics = broker.process.stderr.read().strip()
                if diagnostics:
                    print(diagnostics, file=sys.stderr)

    with temporary_root("netbaiot-will-shutdown-") as root_name:
        root = pathlib.Path(root_name)
        broker = start_netbaiot(root)
        planned_shutdown_will(root, broker, results)
        if broker.process.poll() is None:
            broker.stop()

    if not args.netbaiot_only and MOSQUITTO.exists():
        with temporary_root("netbaiot-differential-") as net_root_name, temporary_root(
            "mosquitto-differential-"
        ) as mosq_root_name:
            net_root = pathlib.Path(net_root_name)
            mosq_root = pathlib.Path(mosq_root_name)
            netbaiot = start_netbaiot(net_root)
            mosquitto = start_mosquitto(mosq_root)
            differential = Results("NetbaIoT vs Mosquitto", args.only)
            try:
                differential_vectors(netbaiot.port, mosquitto.port, differential)
                results.items.extend(differential.items)
            finally:
                netbaiot.stop()
                mosquitto.stop()

    document = {
        "schema_version": 1,
        "suite": "NetbaIoT MQTT 3.1.1 raw conformance",
        "results": results.items,
        "summary": {
            "total": len(results.items),
            "pass": sum(item["result"] == "PASS" for item in results.items),
            "fail": sum(item["result"] == "FAIL" for item in results.items),
        },
    }
    output.write_text(json.dumps(document, indent=2) + "\n")
    print(json.dumps(document["summary"], indent=2))
    for item in results.items:
        print(f"{item['test_id']}: {item['result']} — {item['details']}")
    return 1 if document["summary"]["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
