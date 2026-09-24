"""Raw MQTT 5 integration checks against the NetbaIoT development broker."""

from __future__ import annotations

import pathlib
import socket
import tempfile
import time

from common import (
    PASSWORD,
    TOPIC_A,
    USERNAME_A,
    RawClient,
    binary,
    frame,
    start_netbaiot,
)
from run import event


def connect(client: RawClient, client_id: str, *, clean: bool = True, expiry: int = 0,
            receive_maximum: int = 4, maximum_packet_size: int | None = None,
            will: tuple[str, bytes, int] | None = None) -> bytes:
    properties = b"\x11" + expiry.to_bytes(4, "big") + b"\x21" + receive_maximum.to_bytes(2, "big")
    if maximum_packet_size is not None:
        properties += b"\x27" + maximum_packet_size.to_bytes(4, "big")
    flags = 0xc0 | (2 if clean else 0)
    payload = binary(client_id.encode())
    if will:
        will_topic, will_payload, delay = will
        flags |= 0x2c  # Will QoS1 and retain
        payload += b"\x05\x18" + delay.to_bytes(4, "big")
        payload += binary(will_topic.encode()) + binary(will_payload)
    payload += binary(USERNAME_A.encode()) + binary(PASSWORD.encode())
    body = (binary(b"MQTT") + bytes([5, flags, 0, 30])
            + bytes([len(properties)]) + properties + payload)
    client.send(frame(0x10, body))
    first, body = client.recv()
    assert first == 0x20 and body[1] == 0, (first, body)
    return body


def publish(client: RawClient, sequence: int, packet_id: int, *, retain: bool = False,
            expiry: int | None = None) -> None:
    properties = b"" if expiry is None else b"\x02" + expiry.to_bytes(4, "big")
    body = binary(TOPIC_A.encode()) + packet_id.to_bytes(2, "big") + bytes([len(properties)]) + properties + event(sequence)
    client.send(frame(0x32 | int(retain), body))


def subscribe(client: RawClient, packet_id: int, options: int = 1) -> None:
    body = packet_id.to_bytes(2, "big") + b"\0" + binary(TOPIC_A.encode()) + bytes([options])
    client.send(frame(0x82, body))
    assert client.recv() == (0x90, packet_id.to_bytes(2, "big") + b"\0" + bytes([options & 3]))


def no_packet(client: RawClient, timeout: float = 0.25) -> None:
    client.sock.settimeout(timeout)
    try:
        packet = client.recv()
    except (TimeoutError, socket.timeout):
        return
    finally:
        client.sock.settimeout(client.timeout)
    raise AssertionError(f"unexpected MQTT packet: {packet}")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="netbaiot-v5-") as temporary:
        broker = start_netbaiot(pathlib.Path(temporary))
        try:
            for name, wire, expected in (
                ("unknown-puback", frame(0x40, b"\0\x07"), 0x92),
                ("invalid-topic", frame(0x30, binary(b"bad/+topic") + b"\0"), 0x90),
                ("invalid-filter", frame(0x82, b"\0\x08\0" + binary(b"bad/#/tail") + b"\x01"), 0x8f),
            ):
                invalid = RawClient("127.0.0.1", broker.port)
                try:
                    connect(invalid, f"v5-{name}")
                    invalid.send(wire)
                    assert invalid.recv() == (0xe0, bytes([expected, 0])), name
                finally:
                    invalid.close()

            subscriber = RawClient("127.0.0.1", broker.port)
            try:
                assert connect(subscriber, "v5-sub", clean=True, expiry=60)[0] == 0
                subscribe(subscriber, 1)
                publish(subscriber, 9001, 1)
                received = [subscriber.recv(), subscriber.recv()]
                assert (0x40, b"\0\1") in received, received
                first, body = next(item for item in received if item[0] >> 4 == 3)
                assert first >> 4 == 3 and (first >> 1) & 3 == 1, (first, body)
                topic_end = 2 + int.from_bytes(body[:2], "big")
                assert body[2:topic_end].decode() == TOPIC_A
                delivery_id = int.from_bytes(body[topic_end:topic_end + 2], "big")
                assert body[topic_end + 2] == 0  # empty properties
                assert body[topic_end + 3:] == event(9001)
                subscriber.send(frame(0x40, delivery_id.to_bytes(2, "big")))
                subscriber.send(frame(0xe0))
            finally:
                subscriber.close()
            limited = RawClient("127.0.0.1", broker.port)
            try:
                connect(limited, "v5-small-packet", maximum_packet_size=64)
                subscribe(limited, 2)
                publish(limited, 9002, 2)
                assert limited.recv() == (0x40, b"\0\2")
                assert limited.recv() == (0xe0, b"\x95\0")
            finally:
                limited.close()

            qos2 = RawClient("127.0.0.1", broker.port)
            try:
                connect(qos2, "v5-qos2")
                subscribe(qos2, 3, options=2)
                body = binary(TOPIC_A.encode()) + b"\0\4\0" + event(9003)
                qos2.send(frame(0x34, body))
                assert qos2.recv() == (0x50, b"\0\4")
                qos2.send(frame(0x62, b"\0\4"))
                assert qos2.recv() == (0x70, b"\0\4")
                first, body = qos2.recv()
                assert first >> 4 == 3 and (first >> 1) & 3 == 2
                topic_end = 2 + int.from_bytes(body[:2], "big")
                delivery_id = body[topic_end:topic_end + 2]
                qos2.send(frame(0x50, delivery_id))
                assert qos2.recv() == (0x62, delivery_id)
                qos2.send(frame(0x70, delivery_id))
                qos2.send(frame(0x62, b"\0\x64"))
                assert qos2.recv() == (0x70, b"\0\x64\x92\0")
                qos2.send(frame(0xe0))
            finally:
                qos2.close()

            no_local = RawClient("127.0.0.1", broker.port)
            try:
                connect(no_local, "v5-no-local", expiry=60)
                subscribe(no_local, 5, options=0x05)
                publish(no_local, 9004, 5)
                assert no_local.recv() == (0x40, b"\0\5")
                no_packet(no_local)
                no_local.send(frame(0xe0))
            finally:
                no_local.close()

            publisher = RawClient("127.0.0.1", broker.port)
            try:
                connect(publisher, "v5-expiry-pub")
                publish(publisher, 9005, 6, retain=True, expiry=2)
                assert publisher.recv() == (0x40, b"\0\6")
                publisher.send(frame(0xe0))
            finally:
                publisher.close()
            retained = RawClient("127.0.0.1", broker.port)
            try:
                connect(retained, "v5-retain-options")
                subscribe(retained, 7, options=0x21)  # Retain Handling 2
                no_packet(retained)
                subscribe(retained, 8, options=0x01)  # Retain Handling 0
                first, body = retained.recv()
                assert first & 1 == 1 and first >> 4 == 3
                topic_end = 2 + int.from_bytes(body[:2], "big")
                delivery_id = body[topic_end:topic_end + 2]
                assert body[topic_end + 2:topic_end + 4] == b"\x05\x02"
                remaining = int.from_bytes(body[topic_end + 4:topic_end + 8], "big")
                assert 1 <= remaining <= 2, remaining
                retained.send(frame(0x40, delivery_id))
                retained.send(frame(0xe0))
            finally:
                retained.close()
            time.sleep(2.1)
            expired = RawClient("127.0.0.1", broker.port)
            try:
                connect(expired, "v5-expired-retained")
                subscribe(expired, 9)
                no_packet(expired)
                expired.send(frame(0xe0))
            finally:
                expired.close()

            session = RawClient("127.0.0.1", broker.port)
            try:
                assert connect(session, "v5-session-expiry", clean=False, expiry=1)[0] == 0
                session.send(frame(0xe0))
            finally:
                session.close()
            time.sleep(1.2)
            session = RawClient("127.0.0.1", broker.port)
            try:
                assert connect(session, "v5-session-expiry", clean=False, expiry=60)[0] == 0
                session.send(frame(0xe0))
            finally:
                session.close()

            will = RawClient("127.0.0.1", broker.port)
            connect(will, "v5-will-delay", clean=False, expiry=60,
                    will=(TOPIC_A, event(9006), 1))
            will.close()  # abnormal close, so Will Delay applies
            time.sleep(2.1)  # maintenance cadence is one second
            observer = RawClient("127.0.0.1", broker.port)
            try:
                connect(observer, "v5-will-observer")
                subscribe(observer, 10)
                first, body = observer.recv()
                assert first >> 4 == 3 and first & 1 == 1
                topic_end = 2 + int.from_bytes(body[:2], "big")
                observer.send(frame(0x40, body[topic_end:topic_end + 2]))
                observer.send(frame(0xe0))
            finally:
                observer.close()
        finally:
            broker.stop()
            if broker.process.stderr:
                diagnostics = broker.process.stderr.read().strip()
                if diagnostics:
                    print(diagnostics)
    print("MQTT 5 raw QoS1/2, expiry, subscription options, Will Delay, packet bound: PASS")


if __name__ == "__main__":
    main()
