"""Raw MQTT 5 integration checks against the NetbaIoT development broker."""

from __future__ import annotations

import pathlib
import json
import socket
import tempfile
import time
import urllib.request

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
            will: tuple[str, bytes, int] | None = None,
            password: str = PASSWORD, expected_reason: int = 0,
            will_retain: bool = True) -> bytes:
    properties = b"\x11" + expiry.to_bytes(4, "big") + b"\x21" + receive_maximum.to_bytes(2, "big")
    if maximum_packet_size is not None:
        properties += b"\x27" + maximum_packet_size.to_bytes(4, "big")
    flags = 0xc0 | (2 if clean else 0)
    payload = binary(client_id.encode())
    if will:
        will_topic, will_payload, delay = will
        flags |= 0x0c | (0x20 if will_retain else 0)  # Will QoS1
        payload += b"\x05\x18" + delay.to_bytes(4, "big")
        payload += binary(will_topic.encode()) + binary(will_payload)
    payload += binary(USERNAME_A.encode()) + binary(password.encode())
    body = (binary(b"MQTT") + bytes([5, flags, 0, 30])
            + bytes([len(properties)]) + properties + payload)
    client.send(frame(0x10, body))
    first, body = client.recv()
    assert first == 0x20 and body[1] == expected_reason, (first, body)
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


def v5_password_only_connect_reaches_auth_rejection(port: int) -> None:
    client = RawClient("127.0.0.1", port)
    try:
        body = (binary(b"MQTT") + b"\x05\x42\0\x1e\0"
                + binary(b"v5-password-only") + binary(PASSWORD.encode()))
        client.send(frame(0x10, body))
        assert client.recv() == (0x20, b"\0\x86\0")
        client.expect_closed()
    finally:
        client.close()


def events_accepted(broker) -> int:
    assert broker.config_path is not None
    address = json.loads(broker.config_path.read_text())["management_http"]
    request = urllib.request.Request(
        f"http://{address}/api/v1/metrics",
        headers={"Authorization": f"Bearer {'d' * 64}"},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=2) as response:
        metrics = response.read().decode()
    prefix = "netbaiot_events_accepted_total "
    return int(next(line.removeprefix(prefix) for line in metrics.splitlines()
                    if line.startswith(prefix)))


def takeover_without_will_sends_0x8e_only(port: int) -> None:
    old = RawClient("127.0.0.1", port)
    new = RawClient("127.0.0.1", port)
    try:
        connect(old, "v5-no-will-takeover", clean=False, expiry=60)
        assert connect(new, "v5-no-will-takeover", clean=False, expiry=60)[0] == 1
        assert old.recv() == (0xe0, b"\x8e\0")
        old.expect_closed()
        new.send(frame(0xc0))
        assert new.recv() == (0xd0, b"")
        new.send(frame(0xe0))
    finally:
        old.close()
        new.close()


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="netbaiot-v5-") as temporary:
        broker = start_netbaiot(pathlib.Path(temporary))
        try:
            v5_password_only_connect_reaches_auth_rejection(broker.port)
            takeover_without_will_sends_0x8e_only(broker.port)
            for name, wire, expected in (
                ("unknown-puback", frame(0x40, b"\0\x07"), 0x82),
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

            unknown_pubrec = RawClient("127.0.0.1", broker.port)
            try:
                connect(unknown_pubrec, "v5-unknown-pubrec")
                unknown_pubrec.send(frame(0x50, b"\0\x07"))
                assert unknown_pubrec.recv() == (0x62, b"\0\x07\x92\0")
                unknown_pubrec.send(frame(0xc0))
                assert unknown_pubrec.recv() == (0xd0, b"")
                unknown_pubrec.send(frame(0xe0))
            finally:
                unknown_pubrec.close()

            tiny_auth = RawClient("127.0.0.1", broker.port)
            try:
                response = connect(tiny_auth, "v5-tiny-auth", maximum_packet_size=5,
                                   password="invalid", expected_reason=0x86)
                assert response == b"\0\x86\0"
                tiny_auth.expect_closed()
            finally:
                tiny_auth.close()

            tiny_will = RawClient("127.0.0.1", broker.port)
            try:
                response = connect(tiny_will, "v5-tiny-forbidden-will", maximum_packet_size=5,
                                   will=(TOPIC_A[:-2] + "down", b"will", 0),
                                   expected_reason=0x87)
                assert response == b"\0\x87\0"
                tiny_will.expect_closed()
            finally:
                tiny_will.close()

            tiny_success = RawClient("127.0.0.1", broker.port)
            try:
                response = connect(tiny_success, "v5-tiny-success", maximum_packet_size=17)
                assert response[:3] == b"\0\0\x0c" and response[3] == 0x21, response
                tiny_success.send(frame(0xe0))
            finally:
                tiny_success.close()

            too_tiny = RawClient("127.0.0.1", broker.port)
            try:
                properties = b"\x11\0\0\0\0\x21\0\x04\x27\0\0\0\x04"
                payload = binary(b"v5-too-tiny") + binary(USERNAME_A.encode()) + binary(PASSWORD.encode())
                body = binary(b"MQTT") + b"\x05\xc2\0\x1e" + bytes([len(properties)]) + properties + payload
                too_tiny.send(frame(0x10, body))
                too_tiny.expect_closed()
            finally:
                too_tiny.close()

            unsubscribe_client = RawClient("127.0.0.1", broker.port)
            try:
                connect(unsubscribe_client, "v5-unsubscribe")
                subscribe(unsubscribe_client, 11)
                body = b"\0\x0c\0" + binary(TOPIC_A.encode())
                unsubscribe_client.send(frame(0xa2, body))
                assert unsubscribe_client.recv() == (0xb0, b"\0\x0c\0\0")
                unsubscribe_client.send(frame(0xa2, body))
                assert unsubscribe_client.recv() == (0xb0, b"\0\x0c\0\x11")
                unsubscribe_client.send(frame(0xe0))
            finally:
                unsubscribe_client.close()

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
                no_packet(limited)
                limited.send(frame(0xc0))
                assert limited.recv() == (0xd0, b"")
                limited.send(frame(0xe0))
            finally:
                limited.close()

            limited_qos0 = RawClient("127.0.0.1", broker.port)
            try:
                connect(limited_qos0, "v5-small-qos0", maximum_packet_size=64)
                subscribe(limited_qos0, 13, options=0)
                body = binary(TOPIC_A.encode()) + b"\0" + event(9002)
                limited_qos0.send(frame(0x30, body))
                no_packet(limited_qos0)
                limited_qos0.send(frame(0xc0))
                assert limited_qos0.recv() == (0xd0, b"")
                limited_qos0.send(frame(0xe0))
            finally:
                limited_qos0.close()

            following = RawClient("127.0.0.1", broker.port)
            try:
                connect(following, "v5-small-after-large", maximum_packet_size=150)
                subscribe(following, 14)
                large = event(9010) + b" " * 200
                body = binary(TOPIC_A.encode()) + b"\0\x0f\0" + large
                following.send(frame(0x32, body))
                assert following.recv() == (0x40, b"\0\x0f")
                no_packet(following)
                publish(following, 9011, 16)
                received = [following.recv(), following.recv()]
                assert (0x40, b"\0\x10") in received, received
                first, body = next(item for item in received if item[0] >> 4 == 3)
                assert first >> 4 == 3 and body.endswith(event(9011)), (first, body)
                topic_end = 2 + int.from_bytes(body[:2], "big")
                following.send(frame(0x40, body[topic_end:topic_end + 2]))
                following.send(frame(0xe0))
            finally:
                following.close()

            qos2 = RawClient("127.0.0.1", broker.port)
            try:
                connect(qos2, "v5-qos2")
                subscribe(qos2, 3, options=2)
                accepted_before = events_accepted(broker)
                body = binary(TOPIC_A.encode()) + b"\0\4\x05\x02\0\0\0\x05" + event(9003)
                qos2.send(frame(0x34, body))
                assert qos2.recv() == (0x50, b"\0\4")
                time.sleep(0.02)
                qos2.send(frame(0x34, body))
                assert qos2.recv() == (0x50, b"\0\4")
                changed = binary(TOPIC_A.encode()) + b"\0\4\0" + event(9303)
                qos2.send(frame(0x34, changed))
                assert qos2.recv() == (0x50, b"\0\4")
                qos2.send(frame(0x3c, changed))
                assert qos2.recv() == (0x50, b"\0\4")
                assert events_accepted(broker) == accepted_before
                qos2.send(frame(0x62, b"\0\4"))
                assert qos2.recv() == (0x70, b"\0\4")
                assert events_accepted(broker) == accepted_before + 1
                first, body = qos2.recv()
                assert first >> 4 == 3 and (first >> 1) & 3 == 2
                assert body.endswith(event(9003)), body
                topic_end = 2 + int.from_bytes(body[:2], "big")
                delivery_id = body[topic_end:topic_end + 2]
                qos2.send(frame(0x50, delivery_id))
                assert qos2.recv() == (0x62, delivery_id)
                qos2.send(frame(0x70, delivery_id))
                no_packet(qos2)
                assert events_accepted(broker) == accepted_before + 1
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
            for reason, sequence, expected_sequence in ((0, 9100, 9006), (4, 9104, 9104),
                                                      (0x82, 9182, 9182)):
                will_client = RawClient("127.0.0.1", broker.port)
                try:
                    connect(will_client, f"v5-will-reason-{reason}",
                            will=(TOPIC_A, event(sequence), 0))
                    will_client.send(frame(0xe0, b"" if reason == 0 else bytes([reason, 0])))
                    will_client.expect_closed()
                finally:
                    will_client.close()
                observer = RawClient("127.0.0.1", broker.port)
                try:
                    connect(observer, f"v5-will-reason-observer-{reason}")
                    subscribe(observer, 20 + reason)
                    first, body = observer.recv()
                    assert first >> 4 == 3 and body.endswith(event(expected_sequence)), (first, body)
                    topic_end = 2 + int.from_bytes(body[:2], "big")
                    observer.send(frame(0x40, body[topic_end:topic_end + 2]))
                    observer.send(frame(0xe0))
                finally:
                    observer.close()

            old = RawClient("127.0.0.1", broker.port)
            new = RawClient("127.0.0.1", broker.port)
            try:
                connect(old, "v5-takeover", clean=False, expiry=60,
                        will=(TOPIC_A, event(9199), 0))
                response = connect(new, "v5-takeover", clean=False, expiry=60)
                assert response[0] == 1
                assert old.recv() == (0xe0, b"\x8e\0")
                new.send(frame(0xc0))
                assert new.recv() == (0xd0, b"")
                new.send(frame(0xe0))
            finally:
                old.close()
                new.close()
            observer = RawClient("127.0.0.1", broker.port)
            try:
                connect(observer, "v5-takeover-observer")
                subscribe(observer, 99)
                first, body = observer.recv()
                assert first >> 4 == 3 and body.endswith(event(9199)), (first, body)
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
