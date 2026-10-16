#!/usr/bin/env python3
"""Send one netbaiot-json-v1 uplink over the generic length-prefixed TCP transport."""

import argparse
import json
import socket
import struct
import uuid

PAYLOAD = 0
EVENT_ACCEPTED = 1
ACK_FLAG = 0x80000000


def send_frame(sock, value):
    payload = json.dumps(value, separators=(",", ":")).encode()
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def recv_exact(sock, length):
    chunks = []
    while length:
        chunk = sock.recv(length)
        if not chunk:
            raise ConnectionError("connection closed while reading a frame")
        chunks.append(chunk)
        length -= len(chunk)
    return b"".join(chunks)


def recv_header(sock, expected_kind=None):
    header = struct.unpack("!I", recv_exact(sock, 4))[0]
    kind, length = header >> 31, header & 0x7fffffff
    if length == 0 or length > 65_536:
        raise ValueError(f"invalid frame length {length}")
    if kind == EVENT_ACCEPTED and length != 42:
        raise ValueError("EventAccepted ACK body must be exactly 42 bytes")
    if expected_kind is not None and kind != expected_kind:
        raise ValueError("unexpected TCP frame type")
    return kind, length


def recv_frame(sock):
    _, length = recv_header(sock, PAYLOAD)
    return json.loads(recv_exact(sock, length))


def decode_ack(body):
    """Decode the independent 42-byte binary EventAccepted ACK v1 body."""
    if len(body) != 42:
        raise ValueError("EventAccepted ACK body must be exactly 42 bytes")
    version, kind, event, accepted_at, required, best_effort = struct.unpack("!BB16sqQQ", body)
    if (version, kind) != (1, 1):
        raise ValueError("unsupported EventAccepted ACK version/type")
    return dict(event_id=str(uuid.UUID(bytes=event)), accepted_at=accepted_at,
                required_deliveries=required, best_effort_deliveries=best_effort)


def recv_ack(sock):
    recv_header(sock, EVENT_ACCEPTED)
    return decode_ack(recv_exact(sock, 42))


def recv_downlink(sock):
    """Dispatch by frame type, never by vendor payload contents."""
    kind, length = recv_header(sock)
    body = recv_exact(sock, length)
    return kind, decode_ack(body) if kind == EVENT_ACCEPTED else body


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1:9000")
    parser.add_argument("--credential-id", default="demo-device")
    parser.add_argument(
        "--secret",
        default="000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    )
    parser.add_argument("--wait-command", action="store_true")
    args = parser.parse_args()
    host, port = args.address.rsplit(":", 1)

    with socket.create_connection((host, int(port)), timeout=10) as sock:
        send_frame(sock, {"credential_id": args.credential_id, "secret": args.secret})
        print("handshake:", recv_frame(sock))
        send_frame(
            sock,
            {
                "schema_version": 1,
                "source_message_id": "tcp-demo:1",
                "kind": "heartbeat",
                "data": {"sequence": 1},
            },
        )
        accepted = commanded = False
        while not accepted or (args.wait_command and not commanded):
            kind, value = recv_downlink(sock)
            if kind == EVENT_ACCEPTED:
                print("acceptance:", value)
                accepted = True
                if args.wait_command:
                    sock.settimeout(60)
            else:
                print("command:", json.loads(value))
                commanded = True


if __name__ == "__main__":
    main()
