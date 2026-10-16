#!/usr/bin/env python3
"""Send one netbaiot-json-v1 uplink over the generic length-prefixed TCP transport."""

import argparse
import json
import socket
import struct
import uuid


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


def recv_frame(sock):
    length = struct.unpack("!I", recv_exact(sock, 4))[0]
    if length == 0 or length > 65_536:
        raise ValueError(f"invalid frame length {length}")
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
    if struct.unpack("!I", recv_exact(sock, 4))[0] != 42:
        raise ValueError("EventAccepted ACK body must be exactly 42 bytes")
    return decode_ack(recv_exact(sock, 42))


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
        print("acceptance:", recv_ack(sock))
        if args.wait_command:
            sock.settimeout(60)
            print("command:", recv_frame(sock))


if __name__ == "__main__":
    main()
