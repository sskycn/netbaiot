#!/usr/bin/env python3
"""Send one netbaiot-json-v1 uplink over the generic length-prefixed TCP transport."""

import argparse
import json
import socket
import struct


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
        print("acceptance:", recv_frame(sock))
        if args.wait_command:
            sock.settimeout(60)
            print("command:", recv_frame(sock))


if __name__ == "__main__":
    main()
