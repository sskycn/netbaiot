#!/usr/bin/env python3
"""Dependency-free confirmed business stream client with explicit application ACK."""

import argparse
import json
import socket
import struct
import uuid


def send_frame(sock, value):
    payload = json.dumps(value, separators=(",", ":")).encode()
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def recv_exact(sock, length):
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ConnectionError("business stream closed")
        data.extend(chunk)
    return bytes(data)


def recv_frame(sock):
    length = struct.unpack("!I", recv_exact(sock, 4))[0]
    if length == 0 or length > 1_048_576:
        raise ValueError(f"invalid frame length {length}")
    return json.loads(recv_exact(sock, length))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1:9100")
    parser.add_argument("--token", required=True)
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()
    host, port = args.address.rsplit(":", 1)
    subscription_id = str(uuid.uuid4())
    seen = set()
    with socket.create_connection((host, int(port)), timeout=10) as sock:
        send_frame(sock, {"type": "hello", "version": 1, "token": args.token})
        send_frame(
            sock,
            {
                "type": "subscribe",
                "version": 1,
                "subscription_id": subscription_id,
                "filter": {"tenant": None, "product": None, "device": None, "event_types": []},
            },
        )
        ready = recv_frame(sock)
        if ready.get("type") != "ready":
            raise RuntimeError(f"stream was not accepted: {ready}")
        print("ready:", ready)
        for _ in range(args.count):
            frame = recv_frame(sock)
            delivery = frame["delivery"]
            event_id = delivery["event"]["event_id"]
            duplicate = event_id in seen
            if not duplicate:
                # Persist the business change and event_id atomically before ACK.
                seen.add(event_id)
            print(json.dumps({"duplicate": duplicate, "delivery": delivery}, ensure_ascii=False))
            send_frame(
                sock,
                {
                    "type": "ack",
                    "version": 1,
                    "ack": {
                        "delivery_id": delivery["delivery_id"],
                        "subscription_id": delivery["subscription_id"],
                        "event_id": event_id,
                    },
                },
            )


if __name__ == "__main__":
    main()
