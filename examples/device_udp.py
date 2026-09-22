#!/usr/bin/env python3
"""Send one authenticated NBI1 UDP v1 datagram."""

import argparse
import hashlib
import hmac
import json
import socket
import struct
import time
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1:9001")
    parser.add_argument("--credential-id", default="demo-device")
    parser.add_argument(
        "--secret-hex",
        default="000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    )
    parser.add_argument("--credential-version", type=int, default=1)
    parser.add_argument("--sequence", type=int, default=1)
    args = parser.parse_args()
    host, port = args.address.rsplit(":", 1)
    credential = args.credential_id.encode()
    payload = json.dumps(
        {
            "schema_version": 1,
            "source_message_id": f"udp-demo:{args.sequence}",
            "kind": "heartbeat",
            "data": {"sequence": args.sequence},
        },
        separators=(",", ":"),
    ).encode()
    if not 1 <= len(credential) <= 64 or len(payload) > 65_535:
        raise ValueError("credential or payload is too large")
    signed = b"".join(
        [
            b"NBI1",
            struct.pack("!B", len(credential)),
            credential,
            struct.pack("!I", args.credential_version),
            uuid.uuid4().bytes,
            struct.pack("!QqH", args.sequence, int(time.time() * 1000), len(payload)),
            payload,
        ]
    )
    datagram = signed + hmac.new(bytes.fromhex(args.secret_hex), signed, hashlib.sha256).digest()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sent = sock.sendto(datagram, (host, int(port)))
    print(f"sent {sent} bytes; UDP intentionally has no response")


if __name__ == "__main__":
    main()
