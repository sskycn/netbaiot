#!/usr/bin/env python3
"""Send one NBI1 message with bounded exact-datagram retries and NBA1 verification."""

import argparse
import hashlib
import hmac
import json
import random
import socket
import struct
import time
import uuid


def valid_ack(ack, key, version, boot, sequence):
    return (
        len(ack) == 64
        and ack[:4] == b"NBA1"
        and ack[4:8] == struct.pack("!I", version)
        and ack[8:24] == boot
        and ack[24:32] == struct.pack("!Q", sequence)
        and hmac.compare_digest(ack[32:], hmac.new(key, ack[:32], hashlib.sha256).digest())
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", default="127.0.0.1:8080")
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
    boot = uuid.uuid4().bytes
    key = bytes.fromhex(args.secret_hex)
    if len(key) != 32:
        raise ValueError("credential must decode to 32 bytes")
    signed = b"".join(
        [
            b"NBI1",
            struct.pack("!B", len(credential)),
            credential,
            struct.pack("!I", args.credential_version),
            boot,
            struct.pack("!QqH", args.sequence, int(time.time() * 1000), len(payload)),
            payload,
        ]
    )
    datagram = signed + hmac.new(key, signed, hashlib.sha256).digest()
    if len(datagram) > 1200:
        raise ValueError("datagram exceeds the default gateway limit")
    # Five seconds leaves clock-skew margin under the default 30-second policy.
    # Retransmissions reuse all original bytes, including the timestamp and HMAC.
    deadline = time.monotonic() + 5
    backoff = 0.1
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((host, int(port)))
        while time.monotonic() < deadline:
            sock.send(datagram)
            attempt_deadline = min(deadline, time.monotonic() + backoff * random.uniform(0.9, 1.1))
            while time.monotonic() < attempt_deadline:
                sock.settimeout(max(0.001, attempt_deadline - time.monotonic()))
                try:
                    ack = sock.recv(65)
                except socket.timeout:
                    break
                if valid_ack(ack, key, args.credential_version, boot, args.sequence):
                    print(f"EventAccepted: signed NBA1 sequence={args.sequence}; business completion is separate")
                    return
            backoff = min(backoff * 2, 1.6)
    raise SystemExit("delivery uncertain: no valid NBA1 within the retry budget")


if __name__ == "__main__":
    main()
