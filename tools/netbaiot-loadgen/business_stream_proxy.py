#!/usr/bin/env python3
"""Bounded test-only TCP stream proxy. It does not simulate packet loss."""
import argparse
import asyncio
import json
import random
import time


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--listen", default="127.0.0.1:19102")
    parser.add_argument("--target", default="127.0.0.1:19002")
    parser.add_argument("--delay-ms", type=float, default=0)
    parser.add_argument("--jitter-ms", type=float, default=0)
    parser.add_argument("--bytes-per-second", type=int, default=0)
    parser.add_argument("--blackhole-after-secs", type=float)
    parser.add_argument("--blackhole-for-secs", type=float, default=0)
    parser.add_argument("--disconnect-after-secs", type=float)
    parser.add_argument("--reset-after-secs", type=float)
    parser.add_argument("--half-open-after-secs", type=float)
    parser.add_argument("--max-connections", type=int, default=8)
    parser.add_argument("--capture-prefix", help="test-only prefix for bounded V3 frame-header traces")
    args = parser.parse_args()
    if (args.delay_ms < 0 or args.jitter_ms < 0 or args.bytes_per_second < 0
            or args.max_connections < 1 or args.max_connections > 64
            or args.blackhole_for_secs < 0):
        parser.error("invalid bounded proxy parameters")

    def address(value):
        host, port = value.rsplit(":", 1)
        if host not in ("127.0.0.1", "localhost", "::1"):
            parser.error("proxy endpoints must be loopback")
        return host, int(port)

    target_host, target_port = address(args.target)
    active = set()
    connection_number = 0

    async def serve(reader, writer):
        nonlocal connection_number
        if len(active) >= args.max_connections:
            writer.close()
            await writer.wait_closed()
            return
        current = asyncio.current_task()
        active.add(current)
        connection_number += 1
        capture_number = connection_number
        remote = None
        started = time.monotonic()
        try:
            remote_reader, remote = await asyncio.wait_for(
                asyncio.open_connection(target_host, target_port), timeout=5)

            async def pump(source, sink, upstream):
                # A V3 trace stores only frame headers, never Hello tokens or Event bodies.
                capture = args.capture_prefix is not None and not upstream
                capture_buffer = bytearray()
                capture_seen = 0
                bootstrap_done = False
                while True:
                    elapsed = time.monotonic() - started
                    if args.disconnect_after_secs is not None and elapsed >= args.disconnect_after_secs:
                        return
                    if args.reset_after_secs is not None and elapsed >= args.reset_after_secs:
                        writer.transport.abort()
                        remote.transport.abort()
                        return
                    if upstream and args.half_open_after_secs is not None and elapsed >= args.half_open_after_secs:
                        if sink.can_write_eof():
                            sink.write_eof()
                            await sink.drain()
                        return "half_open"
                    try:
                        chunk = await asyncio.wait_for(source.read(16 * 1024), timeout=0.25)
                    except asyncio.TimeoutError:
                        continue
                    if not chunk:
                        if sink.can_write_eof():
                            sink.write_eof()
                            await sink.drain()
                        return
                    read_chunk_bytes = len(chunk)
                    if capture:
                        remaining = max(0, 2 * 1024 * 1024 - capture_seen)
                        capture_buffer.extend(chunk[:remaining])
                        capture_seen += min(len(chunk), remaining)
                        if not bootstrap_done and len(capture_buffer) >= 4:
                            length = int.from_bytes(capture_buffer[:4], "big")
                            if length > 4096:
                                capture = False
                            elif len(capture_buffer) >= 4 + length:
                                del capture_buffer[:4 + length]
                                bootstrap_done = True
                        if capture and bootstrap_done:
                            trace = f"{args.capture_prefix}-{capture_number}-down.jsonl"
                            with open(trace, "a", encoding="utf-8") as output:
                                while len(capture_buffer) >= 12:
                                    length = int.from_bytes(capture_buffer[:4], "big")
                                    if length > 16 * 1024:
                                        capture = False
                                        break
                                    if len(capture_buffer) < 12 + length:
                                        break
                                    output.write(json.dumps({
                                        "observed_at_ns": time.monotonic_ns(),
                                        "read_chunk_bytes": read_chunk_bytes,
                                        "stream_id": int.from_bytes(capture_buffer[4:8], "big"),
                                        "frame_type": capture_buffer[8],
                                        "flags": capture_buffer[9],
                                        "payload_bytes": length,
                                    }) + "\n")
                                    del capture_buffer[:12 + length]
                        if not capture or capture_seen >= 2 * 1024 * 1024:
                            capture_buffer.clear()
                            capture = False
                    if args.blackhole_after_secs is not None:
                        until = started + args.blackhole_after_secs + args.blackhole_for_secs
                        if time.monotonic() >= started + args.blackhole_after_secs:
                            await asyncio.sleep(max(0, until - time.monotonic()))
                    wait = (args.delay_ms + random.uniform(0, args.jitter_ms)) / 1000
                    if args.bytes_per_second:
                        wait += len(chunk) / args.bytes_per_second
                    if wait:
                        await asyncio.sleep(wait)
                    sink.write(chunk)
                    await sink.drain()

            flows = [asyncio.create_task(pump(reader, remote, True)),
                     asyncio.create_task(pump(remote_reader, writer, False))]
            done, pending = await asyncio.wait(flows, return_when=asyncio.FIRST_COMPLETED)
            if any(task.result() == "half_open" for task in done if not task.cancelled() and task.exception() is None):
                done_again, pending = await asyncio.wait(pending, timeout=30)
                done |= done_again
            for task in pending:
                task.cancel()
            await asyncio.gather(*done, *pending, return_exceptions=True)
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            if remote is not None:
                remote.close()
            await asyncio.gather(writer.wait_closed(), *([remote.wait_closed()] if remote else []), return_exceptions=True)
            active.discard(current)

    host, port = address(args.listen)
    server = await asyncio.start_server(serve, host, port, limit=64 * 1024)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
