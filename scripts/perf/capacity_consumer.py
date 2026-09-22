#!/usr/bin/env python3
"""Bounded confirmed-TCP consumer for capacity audits, including controlled outages.

One connection, one received frame and one ACK at a time. Receipt-to-ACK latency
uses the consumer monotonic clock. Event age is only valid on the same host and
is deliberately omitted: it would otherwise invite cross-host clock mistakes.
"""
import argparse
import asyncio
import json
import pathlib
import signal
import struct
import time

MAX_FRAME = 65536
TOKEN = 'ab' * 32  # Isolated benchmark credential only.


async def read_frame(reader):
    size = struct.unpack('!I', await reader.readexactly(4))[0]
    if not 0 < size <= MAX_FRAME:
        raise ValueError('frame bound exceeded')
    return json.loads(await reader.readexactly(size))


async def write_frame(writer, value):
    data = json.dumps(value, separators=(',', ':')).encode()
    if len(data) > MAX_FRAME:
        raise ValueError('frame bound exceeded')
    writer.write(struct.pack('!I', len(data)) + data)
    await asyncio.wait_for(writer.drain(), 5)


async def run(args):
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    started = time.monotonic()
    state = dict(received=0, ack_sent=0, reconnects=0, errors=0, connected=False,
                 ack_latency_ms_max=0, ack_latency_ms_sum=0, outstanding=0)
    histogram = [0] * 60001
    def snapshot(event):
        result = dict(event=event, epoch=time.time(), elapsed_s=time.monotonic()-started, **state)
        for name, fraction in (('p50', .50), ('p95', .95), ('p99', .99)):
            target = max(1, int(state['ack_sent']*fraction+.999999))
            count = 0
            for i, n in enumerate(histogram):
                count += n
                if count >= target:
                    result['receipt_to_ack_'+name+'_ms_upper'] = i+1
                    break
        print(json.dumps(result), flush=True)
    async def delay(seconds):
        try:
            await asyncio.wait_for(stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass
    async def worker():
        await delay(args.connect_delay)
        disconnected = False
        while not stop.is_set():
            writer = None
            pending_read = None
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(args.host, args.port, limit=MAX_FRAME+4), 5)
                writer.transport.set_write_buffer_limits(high=MAX_FRAME, low=16384)
                await write_frame(writer, dict(type='hello', version=1, token=TOKEN))
                mismatch = time.monotonic()-started < args.filter_mismatch_seconds
                await write_frame(writer, dict(type='subscribe', version=1, subscription_id='00000000-0000-4000-8000-000000000001',
                                              filter={'tenant':'unmatched'} if mismatch else {}))
                frame = await asyncio.wait_for(read_frame(reader), 5)
                if frame.get('type') != 'ready' or frame.get('version') != 1:
                    raise ValueError('consumer handshake rejected')
                state['connected'] = True
                snapshot('ready')
                if args.ready_file:
                    pathlib.Path(args.ready_file).write_text('ready\n')
                connected_at = time.monotonic()
                while not stop.is_set():
                    if mismatch and time.monotonic()-started >= args.filter_mismatch_seconds:
                        break
                    if args.disconnect_after and not disconnected and time.monotonic()-connected_at >= args.disconnect_after:
                        disconnected = True
                        break
                    if pending_read is None:
                        pending_read = asyncio.create_task(read_frame(reader))
                    try:
                        frame = await asyncio.wait_for(asyncio.shield(pending_read), .5)
                    except asyncio.TimeoutError:
                        continue
                    pending_read = None
                    if frame.get('type') != 'event' or frame.get('version') != 1:
                        raise ValueError('unexpected server frame')
                    delivery = frame['delivery']
                    received = time.monotonic()
                    state['received'] += 1
                    state['outstanding'] = 1
                    await delay(args.ack_delay_ms/1000)
                    await write_frame(writer, dict(type='ack', version=1, ack=dict(
                        delivery_id=delivery['delivery_id'], subscription_id=delivery['subscription_id'],
                        event_id=delivery['event']['event_id'])))
                    latency = (time.monotonic()-received)*1000
                    state['ack_sent'] += 1
                    state['outstanding'] = 0
                    state['ack_latency_ms_sum'] += latency
                    state['ack_latency_ms_max'] = max(state['ack_latency_ms_max'], latency)
                    histogram[min(60000, int(latency))] += 1
            except (OSError, ValueError, KeyError, TypeError, asyncio.IncompleteReadError, asyncio.TimeoutError):
                if not stop.is_set():
                    state['errors'] += 1
            finally:
                if pending_read is not None:
                    pending_read.cancel()
                    await asyncio.gather(pending_read, return_exceptions=True)
                state['connected'] = False
                state['outstanding'] = 0
                if writer is not None:
                    writer.close()
                    try:
                        await asyncio.wait_for(writer.wait_closed(), 2)
                    except (OSError, asyncio.TimeoutError):
                        pass
            if not stop.is_set():
                state['reconnects'] += 1
                snapshot('disconnected')
                await delay(args.reconnect_delay)
    task = asyncio.create_task(worker())
    try:
        while not stop.is_set() and time.monotonic()-started < args.max_seconds:
            await delay(1)
            if task.done():
                await task
            snapshot('sample')
    finally:
        stop.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        snapshot('final')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=24005)
    parser.add_argument('--ack-delay-ms', type=float, default=0)
    parser.add_argument('--connect-delay', type=float, default=0)
    parser.add_argument('--disconnect-after', type=float, default=0)
    parser.add_argument('--reconnect-delay', type=float, default=1)
    parser.add_argument('--filter-mismatch-seconds', type=float, default=0)
    parser.add_argument('--max-seconds', type=float, default=300)
    parser.add_argument('--ready-file')
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or not 0 < args.max_seconds <= 20000:
        parser.error('invalid port/lifetime')
    if not 0 <= args.ack_delay_ms <= 1000 or any(not 0 <= x <= 14400 for x in
        (args.connect_delay, args.disconnect_after, args.reconnect_delay, args.filter_mismatch_seconds)):
        parser.error('invalid bounded delay')
    if not .05 <= args.reconnect_delay <= 60:
        parser.error('reconnect delay must be 0.05..60 seconds')
    asyncio.run(run(args))


if __name__ == '__main__':
    main()
