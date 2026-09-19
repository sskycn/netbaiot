#!/usr/bin/env python3
"""Owned loopback HTTP sink with bounded clients/body, configurable delay and metrics."""
import asyncio, json, os, signal, sys, time

async def main():
    port, control = int(sys.argv[1]), sys.argv[2]
    stop = asyncio.Event()
    tasks = set()
    stats = dict(received=0, rejected=0, active=0, max_active=0)
    async def client(reader, writer):
        task = asyncio.current_task(); tasks.add(task)
        stats['active'] += 1; stats['max_active'] = max(stats['max_active'], stats['active'])
        try:
            if stats['active'] > 64:
                stats['rejected'] += 1; return
            while not stop.is_set():
                try:
                    header = await asyncio.wait_for(reader.readuntil(b'\r\n\r\n'), 10)
                except (asyncio.IncompleteReadError, ConnectionError):
                    break
                fields = dict(line.split(b':', 1) for line in header.split(b'\r\n')[1:] if b':' in line)
                size = int(next((v for k,v in fields.items() if k.lower()==b'content-length'), b'0'))
                if not 0 <= size <= 65536: raise ValueError('body limit')
                await asyncio.wait_for(reader.readexactly(size), 5)
                settings = json.loads(open(control).read())
                await asyncio.sleep(min(float(settings.get('delay', 0)), 10))
                status = int(settings.get('status', 204))
                writer.write(f'HTTP/1.1 {status} Test\r\nContent-Length: 0\r\nConnection: keep-alive\r\n\r\n'.encode())
                await asyncio.wait_for(writer.drain(), 5)
                stats['received'] += 1
        except (Exception, asyncio.CancelledError):
            stats['rejected'] += 1
        finally:
            stats['active'] -= 1; writer.close()
            try: await writer.wait_closed()
            except Exception: pass
            tasks.discard(task)
    server = await asyncio.start_server(client, '127.0.0.1', port, limit=8192)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT): loop.add_signal_handler(sig, stop.set)
    print(json.dumps(dict(event='ready', pid=os.getpid())), flush=True)
    while not stop.is_set():
        try: await asyncio.wait_for(stop.wait(), 1)
        except asyncio.TimeoutError: pass
        print(json.dumps(dict(epoch=time.time(), **stats)), flush=True)
    server.close(); await server.wait_closed()
    for task in list(tasks): task.cancel()
    await asyncio.gather(*list(tasks), return_exceptions=True)

if __name__ == '__main__': asyncio.run(main())
