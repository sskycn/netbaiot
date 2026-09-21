#!/usr/bin/env python3
"""Owned loopback HTTP sink with bounded clients/body, configurable delay and metrics."""
import asyncio, json, os, signal, sys, time

async def main():
    port, control = int(sys.argv[1]), sys.argv[2]
    stop = asyncio.Event()
    tasks = set()
    stats = dict(received=0, rejected=0, active=0, max_active=0)
    latency_ms = [0] * 60001
    latency_count = 0
    settings_value = {"delay": 0, "status": 204}
    settings_loaded_at = 0.0
    def settings():
        nonlocal settings_value, settings_loaded_at
        now = time.monotonic()
        if now - settings_loaded_at >= .1:
            settings_value = json.loads(open(control).read())
            settings_loaded_at = now
        return settings_value
    def percentiles():
        if latency_count == 0: return dict(count=0, p50_ms=0, p95_ms=0, p99_ms=0)
        result = dict(count=latency_count)
        for name, ratio in (("p50_ms", .50), ("p95_ms", .95), ("p99_ms", .99)):
            target = max(1, int(latency_count * ratio + .999999))
            seen = 0
            for value, count in enumerate(latency_ms):
                seen += count
                if seen >= target:
                    result[name] = value
                    break
        return result
    async def client(reader, writer):
        nonlocal latency_count
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
                body = await asyncio.wait_for(reader.readexactly(size), 5)
                try:
                    event = json.loads(body)
                    received_at = int(event["received_at"])
                    elapsed = max(0, min(60000, int(time.time() * 1000) - received_at))
                    latency_ms[elapsed] += 1
                    latency_count += 1
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    stats['rejected'] += 1
                current = settings()
                await asyncio.sleep(min(float(current.get('delay', 0)), 10))
                status = int(current.get('status', 204))
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
    await stop.wait()
    print(json.dumps(dict(epoch=time.time(), latency=percentiles(), **stats)), flush=True)
    server.close(); await server.wait_closed()
    for task in list(tasks): task.cancel()
    await asyncio.gather(*list(tasks), return_exceptions=True)

if __name__ == '__main__': asyncio.run(main())
