#!/usr/bin/env python3
"""Paired local UDP accepted throughput/CPU and signed receipt RTT, not production capacity."""
import argparse
import json
import os
from pathlib import Path
import platform
import signal
import socket
import statistics
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]


def request(port, path):
    # Direct loopback connection, independent of host proxy settings.
    with socket.create_connection(('127.0.0.1', port), timeout=3) as stream:
        stream.sendall((f'GET /api/v1/{path} HTTP/1.1\r\nHost: localhost\r\n'
                        f'Authorization: Bearer {"ab" * 32}\r\nConnection: close\r\n\r\n').encode())
        response = bytearray()
        while True:
            chunk = stream.recv(65536)
            if not chunk:
                break
            response.extend(chunk)
            if len(response) > 1_048_576:
                raise ValueError('response limit exceeded')
    head, _, body = response.partition(b'\r\n\r\n')
    if b' 200 ' not in head.split(b'\r\n', 1)[0]:
        raise ValueError(f'HTTP status: {head[:128]!r}')
    return body.decode()


def counters(port):
    return {key.removeprefix('netbaiot_').removesuffix('_total'): int(value)
            for line in request(port, 'metrics').splitlines() if not line.startswith('#')
            for key, value in [line.split()] if '{' not in key and value.isdecimal()}


def cpu(pid):
    raw = subprocess.check_output(['ps', '-p', str(pid), '-o', 'time='], text=True).strip()
    parts = [float(value) for value in raw.split(':')]
    return sum(value * 60 ** n for n, value in enumerate(reversed(parts)))


def run(binary, probe, seconds, mode):
    with tempfile.TemporaryDirectory(prefix='netbaiot-udp-ack-') as folder:
        reservations = [socket.socket() for _ in range(2)]
        for sock in reservations:
            sock.bind(('127.0.0.1', 0))
        device, management = [sock.getsockname()[1] for sock in reservations]
        config = json.loads((ROOT / 'configs/development.json').read_text())
        config.update(device_ingress=f'127.0.0.1:{device}', management_http=f'127.0.0.1:{management}',
                      spool_directory=folder + '/spool')
        config['limits'].update(requests_per_second=2_000_000, requests_per_ip_second=2_000_000,
                                messages_per_device_second=2_000_000, messages_per_tenant_second=2_000_000)
        path = Path(folder) / 'config.json'
        path.write_text(json.dumps(config))
        for sock in reservations:
            sock.close()
        with open(Path(folder) / 'server.log', 'w') as log:
            server = subprocess.Popen([str(binary), str(path)], cwd=ROOT, stdout=log, stderr=log,
                                      env={**os.environ, 'NETBAIOT_ADMIN_SECRET': 'ab' * 32, 'RUST_LOG': 'error'})
            try:
                for _ in range(100):
                    try:
                        request(management, 'ready')
                        break
                    except (OSError, ValueError):
                        if server.poll() is not None:
                            raise RuntimeError((Path(folder) / 'server.log').read_text())
                        time.sleep(0.05)
                else:
                    raise RuntimeError('server did not become ready')
                # Warm cache/runtime, using the same boot in each probe invocation only.
                subprocess.check_output([str(probe), f'127.0.0.1:{device}', '1', 'throughput'], text=True)
                before = counters(management)
                usage_before = json.loads(request(management, 'status'))
                cpu_before = cpu(server.pid)
                result = json.loads(subprocess.check_output([str(probe), f'127.0.0.1:{device}', str(seconds), mode], text=True))
                cpu_after = cpu(server.pid)
                after = counters(management)
                usage_after = json.loads(request(management, 'status'))
                delta = {key: value - before.get(key, 0) for key, value in after.items()}
                accepted = delta['events_accepted']
                result.update(accepted=accepted, accepted_per_second=accepted / result['seconds'],
                              server_cpu_seconds=cpu_after-cpu_before,
                              cpu_us_per_accepted=(cpu_after-cpu_before)*1e6/accepted if accepted else None,
                              counters=delta, status_before=usage_before, status_after=usage_after)
                return result
            finally:
                server.send_signal(signal.SIGTERM)
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--final', type=Path, required=True)
    parser.add_argument('--probe', type=Path, default=ROOT / 'target/release/udp_ack')
    parser.add_argument('--seconds', type=int, default=8)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = []
    for repeat in range(args.repeats):
        order = ['baseline', 'final'] if repeat % 2 == 0 else ['final', 'baseline']
        for label in order:
            result = run(getattr(args, label).resolve(), args.probe.resolve(), args.seconds, 'throughput')
            result.update(label=label, repeat=repeat)
            rows.append(result)
            print(label, repeat, round(result['accepted_per_second']), result['cpu_us_per_accepted'], flush=True)
    rtt = run(args.final.resolve(), args.probe.resolve(), args.seconds, 'rtt')
    summary = {label: {key: statistics.median(row[key] for row in rows if row['label'] == label)
                       for key in ['accepted_per_second', 'cpu_us_per_accepted']}
               for label in ['baseline', 'final']}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(host=platform.platform(), processor=platform.processor(),
                                          rows=rows, rtt=rtt, median=summary), indent=2) + '\n')
    print(json.dumps(summary), flush=True)


if __name__ == '__main__':
    main()
