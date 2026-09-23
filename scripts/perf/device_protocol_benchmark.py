#!/usr/bin/env python3
"""Owned, serialized device-protocol benchmark; not production network capacity.

Plans are JSON arrays of {name, seconds, groups, repeats?, tls?, sink?, limits?}.
Uses public fixture credentials only. Every child, socket and output has a bound.
"""
import argparse
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import platform
import signal
import socket
import ssl
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
CERT = ROOT / 'tests/fixtures/localhost-cert.pem'
SECRET = bytes(range(32)).hex()
BASELINE = '945fe5e386d623c32e2c7d2d0568fe0c058107ec'


def command(args):
    try:
        return subprocess.check_output(args, text=True, stderr=subprocess.STDOUT, timeout=10).strip()
    except subprocess.CalledProcessError as exc:
        return exc.output.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return str(exc)


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as source:
        for block in iter(lambda: source.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def environment():
    return dict(checkout_revision=command(['git','rev-parse','HEAD']), platform=platform.platform(), machine=platform.machine(), cpu_count=os.cpu_count(),
                sysctl=command(['sysctl', 'hw.model', 'hw.memsize', 'hw.ncpu', 'machdep.cpu.brand_string',
                                'kern.ipc.somaxconn', 'net.inet.tcp.msl', 'net.inet.udp.recvspace', 'net.inet.udp.maxdgram']),
                limits=command(['sh', '-c', 'ulimit -a']), rust=command(['rustc', '+1.88.0', '-Vv']),
                server_tokio_workers=10, loadgen_tokio_workers=4, path='loopback',
                limitations=['Shared server/loadgen host; no separate-host capacity claim.',
                             'Process CPU time; no per-thread scheduler or syscall profiling.',
                             'No per-sink gauge; one required sink, pending_required is its depth.',
                             'TCP/MQTT remote close does not identify the cause on wire; use server counters.'])


def process_usage(pid):
    raw = command(['ps', '-p', str(pid), '-o', 'time=,rss=,%cpu=']).split()
    if len(raw) != 3:
        return None
    seconds = 0
    for part in raw[0].split(':'):
        seconds = seconds * 60 + float(part)
    return dict(cpu_seconds=seconds, rss_kib=int(raw[1]), cpu_pct=float(raw[2]))


class Control:
    """Management HTTP closes each response; sampling can fail during saturation."""
    def __init__(self, port, tls):
        if tls:
            self.http = http.client.HTTPSConnection('localhost', port, timeout=2,
                            context=ssl.create_default_context(cafile=str(CERT)))
        else:
            self.http = http.client.HTTPConnection('localhost', port, timeout=2)

    def get(self, path):
        try:
            self.http.request('GET', '/api/v1/' + path, headers={'Authorization': 'Bearer ' + 'ab' * 32})
            response = self.http.getresponse()
            data = response.read(1_048_577)
            if len(data) > 1_048_576 or response.status != 200:
                raise ValueError(f'management {path} status={response.status} bytes={len(data)}')
            return data.decode()
        finally:
            # The server deliberately closes every response. Reset Request-sent
            # state after TLS/overload errors too, so the next sample can recover.
            self.http.close()

    def sample(self):
        status = json.loads(self.get('status'))
        metrics = {key.removeprefix('netbaiot_').removesuffix('_total'): int(value)
                   for line in self.get('metrics').splitlines() if not line.startswith('#')
                   for key, value in [line.split()] if '{' not in key and value.isdecimal()}
        return dict(status=status, counters=metrics)

    def close(self):
        self.http.close()


def group(protocol, rate, workers=None, **kwargs):
    return dict(label=protocol, protocol=protocol, rate=rate,
                workers=workers or dict(mqtt=32, tcp=32, udp=8)[protocol],
                offset=dict(mqtt=128, tcp=256, udp=384)[protocol], window=128, **kwargs)


def server_config(folder, plan, device, management, sink):
    indices = {g.get('offset', 0) + n for g in plan['groups'] for n in range(g['workers'])}
    limits = dict(max_connections_per_ip=256, max_connections_per_tenant=256,
                  max_devices=4096, max_devices_per_tenant=4096,
                  auth_cache_max_entries=4096, auth_cache_max_bytes=8 * 1024 * 1024,
                  requests_per_second=2_000_000, requests_per_ip_second=2_000_000,
                  messages_per_device_second=2_000_000, messages_per_tenant_second=2_000_000)
    if plan.get('sink'):
        limits.update(sink_queue_max_count=128, global_event_max_count=128,
                      shutdown_drain_timeout_ms=1000, shutdown_timeout_ms=10000)
    limits.update(plan.get('limits', {}))
    return dict(device_ingress=f'127.0.0.1:{device}', management_http=f'127.0.0.1:{management}',
                development=True, business_tcp=None, limits=limits,
                tls=dict(certificate=str(CERT), private_key=str(CERT.with_name('localhost-key.pem'))) if plan.get('tls', True) else None,
                delivery_url=f'http://127.0.0.1:{sink}/events' if plan.get('sink') else None,
                auth_provider_url=None, spool_directory=str(folder / 'spool'),
                credentials=[dict(credential_id=f'a{i}', secret_hex=SECRET,
                                  identity=dict(device_key=dict(tenant_id=f't{i//4096}', product_id='p', device_id=f'd{i}'),
                                                credential_version=1, auth_generation=1, codec_id='netbaiot-json', codec_version=1,
                                                permissions=dict(publish=True, commands=True))) for i in sorted(indices)])


def stop(child, seconds):
    if child is None:
        return None
    if child.poll() is None:
        child.send_signal(signal.SIGTERM)
    try:
        return dict(exit_code=child.wait(timeout=seconds), forced=False)
    except subprocess.TimeoutExpired:
        child.kill()
        return dict(exit_code=child.wait(timeout=5), forced=True)


def run(plan, repeat, args):
    if not 0 < plan['seconds'] <= 3600:
        raise ValueError('duration bound')
    if any(g['protocol'] not in ('mqtt', 'tcp', 'udp') for g in plan['groups']):
        raise ValueError('device protocols are MQTT, TCP and UDP only')
    if plan.get('loadgen_workers', 4) != 4:
        raise ValueError('loadgen runtime fixes four workers; environment cannot override it')
    workers = 4  # Frozen and current loadgen main explicitly fixes the Tokio runtime to four.
    settling = plan.get('settle_before', 0)
    if not isinstance(workers, int) or not 1 <= workers <= 32 or not 0 <= settling <= 60:
        raise ValueError('worker/settling bound')
    name = f'{args.label}-{plan["name"]}-{repeat}'
    if any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in name):
        raise ValueError('run name must be alphanumeric')
    folder = ROOT / 'target/mixed-audit' / name
    folder.mkdir(parents=True, exist_ok=False)
    if settling:
        print(json.dumps(dict(name=name, settling_seconds=settling)), flush=True)
        time.sleep(settling)
    # Reserve dynamic ports; close immediately before spawning owned listener.
    # Device ingress requires both TCP and UDP on the same numeric port.
    # A TCP-only reservation can pick a port already held by an unrelated UDP user.
    for _ in range(100):
        device_reservation = socket.socket()
        udp_reservation = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        device_reservation.bind(('127.0.0.1', 0))
        try:
            udp_reservation.bind(device_reservation.getsockname())
            break
        except OSError:
            device_reservation.close()
            udp_reservation.close()
    else:
        raise RuntimeError('could not reserve paired device TCP/UDP port')
    reservations = [device_reservation, socket.socket(), socket.socket()]
    for sock in reservations[1:]:
        sock.bind(('127.0.0.1', 0))
    device, management, sinkport = [s.getsockname()[1] for s in reservations]
    reservations.append(udp_reservation)
    config = server_config(folder, plan, device, management, sinkport)
    (folder / 'server.json').write_text(json.dumps(config))
    (folder / 'sink-control.json').write_text(json.dumps(dict(delay=plan.get('sink_delay', 2), status=204)))
    env = {**os.environ, 'NETBAIOT_ADMIN_SECRET': 'ab' * 32, 'RUST_LOG': 'error', 'TOKIO_WORKER_THREADS': '10'}
    server = load = sink = control = None
    files = []
    samples = []
    row = dict(name=name, plan=plan, repeat=repeat, label=args.label, timestamp=time.time(),
               baseline=args.baseline, production_revision=plan.get('production_revision', args.baseline), server_sha256=args.server_hash, loadgen_sha256=args.loadgen_hash,
               harness_version=3, loadgen_tokio_workers=workers, loadgen_worker_source='main tokio macro (environment ignored)', server_tokio_workers=10, harness_sha256=digest(Path(__file__)), server_config=config, network_before=command(['netstat', '-s', '-p', 'udp']),
               tcp_before=command(['netstat', '-s', '-p', 'tcp']))
    try:
        for s in reservations:
            s.close()
        if plan.get('sink'):
            log = open(folder / 'sink.log', 'w'); files.append(log)
            sink = subprocess.Popen([sys.executable, str(ROOT / 'scripts/perf/sink.py'), str(sinkport), str(folder / 'sink-control.json')], stdout=log, stderr=log)
            time.sleep(.3)
        log = open(folder / 'server.log', 'w'); files.append(log)
        server = subprocess.Popen([str(args.server), str(folder / 'server.json')], cwd=ROOT, env=env, stdout=log, stderr=log)
        for _ in range(100):
            try:
                control = Control(management, plan.get('tls', True))
                control.get('ready')
                break
            except (OSError, ValueError, http.client.HTTPException):
                control.close()
                if server.poll() is not None:
                    raise RuntimeError((folder / 'server.log').read_text())
                time.sleep(.05)
        else:
            raise RuntimeError('readiness timeout')
        row['idle'] = control.sample()
        row['idle_process'] = process_usage(server.pid)
        row['idle_fds'] = command(['lsof', '-a', '-p', str(server.pid), '-F', 'f'])
        start = time.time() + plan.get('warmup', 5) + plan.get('ramp', 1) + 2
        workload = dict(address=f'127.0.0.1:{device}', tls_ca=str(CERT) if plan.get('tls', True) else None,
                        start_ms=round(start * 1000), duration_secs=plan['seconds'], warmup_secs=plan.get('warmup', 5),
                        ramp_secs=plan.get('ramp', 1), timeout_ms=plan.get('timeout_ms', 1000), payload_bytes=256,
                        groups=plan['groups'])
        (folder / 'loadgen.json').write_text(json.dumps(workload))
        row['workload'] = workload
        log = open(folder / 'loadgen.jsonl', 'w'); files.append(log)
        load = subprocess.Popen([str(args.loadgen), '--mixed', str(folder / 'loadgen.json')], cwd=ROOT,
                                env={**env, 'TOKIO_WORKER_THREADS': str(workers)}, stdout=log, stderr=log)
        next_sample = time.time()
        restored = False
        shutdown = False
        while load.poll() is None:
            now = time.time()
            if now > start + plan['seconds'] + 15:
                raise RuntimeError('load generator exceeded bounded deadline')
            if 'host_cpu_sample' not in row and now >= start + plan['seconds'] * .5:
                row['host_cpu_sample'] = command(['top', '-l', '2', '-s', '1', '-n', '0'])
                now = time.time()
            if plan.get('sink') and not restored and now >= start + plan.get('sink_restore_at', plan['seconds'] * .5):
                (folder / 'sink-control.json').write_text(json.dumps(dict(delay=0, status=204)))
                row['sink_restored_at'] = now - start
                restored = True
            if plan.get('shutdown_at') and not shutdown and now >= start + plan['shutdown_at']:
                row['shutdown_at'] = now - start
                server.send_signal(signal.SIGTERM)
                shutdown = True
            sample = dict(t=now-start, server=process_usage(server.pid), loadgen=process_usage(load.pid))
            try:
                sample.update(control.sample())
            except (OSError, ValueError, http.client.HTTPException) as exc:
                sample['management_error'] = type(exc).__name__ + ': ' + str(exc)
            samples.append(sample)
            next_sample += 1
            time.sleep(max(0, next_sample-time.time()))
        row['loadgen_exit'] = load.wait()
        values = []
        for line in (folder / 'loadgen.jsonl').read_text().splitlines():
            try:
                values.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        finals = [v for v in values if v.get('event') == 'final']
        if row['loadgen_exit'] != 0 or not finals:
            raise RuntimeError('loadgen failed: ' + (folder / 'loadgen.jsonl').read_text()[-2000:])
        starts = [v for v in values if v.get('event') == 'start']
        row['loadgen_schema_version'] = starts[0].get('schema_version', 1) if starts else 1
        row['result'] = finals[-1]
        row['client_samples'] = [v for v in values if v.get('event') == 'sample' and v['measurement_secs'] > 0]
        time.sleep(1)
        try:
            row['cooldown'] = control.sample()
            row['cooldown_process'] = process_usage(server.pid)
            row['cooldown_fds'] = command(['lsof', '-a', '-p', str(server.pid), '-F', 'f'])
        except (OSError, ValueError, http.client.HTTPException) as exc:
            row['cooldown_error'] = str(exc)
        row['network_after'] = command(['netstat', '-s', '-p', 'udp'])
        row['tcp_after'] = command(['netstat', '-s', '-p', 'tcp'])
    finally:
        row['load_stop'] = stop(load, 5)
        row['server_stop'] = stop(server, 40)
        row['sink_stop'] = stop(sink, 5)
        if control:
            control.close()
        for f in files:
            f.close()
        row['samples'] = samples
        row['spool_files'] = [{ 'name': str(p.relative_to(folder)), 'bytes': p.stat().st_size}
                              for p in folder.rglob('*') if p.is_file() and 'spool' in p.parts]
        row['server_log_tail'] = (folder / 'server.log').read_text()[-4000:] if (folder / 'server.log').exists() else ''
        row['sink_log'] = (folder / 'sink.log').read_text()[-4000:] if (folder / 'sink.log').exists() else ''
        args.output.mkdir(parents=True, exist_ok=True)
        (args.output / (name + '.json')).write_text(json.dumps(row, separators=(',', ':')) + '\n')
    measured = [s for s in samples if 0 <= s['t'] < plan['seconds'] and s['server']]
    cpu = None
    if len(measured) > 1:
        cpu = (measured[-1]['server']['cpu_seconds'] - measured[0]['server']['cpu_seconds']) / (measured[-1]['t']-measured[0]['t']) * 100
    summary = {label: dict(rate=round(v['counters'].get('accepted', 0) / plan['seconds'], 1),
                          success=round(v['counters'].get('accepted', 0) / max(1, v['counters'].get('attempted', 0)) * 100, 2),
                          p99=v['latencies'].get('acceptance', {}).get('p99_ms'), counters=v['counters'])
               for label, v in row['result']['groups'].items()}
    print(json.dumps(dict(name=name, cpu=cpu, groups=summary)), flush=True)
    if row['server_stop']['forced'] or row['server_stop']['exit_code'] != 0:
        raise RuntimeError('server did not exit gracefully')
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--plan', type=Path, required=True)
    parser.add_argument('--server', type=Path, default=ROOT / 'target/remove-device-http/before-server')
    parser.add_argument('--candidate', type=Path, help='Optional candidate server for interleaved paired plans')
    parser.add_argument('--loadgen', type=Path, default=ROOT / 'target/release/netbaiot-loadgen')
    parser.add_argument('--label', default='baseline')
    parser.add_argument('--baseline', default=BASELINE, help='Source revision of the frozen baseline server')
    parser.add_argument('--resume', action='store_true', help='Skip only matching, completed successful raw records')
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/performance/remove-device-http')
    args = parser.parse_args()
    args.server = args.server.resolve(); args.loadgen = args.loadgen.resolve()
    args.server_hash = digest(args.server); args.loadgen_hash = digest(args.loadgen)
    args.output.mkdir(parents=True, exist_ok=True)
    (ROOT / 'target/mixed-audit').mkdir(parents=True, exist_ok=True)
    with open(ROOT / 'target/mixed-audit/audit.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        (args.output / (args.label + '-environment.json')).write_text(json.dumps(environment(), indent=2) + '\n')
        plans = json.loads(args.plan.read_text())
        if len(plans) > 100:
            raise ValueError('plan count bound')
        for plan in plans:
            selected = argparse.Namespace(**vars(args))
            if plan.get('binary', plan.get('variant')) == 'candidate':
                if args.candidate is None:
                    raise ValueError('candidate plan requires --candidate')
                selected.server = args.candidate.resolve()
                selected.server_hash = digest(selected.server)
            selected.label = plan.get('variant', args.label)
            for repeat in range(plan.get('repeats', 1)):
                if (ROOT / 'target/mixed-audit/stop-after-run').exists():
                    print('Stopped at completed-run boundary.', flush=True)
                    return
                previous = args.output / f'{selected.label}-{plan["name"]}-{repeat}.json'
                if args.resume and previous.exists():
                    old = json.loads(previous.read_text())
                    if (old.get('plan') != plan or old.get('server_sha256') != selected.server_hash
                            or old.get('loadgen_sha256') != selected.loadgen_hash
                            or old.get('loadgen_exit') != 0 or 'result' not in old
                            or old.get('server_stop') != {'exit_code': 0, 'forced': False}):
                        raise ValueError(f'Cannot resume mismatched or unsuccessful result: {previous}')
                    print(json.dumps(dict(resumed_completed=old['name'])), flush=True)
                    continue
                run(plan, repeat, selected)


if __name__ == '__main__':
    main()
