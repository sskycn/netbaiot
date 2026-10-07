#!/usr/bin/env python3
"""Serial cold-target build, startup, and RPC profile comparison; default unchanged."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace

from connection_memory import credential, free_ports, rss_kib, status

ROOT = Path(__file__).resolve().parents[2]


def binaries():
    return {label: OUT / ('cold-' + label) / label / 'netbaiot-server'
            for label in ('release', 'production')}


def build():
    rows = []
    for label in ('release', 'production'):
        target = OUT / ('cold-' + label)
        if target.exists():
            raise RuntimeError('cold target already exists; choose a fresh --output')
        command = ['cargo']
        if label == 'production':
            command += ['--config', 'profile.production.inherits="release"',
                        '--config', 'profile.production.lto="thin"',
                        '--config', 'profile.production.codegen-units=1']
        command += ['build', '--locked', '--offline', '--profile', label,
                    '--target-dir', str(target), '-p', 'netbaiot-server',
                    '--bin', 'netbaiot-server']
        started = time.perf_counter()
        with (OUT / (label + '-build.log')).open('w') as log:
            subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                           check=True)
        binary = binaries()[label]
        rows.append({'label': label, 'seconds': time.perf_counter() - started,
                     'bytes': binary.stat().st_size,
                     'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                     'command': command,
                     'source_sha': subprocess.check_output(
                         ['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()})
        (OUT / 'builds.json').write_text(json.dumps(rows, indent=2))
        print(rows[-1], flush=True)


def startup():
    rows = []
    for index, label in enumerate('ABBAABBAAB'):
        binary = binaries()['release' if label == 'A' else 'production']
        device, admin = free_ports(2)
        with tempfile.TemporaryDirectory() as temporary:
            config = {'development': True, 'device_ingress': f'127.0.0.1:{device}',
                      'management_http': f'127.0.0.1:{admin}', 'business_tcp': None,
                      'credentials': [credential(0)], 'tls': None,
                      'delivery_url': None, 'auth_provider_url': None,
                      'spool_directory': temporary + '/spool'}
            config_path = Path(temporary) / 'config.json'
            config_path.write_text(json.dumps(config))
            environment = os.environ.copy()
            environment['NETBAIOT_ADMIN_SECRET'] = 'ab' * 32
            environment['NETBAIOT_PERF_LOCK_METRICS'] = '0'
            with (OUT / f'startup-{index}-{label}.log').open('w') as log:
                started = time.perf_counter()
                server = subprocess.Popen([str(binary), str(config_path)], cwd=ROOT,
                                          env=environment, stdout=log,
                                          stderr=subprocess.STDOUT)
                try:
                    deadline = started + 10
                    while True:
                        if server.poll() is not None:
                            raise RuntimeError('server exited before ready')
                        try:
                            status(admin, False)
                            break
                        except OSError:
                            if time.perf_counter() > deadline:
                                raise
                            time.sleep(.001)
                    elapsed = time.perf_counter() - started
                    rows.append({'label': label, 'ready_seconds': elapsed,
                                 'rss_kib': rss_kib(server.pid)})
                finally:
                    if server.poll() is None:
                        server.send_signal(signal.SIGTERM)
                        server.wait(timeout=30)
                    if server.returncode:
                        raise RuntimeError('startup shutdown failed')
        (OUT / 'startup.json').write_text(json.dumps(rows, indent=2))
    print(rows)


def rpc():
    spec = importlib.util.spec_from_file_location(
        'hol', ROOT / 'tools/netbaiot-loadgen/run_business_rpc_v3_hol.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.LOADGEN = ROOT / 'target/second-round/baseline/bin/business_rpc'
    for index, label in enumerate('ABBA'):
        module.GATEWAY = binaries()['release' if label == 'A' else 'production']
        output = OUT / f'rpc-steady-{index}-{label}'
        output.mkdir(exist_ok=True)
        args = SimpleNamespace(
            capacity_fixture=True, duration_secs=15, event_payload_bytes=1024,
            event_rate=500, auth_concurrency=4, auth_unique_devices=False,
            stream_window_bytes=None, disconnect_after_secs=None,
            send_ahead_stream_bytes=None, send_ahead_connection_bytes=None,
            socket_send_buffer_bytes=None, no_proxy=True, trace_gateway=False,
            trace_socket=False, warmup_secs=3, recovery_secs=2,
            bytes_per_second=32768, delay_ms=0, output=output)
        module.run_mode('v3-8192', args)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('build', 'startup', 'rpc'))
    parser.add_argument('--output', type=Path, default=ROOT / 'target/second-round/profile')
    arguments = parser.parse_args()
    OUT = arguments.output.resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    {'build': build, 'startup': startup, 'rpc': rpc}[arguments.mode]()
