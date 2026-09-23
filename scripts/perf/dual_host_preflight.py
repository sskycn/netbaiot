#!/usr/bin/env python3
"""Reproduce dual-host plaintext configuration blockers without changing policy.

Uses fixed test ports 24000..24004, one owned child at a time, no published events,
and a temporary recovery directory. This is a startup check, not a capacity test.
"""
import argparse
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[2]


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def check(server, config, directory, name, expect_ready):
    path = directory / (name + '.json')
    path.write_text(json.dumps(config, indent=2) + '\n')
    path.chmod(0o600)
    log = directory / (name + '.log')
    ready = False
    with log.open('w') as output:
        child = subprocess.Popen([str(server), str(path)], cwd=ROOT,
                                 env=dict(os.environ, RUST_LOG='info'),
                                 stdout=output, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 10
            while child.poll() is None and time.monotonic() < deadline:
                if log.stat().st_size > 1_048_576:
                    raise RuntimeError('startup log exceeded 1 MiB')
                if 'runtime ready' in log.read_text():
                    ready = True
                    break
                time.sleep(0.05)
        finally:
            if child.poll() is None:
                child.terminate()
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                # No ingress workload is sent by this startup-only probe.
                child.kill()
                child.wait()
                raise RuntimeError('startup-only child did not stop')
    text = log.read_text()
    passed = (ready and child.returncode == 0) if expect_ready else (
        not ready and child.returncode != 0 and 'Error: Configuration' in text)
    return {'name': name, 'expected': 'ready' if expect_ready else 'configuration rejection',
            'passed': passed, 'ready': ready, 'exit_code': child.returncode,
            'config': copy.deepcopy(config), 'log': text}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, default=ROOT / 'target/release/netbaiot-server')
    parser.add_argument('--output', type=Path,
                        default=ROOT / 'target/perf-audit/dual-host/preflight.json')
    args = parser.parse_args()
    server = args.server.resolve()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    config = json.loads((ROOT / 'configs/development.json').read_text())
    for offset, key in enumerate(('device_ingress', 'management_http')):
        config[key] = '127.0.0.1:%d' % (24000 + offset)
    rows = []
    # Serialize this tool's fixed-port control; never reserve then hand off a port.
    with (args.output.parent / 'preflight.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with tempfile.TemporaryDirectory(prefix='netbaiot-dual-preflight-') as tmp:
            directory = Path(tmp)
            config['spool_directory'] = str(directory / 'spool')
            rows.append(check(server, config, directory, 'loopback-development-control', True))
            remote = copy.deepcopy(config)
            remote['device_ingress'] = '0.0.0.0:24002'
            rows.append(check(server, remote, directory, 'remote-development-plaintext', False))
            remote['development'] = False
            # A configured required sink isolates the independent TLS guard. It is
            # never contacted: validation precedes listener and sink construction.
            remote['delivery_url'] = 'http://127.0.0.1:24006/events'
            rows.append(check(server, remote, directory, 'remote-production-plaintext-ipv4', False))
            remote['device_ingress'] = '[::]:24002'
            rows.append(check(server, remote, directory, 'remote-production-plaintext-ipv6', False))
            config['development'] = False
            rows.append(check(server, config, directory, 'production-without-required-sink', False))
    result = {'kind': 'configuration-preflight-not-capacity',
              'source_sha': command('git', 'rev-parse', 'HEAD'),
              'source_status': command('git', 'status', '--short'),
              'server_binary': str(server),
              'server_sha256': hashlib.sha256(server.read_bytes()).hexdigest(),
              'hostname': platform.node(), 'platform': platform.platform(),
              'rustc': command('rustc', '--version', '--verbose'),
              'cargo': command('cargo', '--version', '--verbose'),
              'rustflags': os.environ.get('RUSTFLAGS', ''),
              'cases': rows, 'all_passed': all(row['passed'] for row in rows)}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'output': str(args.output), 'all_passed': result['all_passed'],
                      'cases': [{k: row[k] for k in ('name', 'passed', 'exit_code')} for row in rows]}))
    return 0 if result['all_passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
