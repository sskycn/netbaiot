#!/usr/bin/env python3
"""Own four codec/transport pairs at both real 6h and 24h durations.

One controller process owns eight finite supervisors. Each supervisor owns its
gateway/load/sink processes. Use an unused output path; preserve all failed runs.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
PAIRS = (('json', 'mqtt311'), ('cbor', 'mqtt5'), ('msgpack', 'tcp'), ('protobuf', 'udp'))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--devices', type=int, default=1000)
    args = p.parse_args()
    if not 1 <= args.devices <= 10000:
        p.error('bounded devices must be 1..10000')
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    frozen = out / 'bin'; frozen.mkdir()
    hashes = {}
    for name in ('netbaiot-server', 'netbaiot-loadgen'):
        shutil.copy2(ROOT / 'target/release' / name, frozen / name)
        hashes[name] = hashlib.sha256((frozen / name).read_bytes()).hexdigest()
    manifest = dict(status='running', controller=os.getpid(), started_epoch=time.time(),
        placement='eight concurrent same-host test gateways, not one 8000-device gateway',
        devices_per_gateway=args.devices, binary_sha256=hashes,
        revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), runs=[])
    children = []
    logs = []

    def save():
        (out / 'campaign.json').write_text(json.dumps(manifest, indent=2) + '\n')

    def interrupted(_signal, _frame):
        raise KeyboardInterrupt('soak campaign cancelled')

    signal.signal(signal.SIGTERM, interrupted)
    save()
    try:
        for hours in (6, 24):
            for codec, transport in PAIRS:
                label = f'{hours}h-{codec}-{transport}'
                destination = out / label
                command = [sys.executable, str(ROOT / 'scripts/perf/production_readiness.py'),
                    '--devices', str(args.devices), '--codec', codec, '--transport', transport,
                    '--scenario', 'telemetry64' if hours == 6 else 'sensor', '--duration', str(hours * 3600),
                    '--sample-interval', '60', '--threads', '2', '--tls', '--output', str(destination),
                    '--server', str(frozen / 'netbaiot-server'), '--loadgen', str(frozen / 'netbaiot-loadgen')]
                log = (out / f'{label}.log').open('w'); logs.append(log)
                child = subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=log)
                children.append(child)
                manifest['runs'].append(dict(label=label, pid=child.pid, hours=hours, codec=codec,
                    transport=transport, output=str(destination), command=command, status='running'))
                save()
                # Serialize bind/startup so probes do not compete for ephemeral reservations.
                for _ in range(100):
                    if list(destination.glob('*/pid.json')): break
                    if child.poll() is not None: raise RuntimeError(f'{label} failed at startup')
                    time.sleep(.1)
                else: raise TimeoutError(f'{label} did not establish an owned gateway')
                time.sleep(.2)
        while any(c.poll() is None for c in children):
            for child, row in zip(children, manifest['runs']):
                if child.poll() is not None:
                    row.update(status='completed' if child.returncode == 0 else 'failed', exit_code=child.returncode)
            save()
            time.sleep(10)
        manifest['status'] = 'completed' if all(c.returncode == 0 for c in children) else 'failed'
    except BaseException as error:
        manifest.update(status='cancelled_or_failed', error=f'{type(error).__name__}: {error}')
        raise
    finally:
        for child in children:
            if child.poll() is None: child.terminate()
        for child in children:
            if child.poll() is None:
                try: child.wait(timeout=30)
                except subprocess.TimeoutExpired: child.kill(); child.wait(timeout=5)
        for child, row in zip(children, manifest['runs']):
            row['exit_code'] = child.returncode
        manifest['ended_epoch'] = time.time()
        save()
        for log in logs: log.close()


if __name__ == '__main__':
    main()
