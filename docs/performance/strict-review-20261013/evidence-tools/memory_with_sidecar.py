#!/usr/bin/env python3
"""Invoke the unchanged repository density harness; record separate macOS counters."""
import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

parser = argparse.ArgumentParser()
parser.add_argument('--repo', type=Path, required=True)
parser.add_argument('--sidecar', type=Path, required=True)
args, harness_args = parser.parse_known_args()
if harness_args and harness_args[0] == '--':
    harness_args = harness_args[1:]
if args.sidecar.exists():
    raise SystemExit('sidecar already exists; preserve previous evidence')
sys.path.insert(0, str(args.repo / 'scripts/perf'))
import connection_memory as harness
samples = []
statuses = []
original_rss = harness.rss_kib
original_status = harness.status

def cpu_seconds(value):
    value = value.strip()
    days = 0
    if '-' in value:
        prefix, value = value.split('-', 1)
        days = int(prefix)
    seconds = 0.0
    for component in value.split(':'):
        seconds = seconds * 60 + float(component)
    return days * 86400 + seconds

def rss_with_cpu(pid):
    rss = original_rss(pid)
    record = {'pid': pid, 'monotonic_seconds': time.monotonic(), 'rss_kib': rss}
    try:
        record['process_cpu_seconds'] = cpu_seconds(subprocess.check_output(
            ['ps', '-o', 'time=', '-p', str(pid)], text=True, timeout=5))
        record['host_swapusage'] = subprocess.check_output(
            ['sysctl', '-n', 'vm.swapusage'], text=True, timeout=5).strip()
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        record['supplemental_measurement_error'] = str(error)
    samples.append(record)
    return rss

def record_status(port, tls):
    result = original_status(port, tls)
    statuses.append({'monotonic_seconds': time.monotonic(), 'status': result})
    return result

def interrupted(signum, frame):
    raise SystemExit(128 + signum)

signal.signal(signal.SIGTERM, interrupted)
harness.rss_kib = rss_with_cpu
harness.status = record_status
sys.argv = [str(args.repo / 'scripts/perf/connection_memory.py'), *harness_args]
try:
    harness.main()
finally:
    result = {'measurement': 'separate macOS ps cumulative user/system process CPU; host-wide swapusage',
              'notes': ['Original harness JSON and Linux-only fields remain unchanged.',
                        'Host swapusage includes other processes and is not process swap.',
                        'Each supplemental snapshot follows the unchanged harness RSS snapshot.',
                        'No task/Future/Channel allocation or connection byte-budget balance is exported by this wrapper.'],
              'samples': samples, 'status_snapshots': statuses}
    if len(samples) >= 2:
        def fraction(a, b):
            if 'process_cpu_seconds' not in a or 'process_cpu_seconds' not in b:
                return None
            elapsed = b['monotonic_seconds'] - a['monotonic_seconds']
            return (100 * (b['process_cpu_seconds'] - a['process_cpu_seconds']) / elapsed) if elapsed > 0 else None
        result['process_cpu_percent_ramp'] = fraction(samples[0], samples[1])
        result['process_cpu_percent_stable'] = fraction(samples[1], samples[-1])
    args.sidecar.write_text(json.dumps(result, indent=2) + '\n')
