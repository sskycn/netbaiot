#!/usr/bin/env python3
"""Run owned real gateway/load processes; retain bounded JSON reports and minute samples.

Test credentials only. Same-host results are qualified by placement and workload;
an admitted limit or a short run is never a production capacity recommendation.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import signal
import ssl
import statistics
import subprocess
import time
import urllib.request

from codec_campaign import ADMIN, KEY, ROOT, usage
from connection_memory import free_ports

CODECS = ('json', 'cbor', 'msgpack', 'protobuf')
TRANSPORTS = ('mqtt311', 'mqtt5', 'tcp', 'udp')
SCENARIOS = ('sensor', 'telemetry16', 'telemetry64')
FAULTS = ('none', 'wrong_hmac', 'wrong_version', 'wrong_codec_version', 'oversize', 'repeat_sequence', 'reconnect', 'half_connection')

CONTEXT = ssl.create_default_context(cafile=str(ROOT / 'tests/fixtures/localhost-cert.pem'))
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=CONTEXT))


def management(address, path):
    req = urllib.request.Request(f'{address}/api/v1/{path}', headers={'Authorization': 'Bearer ' + ADMIN})
    with OPENER.open(req, timeout=5) as response:
        data = response.read(1048577)
        if len(data) > 1048576: raise ValueError('management response limit')
        return data


def write(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def metrics(address):
    text = management(address, 'metrics').decode()
    result = {}
    for line in text.splitlines():
        if not line.startswith('#') and ' ' in line:
            key, value = line.rsplit(' ', 1)
            result[key] = float(value)
    return result


def histograms(before, after):
    """Prometheus bucket upper bounds, not invented exact percentiles."""
    result = {}
    for key, value in after.items():
        if not key.endswith('_us_count'):
            continue
        count = value - before.get(key, 0)
        name = key[:-6]
        buckets = []
        for bucket, total in after.items():
            match = re.fullmatch(re.escape(name) + r'_bucket\{le="([^"]+)"\}', bucket)
            if match:
                bound = float(match[1])
                buckets.append((bound, total - before.get(bucket, 0)))
        buckets.sort()
        quantiles = {}
        for percent in (50, 95, 99):
            bound = next((bound for bound, n in buckets if n >= count * percent / 100), None) if count else None
            quantiles[f'p{percent}_upper_ms'] = bound / 1000 if bound is not None and bound != float('inf') else None
        result[name.removeprefix('netbaiot_')] = dict(count=int(count), **quantiles)
    return result


def fd_count(pid):
    if platform.system() == 'Linux':
        return len(list(Path(f'/proc/{pid}/fd').iterdir()))
    text = subprocess.check_output(['lsof', '-n', '-a', '-p', str(pid), '-F', 'f'], text=True)
    return sum(bool(re.fullmatch(r'f\d+', line)) for line in text.splitlines())


def credential(i, codec, count):
    return dict(credential_id=f'a{i}', secret_hex=KEY.hex(), identity=dict(
        device_key=dict(tenant_id=f't{i // (count + 1)}', product_id='p', device_id=f'd{i}'),
        credential_version=1, auth_generation=1, codec_id=f'netbaiot-{codec}', codec_version=1,
        permissions=dict(publish=True, commands=True)))


def configs(args, out, codec, transport, scenario, devices, fault):
    device_port, admin_port, sink_port = free_ports(3)
    maximum = max(256, devices + 32)
    # Existing validated test profile; packet/frame hard limits and wire rules stay unchanged.
    reservation = 65536 if maximum * 524288 > 2**32 - 1 else 524288
    limits = dict(max_connections=maximum, max_connections_per_ip=maximum,
        max_device_connections_per_protocol=maximum, max_connections_per_tenant=maximum,
        max_devices=maximum, max_devices_per_tenant=maximum,
        max_persistent_sessions=maximum, max_persistent_sessions_per_tenant=maximum,
        max_subscriptions=maximum * 2, max_subscriptions_per_tenant=maximum * 2,
        connection_memory_reservation=reservation, global_connection_logical_bytes=maximum * reservation,
        auth_cache_max_entries=maximum, auth_cache_max_bytes=64 * 1024 * 1024,
        rate_entries=maximum, requests_per_second=100000, requests_per_ip_second=100000,
        messages_per_device_second=100000, messages_per_tenant_second=100000,
        max_replay_entries=maximum * 2, max_replay_entries_per_tenant=maximum * 2,
        max_replay_entries_per_device=2, sink_delivery_concurrency=16,
        mqtt_recovery_max_bytes=512 * 1024 * 1024)
    server = dict(development=True, device_ingress=f'127.0.0.1:{device_port}',
        management_http=f'127.0.0.1:{admin_port}', spool_directory=str(out / 'spool'),
        delivery_url=f'http://127.0.0.1:{sink_port}/events', limits=limits,
        credentials=[credential(i, codec, devices) for i in range(devices)])
    if fault == 'wrong_codec_version':
        for entry in server['credentials']: entry['identity']['codec_version'] = 2
    if args.tls and transport != 'udp':
        server['tls'] = dict(certificate=str(ROOT / 'tests/fixtures/localhost-cert.pem'),
                             private_key=str(ROOT / 'tests/fixtures/localhost-key.pem'))
    rate = args.rate if args.rate is not None else devices * (0.1 if scenario == 'sensor' else 1)
    load = dict(codec=f'netbaiot-{codec}', scenario=scenario, fault=fault,
        business_sink=f'127.0.0.1:{sink_port}', transport='mqtt' if transport.startswith('mqtt') else transport,
        mqtt_version=5 if transport == 'mqtt5' else 4, qos=1, subscribe=False,
        address=server['device_ingress'], connections=devices, tenant_width=devices + 1,
        ramp_per_sec=args.ramp, warmup_secs=2, duration_secs=args.duration, cooldown_secs=2,
        publish_rate=rate, window=4, timeout_secs=1, report_every_secs=min(args.sample_interval, 60),
        retry_connections=fault in ('wrong_version', 'wrong_codec_version', 'oversize', 'reconnect', 'half_connection'),
        reconnect_every_secs=1 if fault == 'reconnect' else 0)
    if args.tls and transport != 'udp':
        load['tls_ca'] = server['tls']['certificate']
    return server, load


def final_load(path):
    final = None
    with path.open() as stream:
        for line in stream:
            if len(line) > 65536:
                raise ValueError('load report line limit')
            row = json.loads(line)
            if row.get('event') == 'final':
                final = row
    if final is None:
        raise ValueError('load final report missing')
    return final


def growth(samples):
    # Warmup/ramp growth is expected. A trend is a suspect, not proof of a leak.
    steady = [s for s in samples if s['measurement_s'] >= 60]
    if len(steady) < 6:
        return dict(status='insufficient_observation', suspects=[])
    width = max(3, len(steady) // 4)
    first, last = steady[:width], steady[-width:]
    suspects = []
    deltas = {}
    for key in ('rss_kib', 'runtime_tasks', 'active_sessions', 'event_count', 'sink_pending'):
        a = statistics.median(s[key] for s in first)
        b = statistics.median(s[key] for s in last)
        deltas[key] = b - a
        threshold = max(8192, a * .2) if key == 'rss_kib' else max(8, a * .1)
        if b - a > threshold:
            suspects.append(key)
    return dict(status='growth_suspected' if suspects else 'no_threshold_growth_observed',
                suspects=suspects, median_deltas=deltas, observation_seconds=steady[-1]['measurement_s'] - steady[0]['measurement_s'])


def run_case(args, out, codec, transport, scenario, devices, fault):
    out.mkdir(parents=True, exist_ok=False)
    server_config, load_config = configs(args, out.resolve(), codec, transport, scenario, devices, fault)
    write(out / 'server.json', server_config); write(out / 'load.json', load_config)
    environment = dict(platform=platform.platform(), machine=platform.machine(), cpu_count=os.cpu_count(),
        revision=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True)),
        server_sha256=hashlib.sha256(args.server.read_bytes()).hexdigest(),
        loadgen_sha256=hashlib.sha256(args.loadgen.read_bytes()).hexdigest(),
        placement='same_host_loopback', tls=args.tls and transport != 'udp',
        fd_limits=list(resource.getrlimit(resource.RLIMIT_NOFILE)),
        cpu_unit='percent_of_one_core', rss_unit='KiB', sample_interval_s=args.sample_interval,
        latency='client monotonic send to validated transport receipt; send to business 204 ACK emitted; server stage histograms use upper bounds',
        limitations=['shared gateway/generator/sink CPU and loopback; no NIC or WAN test',
            'RSS sampled, not peak allocator use', 'business ACK is test application acceptance, not durable storage',
            'closed finite client window; missed schedule slots counted', 'histogram buckets cannot be added to derive end-to-end percentiles'])
    write(out / 'environment.json', environment)
    address = ('https://' if args.tls and transport != 'udp' else 'http://') + server_config['management_http']
    server = load = None
    samples = []
    started = time.monotonic()
    summary = dict(status='failed', codec=codec, transport=transport, scenario=scenario, devices=devices, fault=fault)
    try:
        with (out / 'server.log').open('w') as server_log, (out / 'load.jsonl').open('w') as load_log, (out / 'resources.jsonl').open('w') as resources:
            command = [str(args.server.resolve()), str((out / 'server.json').resolve())]
            if args.heaptrack:
                command = ['heaptrack', '-o', str((out / 'heaptrack').resolve()), *command]
            server = subprocess.Popen(command, cwd=ROOT, stdout=server_log, stderr=server_log,
                env={**os.environ, 'NETBAIOT_ADMIN_SECRET': ADMIN, 'RUST_LOG': 'error', 'TOKIO_WORKER_THREADS': str(args.threads)})
            for _ in range(200):
                try:
                    management(address, 'ready'); break
                except OSError:
                    if server.poll() is not None:
                        if fault == 'wrong_codec_version' and server.returncode != 0:
                            # Static startup catalog validates the immutable codec profile before listening.
                            summary.update(status='fault_observed', startup_rejected=True, server_exit_code=server.returncode,
                                observation='unsupported static codec version rejected before listener startup; no network load claimed')
                            print(json.dumps(summary), flush=True)
                            return summary
                        raise RuntimeError('server startup failed; inspect server.log')
                    time.sleep(.05)
            else: raise TimeoutError('server readiness')
            initial_status = json.loads(management(address, 'status'))
            pid = initial_status['process_id']
            write(out / 'pid.json', dict(supervisor=os.getpid(), server=pid))
            before = metrics(address)
            load = subprocess.Popen([str(args.loadgen.resolve()), str((out / 'load.json').resolve())],
                cwd=ROOT, stdout=load_log, stderr=load_log, env={**os.environ, 'TOKIO_WORKER_THREADS': str(args.threads)})
            write(out / 'pid.json', dict(supervisor=os.getpid(), server=pid, loadgen=load.pid))
            last_time = time.monotonic(); last_cpu = usage(pid)['cpu_seconds']
            deadline = last_time + devices / args.ramp + args.duration + 30
            while load.poll() is None:
                now = time.monotonic()
                if now > deadline: raise TimeoutError('load absolute deadline')
                state = json.loads(management(address, 'status'))
                process_usage = usage(pid); cpu, rss = process_usage['cpu_seconds'], process_usage['rss_kib']
                row = dict(epoch_ms=int(time.time() * 1000), elapsed_s=now - started,
                    measurement_s=max(0, now - started - devices / args.ramp - 2),
                    cpu_pct=(cpu - last_cpu) / max(.001, now - last_time) * 100,
                    rss_kib=rss, fd_count=fd_count(pid),
                    runtime_tasks=state['runtime_tasks'], connections=sum(state['active_connections'].values()),
                    active_sessions=state['active_sessions'], event_count=state['event_count'],
                    event_bytes=state['event_bytes'], pending_required=state['pending_required'],
                    sink_pending=sum(s['queue_count'] for s in state['sink_diagnostics']),
                    sink_bytes=sum(s['queue_bytes'] for s in state['sink_diagnostics']))
                resources.write(json.dumps(row) + '\n'); resources.flush()
                samples.append(row)  # <=86401 compact samples, bounded by duration/interval validation.
                last_time, last_cpu = now, cpu
                for _ in range(max(1, int(args.sample_interval * 10))):
                    if load.poll() is not None: break
                    time.sleep(.1)
            if load.returncode != 0: raise RuntimeError('load process failed; inspect load.jsonl')
            final = final_load(out / 'load.jsonl')
            state = json.loads(management(address, 'status'))
            after = metrics(address)
            write(out / 'final-status.json', state)
            write(out / 'metrics-before.json', before); write(out / 'metrics-after.json', after)
            delta = {key: value - before.get(key, 0) for key, value in after.items() if '{' not in key and not key.endswith(('_count', '_sum'))}
            counters = final['stats']['counters']
            acked = int(delta.get('netbaiot_sink_acks_total', 0))
            accepted = int(delta.get('netbaiot_events_accepted_total', 0))
            healthy = (fault == 'none' and accepted == acked == counters.get('published', 0)
                and (transport == 'udp' or counters.get('connected', 0) == devices)
                and not state['pending_required'] and not state['active_sessions']
                and not any(counters.get(k, 0) for k in ('client_errors', 'forced_tasks', 'panicked_tasks',
                    'schedule_missed', 'client_window_full', 'business_ack_timeouts', 'sink_probe_overloads', 'sink_owner_errors')))
            fault_ok = (not state['active_sessions'] and not state['pending_required'] and not state['event_count']
                and not any(counters.get(k, 0) for k in ('forced_tasks', 'panicked_tasks', 'sink_owner_errors'))
                and (accepted == 0 if fault in ('wrong_hmac', 'wrong_version', 'oversize', 'half_connection') else accepted == acked))
            measured = [s for s in samples if 0 < s['measurement_s'] < args.duration] or samples
            summary.update(status='pass' if healthy else 'fault_observed' if fault != 'none' and fault_ok else 'failed',
                measured_duration_s=args.duration, wall_seconds=time.monotonic() - started,
                connected=counters.get('connected', 0), connection_attempts=counters.get('connect_attempts', 0),
                connection_success_ratio=counters.get('connected', 0) / max(1, counters.get('connect_attempts', 0)) if transport != 'udp' else None,
                submitted=counters.get('published', 0), event_accepted=accepted, sink_confirmed=acked,
                transport_received=int(sum(delta.get(k, 0) for k in ('netbaiot_mqtt_publishes_total', 'netbaiot_tcp_frames_total', 'netbaiot_udp_datagrams_total'))),
                accepted_per_s=accepted / args.duration, dropped=counters.get('schedule_missed', 0) + counters.get('client_window_full', 0),
                server_counters=delta, client=final['stats'], stages=histograms(before, after),
                resources=dict(samples=len(samples), peak_rss_kib=max(s['rss_kib'] for s in samples),
                    median_rss_kib=statistics.median(s['rss_kib'] for s in measured),
                    median_cpu_pct=statistics.median(s['cpu_pct'] for s in measured),
                    max_fd=max(s['fd_count'] for s in samples), max_tasks=max(s['runtime_tasks'] for s in samples),
                    max_connections=max(s['connections'] for s in samples), max_sessions=max(s['active_sessions'] for s in samples)),
                growth=growth(samples), final_queues_empty=not state['event_count'] and not state['pending_required'],
                final_sessions=state['active_sessions'])
            if summary['growth']['suspects']:
                write(out / 'reproducer.json', dict(status='candidate_requires_reduction_and_confirmation',
                    command=['python3', 'scripts/perf/production_readiness.py', '--devices', str(devices), '--codec', codec,
                        '--transport', transport, '--scenario', scenario, '--duration', str(args.duration), '--output', '<fresh-output>'],
                    load_config=load_config, suspected_growth=summary['growth']))
            os.kill(pid, signal.SIGTERM)  # Actual gateway, also when heaptrack owns the wrapper PID.
            try: server.wait(timeout=20)
            except subprocess.TimeoutExpired: raise RuntimeError('gateway retained ownership during shutdown; inspect spool/server.log')
            summary['server_exit_code'] = server.returncode
            if server.returncode != 0: summary['status'] = 'failed'
    except BaseException as error:
        summary['error'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        # Own only the processes launched by this case. Failed shutdown is recorded, never a PASS.
        for process in (load, server):
            if process is not None and process.poll() is None:
                process.terminate()
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired: process.kill(); process.wait(timeout=5)
        summary['evidence'] = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in out.glob('*.json') if p.name != 'summary.json'}
        write(out / 'summary.json', summary)
    print(json.dumps({k: summary[k] for k in ('status', 'devices', 'codec', 'transport', 'scenario', 'event_accepted', 'sink_confirmed', 'resources')}), flush=True)
    return summary


def main():
    def interrupted(_signal, _frame):
        raise KeyboardInterrupt('production campaign cancelled')
    signal.signal(signal.SIGTERM, interrupted)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--server', type=Path, default=ROOT / 'target/release/netbaiot-server')
    p.add_argument('--loadgen', type=Path, default=ROOT / 'target/release/netbaiot-loadgen')
    p.add_argument('--devices', type=int, nargs='+', default=[100])
    p.add_argument('--codec', nargs='+', choices=CODECS, default=list(CODECS))
    p.add_argument('--transport', nargs='+', choices=TRANSPORTS, default=list(TRANSPORTS))
    p.add_argument('--scenario', nargs='+', choices=SCENARIOS, default=['sensor'])
    p.add_argument('--fault', choices=FAULTS, default='none')
    p.add_argument('--duration', type=float, default=60)
    p.add_argument('--sample-interval', type=float, default=60)
    p.add_argument('--rate', type=float, help='aggregate messages/s; default sensor 0.1/device, other 1/device')
    p.add_argument('--ramp', type=float, default=500)
    p.add_argument('--threads', type=int, default=min(os.cpu_count() or 1, 8))
    p.add_argument('--tls', action='store_true')
    p.add_argument('--heaptrack', action='store_true')
    args = p.parse_args()
    if not (1 <= args.duration <= 86400 and 1 <= args.sample_interval <= 60 and 1 <= args.ramp <= 2000 and 1 <= args.threads <= 64) or any(not 1 <= n <= 10000 for n in args.devices) or (args.rate is not None and not 1 <= args.rate <= 1000000):
        p.error('invalid bounded duration/population/rate/sample configuration')
    if len(args.devices) * len(args.codec) * len(args.transport) * len(args.scenario) > 192:
        p.error('matrix exceeds 192 cases')
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    try: resource.setrlimit(resource.RLIMIT_NOFILE, (min(32768, hard) if hard != resource.RLIM_INFINITY else 32768, hard))
    except (OSError, ValueError): pass
    args.output.mkdir(parents=True, exist_ok=True)
    results = []
    for n in args.devices:
        for codec in args.codec:
            for transport in args.transport:
                if (args.fault in ('wrong_hmac', 'repeat_sequence') and transport != 'udp') or (args.fault in ('reconnect', 'half_connection') and transport == 'udp'):
                    continue
                for scenario in args.scenario:
                    out = args.output / f'{n}-{codec}-{transport}-{scenario}-{args.fault}'
                    results.append(run_case(args, out, codec, transport, scenario, n, args.fault))
                    write(args.output / 'index.json', dict(cases=[{k: r[k] for k in ('status', 'devices', 'codec', 'transport', 'scenario', 'fault')} for r in results]))
    if not results or any(r['status'] == 'failed' for r in results):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
