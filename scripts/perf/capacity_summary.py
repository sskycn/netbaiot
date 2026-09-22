#!/usr/bin/env python3
"""Summarize retained capacity audit records without manufacturing missing metrics."""
import argparse
import csv
import json
import pathlib
import statistics

from eventbus_summary import histogram


def network_rates(path, begin, end):
    if not path.exists():
        return None
    with path.open() as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames or reader.fieldnames[0] != 'epoch':
            return None  # Early collector files have no timestamps; do not guess.
        rows = [dict(epoch=float(r['epoch']), rx=int(r['bytes_in']), tx=int(r['bytes_out'])) for r in reader]
    if len(rows) < 2:
        return None
    first = min(rows, key=lambda r: abs(r['epoch']-begin))
    last = min(rows, key=lambda r: abs(r['epoch']-end))
    seconds = last['epoch']-first['epoch']
    if seconds <= 0 or max(abs(first['epoch']-begin), abs(last['epoch']-end)) > 2:
        return None
    return dict(rx_bytes_s=(last['rx']-first['rx'])/seconds,
                tx_bytes_s=(last['tx']-first['tx'])/seconds, window_seconds=seconds,
                max_boundary_offset_s=max(abs(first['epoch']-begin), abs(last['epoch']-end)))


def summarize(path):
    result = json.loads(path.read_text())
    directory = path.parent
    load = json.loads((directory/'loadgen-metadata.json').read_text())
    config = load['configuration']
    result.update(qos=config['qos'],connections=config['connections'],tls='tls_ca' in config,
                  requested_payload_bytes=config['payload_bytes'],duration_s=config['duration_secs'],
                  warmup_s=config['warmup_secs'],cooldown_s=config['cooldown_secs'],
                  sink_mode=load['manifest']['sink_mode'])
    if result.get('idle'):
        result['connect_latency'] = result['final']['stats']['latencies'].get('connect')
        result['errors'] = result['final']['stats']['counters'].get('client_errors', 0)
        result.pop('final')
        return result
    bounds = json.loads((directory/'boundaries.json').read_text())
    start, finish = bounds['measurement_start'], bounds['measurement_end']
    result['server_network'] = network_rates(directory/'bundle/server-network.csv', start['epoch'], finish['epoch'])
    result['loadgen_network'] = network_rates(directory/'loadgen-network.csv', start['epoch'], finish['epoch'])
    rows = [json.loads(line) for line in (directory/'samples.jsonl').read_text().splitlines()]
    measured = [row for row in rows if row['phase']=='measurement']
    latency_name={0:None,1:'puback',2:'pubcomp'}[config['qos']]
    latency=result['latencies'].get(latency_name,{})
    result.update({key:latency.get(key) for key in ('p50_ms','p95_ms','p99_ms')})
    result['server_rx_bytes_s']=result['server_network']['rx_bytes_s'] if result['server_network'] else None
    result['server_tx_bytes_s']=result['server_network']['tx_bytes_s'] if result['server_network'] else None
    result['fds_peak'] = max((r['process'].get('fds') or 0 for r in measured), default=None)
    result['threads_peak'] = max((r['process'].get('threads') or 0 for r in measured), default=None)
    result['connections_peak'] = max((r['status']['active_connections']['mqtt'] for r in measured), default=None)
    if start['sink_process'] and finish['sink_process']:
        result['sink_cpu_cores'] = (finish['sink_process']['cpu_seconds']-start['sink_process']['cpu_seconds'])/result['server_window_seconds']
    else:
        result['sink_cpu_cores'] = None
    result['sink_acks_s'] = result['metric_deltas'].get('netbaiot_sink_acks_total',0)/result['server_window_seconds']
    result['sink_retries'] = result['metric_deltas'].get('netbaiot_sink_retries_total')
    result['event_accepted_to_sink_ack_us'] = histogram(result['metric_deltas'], 'event_accepted_to_sink_ack_us')
    result['event_bus_state_wait_us'] = histogram(result['metric_deltas'], 'event_bus_state_wait_us')
    result['mqtt_pubacks_s'] = result['metric_deltas'].get('netbaiot_mqtt_pubacks_total',0)/result['server_window_seconds']
    result['mqtt_packets_received_s'] = result['metric_deltas'].get('netbaiot_mqtt_packets_received_total',0)/result['server_window_seconds']
    result['rejections'] = {k:v for k,v in result['metric_deltas'].items() if 'reject' in k and k.endswith('_total')}
    result['errors'] = result['counters'].get('client_errors',0)
    consumer_log=directory/'bundle/sink.log'
    if result['sink_mode']=='tcp' and consumer_log.exists():
        records=[]
        for line in consumer_log.read_text().splitlines():
            try:
                records.append(json.loads(line))
            except ValueError:
                pass
        ready=[r for r in records if r.get('event')=='ready']
        final=next((r for r in reversed(records) if r.get('event')=='final'),None)
        result['consumer_final']=final
        result['consumer_outstanding_sampled_peak']=max((r.get('outstanding',0) for r in records),default=None)
        if ready and any(load['manifest'].get(k,0)>0 for k in ('tcp_connect_delay','tcp_disconnect_after','tcp_filter_mismatch_seconds')):
            recovery_start=ready[-1]['epoch']
            recovered=next((r for r in rows if r['epoch']>=recovery_start and r['status']['pending_required']<=max(1,result['intended_s']*.01)),None)
            result['recovery_after_consumer_ready_s']=recovered['epoch']-recovery_start if recovered else None
    result['rss_drift_kib'] = result['rss_last_kib']-result['rss_first_kib']
    result['max_boundary_receipt_delay_s'] = max((r.get('boundary_receipt_delay_seconds',0) for r in (start,finish)), default=None)
    result['scheduled_slots_s'] = result['counters'].get('measurement_scheduled_slots',0)/result['duration_s']
    result['missed_schedule_s'] = result['counters'].get('measurement_schedule_missed',0)/result['duration_s']
    for key in ('network_begin','network_end','metric_deltas'):
        result.pop(key,None)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=pathlib.Path)
    parser.add_argument('--output',type=pathlib.Path,required=True)
    args=parser.parse_args()
    points=[summarize(path) for path in sorted(args.root.glob('*/result.json')) if not path.parent.name.startswith('smoke')]
    repeats={}
    for rate in (10000,20000,30000):
        group=[p for p in points if p['label'].startswith('q1-%d-r' % rate)]
        if group:
            accepted=[p['accepted_s'] for p in group]
            repeats[str(rate)]=dict(repeats=len(group),accepted_s_median=statistics.median(accepted),
                accepted_s_best=max(accepted),accepted_s_worst=min(accepted),
                variation_percent=(max(accepted)-min(accepted))/statistics.median(accepted)*100,
                all_eligible=all(p['result']=='HEALTHY' for p in group))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(dict(points=points,qos1_repeats=repeats),indent=2,allow_nan=False)+'\n')
    fields=['label','qos','connections','tls','intended_s','attempted_s','accepted_s','client_completed_s',
            'p50_ms','p95_ms','p99_ms','server_cpu_cores','loadgen_cpu_cores','server_rss_peak_kib','loadgen_rss_peak_kib','server_rx_bytes_s','server_tx_bytes_s',
            'pending_peak','pending_end','missed_schedule_s','errors','result']
    with args.output.with_suffix('.csv').open('w') as output:
        writer=csv.DictWriter(output,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(points)
    print(json.dumps(repeats,indent=2))


if __name__=='__main__':
    main()
