#!/usr/bin/env python3
"""Derive matched-window summaries; raw results remain authoritative."""
import csv,json,pathlib,statistics,sys
ROOT=pathlib.Path(__file__).resolve().parents[2]
OUT=ROOT/'docs/performance'

def final(d,label='generator'):
    return next((x.get('stats',{}) for x in reversed(d.get(label,[])) if x.get('event')=='final'),{})

def summary(d):
    load=d.get('spec',{}).get('load',{});n=load.get('connections',100)
    begin=n/load.get('ramp_per_sec',100)+load.get('warmup_secs',3)
    duration=sum(p['seconds'] for p in load.get('phases',[])) or load.get('duration_secs',30)
    rows=[r for r in d.get('samples',[]) if begin+2<=r['elapsed_s']<=begin+duration-1 and 'rss_kib' in r.get('server',{})]
    f=final(d);c=f.get('counters',{});lat=f.get('latencies',{})
    final_elapsed=next((x.get('elapsed_s') for x in reversed(d.get('generator',[])) if x.get('event')=='final'),None)
    def vals(k,label='server'):return [r[label][k] for r in rows if k in r.get(label,{})]
    def stats(k,label='server'):
        v=vals(k,label);return dict(median=statistics.median(v),min=min(v),max=max(v)) if v else {}
    def rate(k,label):
        good=[r for r in rows if k in r.get(label,{})]
        if len(good)<2:return None
        elapsed=good[-1]['elapsed_s']-good[0]['elapsed_s']
        if k=='cpu_seconds':
            return sum(max(0,b[label][k]-a[label][k]) for a,b in zip(good,good[1:]) if a[label].get('pid')==b[label].get('pid'))/elapsed
        return (good[-1][label][k]-good[0][label][k])/elapsed
    accepted=c.get('accepted',0)/duration
    acceptance_scope='client application receipts; commands counted separately'
    if load.get('transport')=='udp':
        accepted=None
        acceptance_scope='no UDP receipts; mixed node aggregate is not attributed to this generator'
        if not d.get('spec',{}).get('extra_loads'):
            end=d.get('cooldown',{}).get('metrics',{}).get('netbaiot_ingress_accepted_total')
            begin_count=d.get('baseline',{}).get('metrics',{}).get('netbaiot_ingress_accepted_total')
            if end is not None and begin_count is not None and end>=begin_count:
                accepted=(end-begin_count)/duration
                acceptance_scope='server ingress counter delta, isolated UDP workload; no per-datagram receipt'
    result=dict(name=d.get('name'),error=d.get('error'),duration_s=duration,connections=n,counters=c,latencies=lat,
      measured_accepted_per_s=accepted,acceptance_scope=acceptance_scope,
      generated_payload_bytes_per_measurement_second=c['payload_bytes']/duration if 'payload_bytes' in c else None,
      lifetime_wire_bytes_per_second=c['wire_bytes_sent']/final_elapsed if 'wire_bytes_sent' in c and final_elapsed else None,
      byte_rate_scope='generated telemetry/heartbeat payload over configured measurement; wire includes connection/control writes over full generator lifetime, excludes reqwest traffic and received bytes',
      server_rss_kib=stats('rss_kib'),server_cpu_cores=rate('cpu_seconds','server'),
      generator_cpu_cores=rate('cpu_seconds','generator'),sink_cpu_cores=rate('cpu_seconds','sink'),
      pg_commits_per_s=rate('xact_commit','postgres'),pg_wal_bytes_per_s=rate('wal_bytes','postgres'),
      pg_lock_waiters_max=max(vals('lock_waiters','postgres'),default=None),pg_connections_max=max(vals('connections','postgres'),default=None),
      outbox_peak=max(vals('outbox_pending','postgres'),default=None),outbox_oldest_peak_ms=max(vals('oldest_outbox_ms','postgres'),default=None),
      outbound_bytes_peak=max(vals('netbaiot_queue_bytes','metrics'),default=None),outbound_count_peak=max(vals('netbaiot_queue_depth','metrics'),default=None),
      runtime_tasks=stats('netbaiot_runtime_alive_tasks','metrics'),fds=stats('fds'),threads=stats('threads'),
      ingress_inflight_peak=max(vals('netbaiot_ingress_inflight','metrics'),default=None),
      baseline_rss_kib=d.get('baseline',{}).get('server',{}).get('rss_kib'),
      after_disconnect_rss_kib=d.get('after_disconnect',{}).get('server',{}).get('rss_kib'),
      cooldown_rss_kib=d.get('cooldown',{}).get('server',{}).get('rss_kib'),
      cooldown_outbox=d.get('cooldown',{}).get('postgres',{}).get('outbox_pending'),
      cooldown_metrics=d.get('cooldown',{}).get('metrics',{}),
      baseline_db_bytes=d.get('baseline',{}).get('postgres',{}).get('db_bytes'),
      cooldown_db_bytes=d.get('cooldown',{}).get('postgres',{}).get('db_bytes'),
      limits=d.get('limits'),measurement_limitation=d.get('measurement_limitation'))
    if (rows and result['baseline_rss_kib'] is not None and load.get('transport','mqtt') in ('mqtt','tcp')
        and not d.get('spec',{}).get('extra_loads') and not load.get('reconnect_every_secs',0)
        and c.get('connected')==n):
        # Cumulative successful reconnects are never a concurrent population.
        result['incremental_rss_bytes_per_connection']=(result['server_rss_kib']['median']-result['baseline_rss_kib'])*1024/n
        result['rss_estimate_scope']='growth / initial successful connections; includes allocator/runtime effects; inspect live population and errors'
    for label in [f'generator_{i}' for i in range(1,8)]:
        if label in d:result[label]=dict(final=final(d,label),cpu_cores=rate('cpu_seconds',label),rss_kib=stats('rss_kib',label))
    return result

def main():
    results=[]
    for p in sorted(OUT.glob('*.json')):
        d=json.loads(p.read_text())
        if 'samples' in d and d.get('ended_epoch'):results.append(summary(d))
    (OUT/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
    print(json.dumps([{k:r[k] for k in ['name','error','measured_accepted_per_s','server_rss_kib','server_cpu_cores','outbox_peak','cooldown_outbox']} for r in results],indent=2))
if __name__=='__main__':main()
