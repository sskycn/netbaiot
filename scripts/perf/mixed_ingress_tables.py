#!/usr/bin/env python3
"""Render audit tables from aggregate evidence. Never substitutes missing results."""
import argparse
import json
from pathlib import Path

PROTOCOLS = ['http','mqtt','tcp','udp']


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('results',type=Path)
    parser.add_argument('output',type=Path)
    args=parser.parse_args()
    evidence=json.loads(args.results.read_text()); data=evidence['aggregate']; lines=[]
    def add(*text): lines.extend(text)
    def stat(group,field):
        value=group.get(field)
        return value['median'] if value else None
    def number(value,decimals=1):
        return '—' if value is None else f'{value:,.{decimals}f}'
    def table(headers,rows):
        add('| '+' | '.join(headers)+' |','|'+'|'.join('---' for _ in headers)+'|')
        for row in rows: add('| '+' | '.join(map(str,row))+' |')
        add('')
    def existing(variant,scenario): return data.get(variant+'/'+scenario)
    add('## Repeated solo operating points','',
        'Rates are acknowledged events/s. These are the highest calibrated offered points selected for repetition, not a formal maximum or zero-error SLA. Percentiles are medians of per-run successful-receipt percentiles. Ranges are across repeats.','')
    rows=[]
    for p in PROTOCOLS:
        r=existing('baseline','solo-'+p)
        if not r: continue
        g=r['groups'][p]; speed=g['accepted_per_second']; lat=g['latency']
        rows.append([p,speed['n'],number(speed['median'],0),f'{speed["minimum"]:,.0f}–{speed["maximum"]:,.0f}',number(stat(g,'success_pct'),2),number(stat(g,'offered_coverage_pct'),2),number(stat(lat,'p95_ms'),2),number(stat(lat,'p99_ms'),2)])
    table(['Protocol','N','Accepted/s','Range','ACK/attempted %','ACK/offered %','P95 ms','P99 ms'],rows)
    add('## TLS mixed throughput and resources','','These five scenario tables use the frozen baseline. Each key mixed run lasts 300 measurement seconds; warmup is excluded. Final-candidate paired costs are reported separately below. CPU is process CPU time expressed in cores. RSS is the median run peak.','')
    rows=[]
    for scenario in ['balanced','mqtt-heavy','http-heavy','tcp-heavy','udp-heavy']:
        r=existing('baseline',scenario)
        if not r: continue
        row=[scenario,len(r['runs'])]
        for p in PROTOCOLS:
            g=r['groups'][p]; row.append(f'{number(stat(g,"accepted_per_second"),0)} ({number(stat(g,"offered_coverage_pct"),1)}%)')
        row.extend([number(stat(r['cpu_pct'],'server')/100,2),number(stat(r,'peak_rss_kib')/1024,2)])
        rows.append(row)
    table(['Scenario','N','HTTP/s (coverage)','MQTT/s (coverage)','TCP/s (coverage)','UDP/s (coverage)','CPU cores','RSS MiB'],rows)
    add('## Spread across mixed repetitions', '', 'Minimum–maximum across complete trials, including slower runs. These are observed ranges, not confidence intervals.', '')
    rows=[]
    for scenario in ['balanced','mqtt-heavy','http-heavy','tcp-heavy','udp-heavy']:
        r=existing('baseline',scenario)
        if not r: continue
        for p in PROTOCOLS:
            g=r['groups'][p]; rate=g['accepted_per_second']; coverage=g['offered_coverage_pct']; tail=g['latency']['p99_ms']
            rows.append([scenario,p,number(rate['minimum'],0)+'–'+number(rate['maximum'],0),
                         number(coverage['minimum'],2)+'–'+number(coverage['maximum'],2),
                         number(tail['minimum'],2)+'–'+number(tail['maximum'],2)])
    table(['Scenario','Protocol','ACK/s range','Offered coverage % range','P99 ms range'],rows)
    add('## Fairness and acknowledged latency','','Retained capacity = mixed accepted/s divided by baseline solo accepted/s. Reduced allocated share alone is not starvation. Receipt failure % uses attempts, including failed writes, and excludes generator schedule/window drops; offered coverage in the preceding table includes them.','')
    rows=[]
    for scenario in ['balanced','mqtt-heavy','http-heavy','tcp-heavy','udp-heavy']:
        r=existing('baseline',scenario)
        if not r: continue
        for p in PROTOCOLS:
            solo=existing('baseline','solo-'+p)
            if not solo: continue
            g=r['groups'][p]; capacity=stat(solo['groups'][p],'accepted_per_second'); rate=stat(g,'accepted_per_second')
            rows.append([scenario,p,number(capacity,0),number(rate,0),number(rate/capacity*100,1)+'%',number(stat(g['latency'],'p95_ms'),2),number(stat(g['latency'],'p99_ms'),2),number(100-stat(g,'success_pct'),2) if stat(g,'success_pct') is not None else '—'])
    table(['Scenario','Protocol','Solo/s','Mixed/s','Retained','P95 ms','P99 ms','Receipt failure %'],rows)
    add('## Mixed failure signals', '', 'Median rates across 300-second trials. Disconnects overlap their causal errors; do not add these columns. All remaining wire failure categories are retained in the JSON index.', '')
    rows=[]
    for scenario in ['balanced','mqtt-heavy','http-heavy','tcp-heavy','udp-heavy']:
        r=existing('baseline',scenario)
        if not r: continue
        for p in PROTOCOLS:
            g=r['groups'][p]
            row=[scenario,p,number(100-stat(g,'success_pct'),2)]
            for field in ['disconnects','http_overloaded','udp_no_ack','read_timeout','tls_failure']:
                row.append(number(stat(g['errors'],field)/300,3))
            rows.append(row)
    table(['Scenario','Protocol','Receipt failure %','Disconnect/s','HTTP overload/s','UDP no ACK/s','Read timeout/s','TLS failure/s'],rows)
    add('## Paired production-candidate cost','','Interleaved baseline/final runs use the same generator and offered workloads.','')
    rows=[]
    for scenario in ['solo-http','solo-mqtt','solo-tcp','balanced']:
        before=existing('baseline',scenario); after=existing('final',scenario)
        if not before or not after: continue
        a=stat(before,'total_accepted_per_second'); b=stat(after,'total_accepted_per_second')
        rows.append([scenario,len(before['runs']),len(after['runs']),number(a,0),number(b,0),number((b/a-1)*100,2)+'%',
                     number(stat(before['cpu_pct'],'server')/100,2)+' → '+number(stat(after['cpu_pct'],'server')/100,2),
                     number(stat(before,'peak_rss_kib')/1024,2)+' → '+number(stat(after,'peak_rss_kib')/1024,2)])
    table(['Scenario','Before N','After N','Before/s','After/s','Delta','CPU cores before → after','RSS MiB before → after'],rows)
    add('## Connection interference','','Values are accepted/offered coverage except `new MQTT auth`, which is application-authenticated/connection-attempt percentage including warmup. TCP/TLS connection success is also preserved separately in JSON. Blank means no such probe, not zero errors.','')
    rows=[]
    for variant,scenario in [('baseline','idle-final-256'),('candidate','idle-256-v3'),('final','idle-final-256'),
                             ('baseline','default-ip-occupancy-32'),('final','default-ip-occupancy-32'),
                             ('baseline','tls-pending-v3'),('candidate','tls-pending-v3'),('final','tls-pending-final')]+[(v,s+'-established-and-new') for s in ['slow_http','slow_tcp','unclassified','tls_storm'] for v in ['baseline','final']]:
        r=existing(variant,scenario)
        if not r: continue
        fields=[variant+'/'+scenario,len(r['runs'])]
        for p in PROTOCOLS:
            g=r['groups'].get(p,{}); fields.append(number(stat(g,'offered_coverage_pct'),2))
        g=r['groups'].get('mqtt_new',{})
        fields += [number(stat(g,'app_connect_success_pct'),2),number(stat(r['cpu_pct'],'server')/100,2)]
        rows.append(fields)
    table(['Scenario','N','HTTP %','MQTT %','TCP %','UDP %','New MQTT auth %','CPU cores'],rows)
    add('## Offered-load staircases','','Exploratory steps are one 30-second run each; repeated saturation trials above validate the connection cliff. Percentages refer to the calibrated offered operating points (HTTP 8,000/s; UDP 80,000/s), or 256 idle connections for occupancy. Actual attempt rates and window misses remain in JSON.','')
    for kind in ['http-cliff','udp-cliff','occupancy-cliff']:
        add('### '+kind,''); rows=[]
        for percent in [20,40,60,80,100,120]:
            for variant in ['baseline','final'] if kind=='occupancy-cliff' else ['baseline']:
                r=existing(variant,kind+'-'+str(percent))
                if not r: continue
                row=[variant,percent]
                for p in PROTOCOLS:
                    g=r['groups'].get(p,{})
                    row.append(f'{number(stat(g,"offered_coverage_pct"),1)}% / {number(stat(g.get("latency",{}),"p99_ms"),2)}')
                rows.append(row)
        table(['Build','Load %','HTTP coverage/P99 ms','MQTT coverage/P99 ms','TCP coverage/P99 ms','UDP coverage/P99 ms'],rows)
    args.output.write_text('\n'.join(lines)+'\n')


if __name__=='__main__':
    main()
