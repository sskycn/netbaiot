#!/usr/bin/env python3
"""Summarize ACK/delivery and owned cleanup without assigning capacity or CPU windows."""
import argparse
import json
from pathlib import Path
parser=argparse.ArgumentParser()
parser.add_argument('--input',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
if args.output.exists():raise SystemExit('preserve existing summary')
records=json.loads((args.input/'outcomes.json').read_text())
rows=[]
for r in records:
    result=json.loads((args.input/r['json']).read_text())
    stats=result['load']['stats'];latencies=stats['latencies'];counters=stats['counters']
    rows.append({**r,'application_ack_p99_ms':(latencies.get('application_ack') or {}).get('p99_ms'),
                 'puback_p99_ms':(latencies.get('puback') or {}).get('p99_ms'),
                 'connect_p99_ms':(latencies.get('connect') or {}).get('p99_ms'),
                 'server_cpu_exact_measured_window_percent':None,
                 'held_online_status':None,
                 'measured_client_count':counters.get('connected'),
                 'sink_rejected_including_prewarm':result.get('sink',{}).get('rejected'),
                 'lifecycle_samples':len(result.get('samples',[]))})
summary={'source_commit':'b4db4b7be6f3207aac34a9610eb0ce96d4f23e96',
         'placement':'macOS same-host serial loopback; 32 clients, not 10K large-payload population',
         'method_metadata':'large-prewarmed/campaign.json','runs':rows,
         'notes':['Normal cases publish 2000 padded 65000-byte wire payloads over 20 seconds; one separate 256-byte prewarm event is excluded from measured counters.',
                  'HTTP received/ACK totals include prewarm; the explicitly subtracted measured counts must equal main published counts.',
                  'Connected/disconnected are real load-generator counters; the event harness does not export held online status, which remains null.',
                  'MQTT subscribed raw payload delivery is not a DeviceCommand benchmark.',
                  'JSON whitespace padding is not part of normalized business payload; the normalized body is smaller than the wire payload.',
                  'The legacy event harness CPU field is cumulative process CPU, including initialization and prewarm; exact measured-window CPU remains null.',
                  'Intentional partial-frame disconnects publish zero events and are never counted as successful publish throughput.']}
args.output.write_text(json.dumps(summary,indent=2)+'\n')
