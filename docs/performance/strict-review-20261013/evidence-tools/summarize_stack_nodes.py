#!/usr/bin/env python3
"""Count only native sample call-graph nodes, excluding repeated aggregate tables."""
import argparse
import json
from pathlib import Path
parser=argparse.ArgumentParser()
parser.add_argument('--evidence-root',type=Path,required=True)
parser.add_argument('--summary',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
if args.output.exists():raise SystemExit('preserve existing output')
summary=json.loads(args.summary.read_text())
for phase,case in summary['cases'].items():
    text=(args.evidence_root/phase/'server-stacks.txt').read_text()
    if text.count('Call graph:')!=1 or text.count('Total number in stack')!=1:
        raise SystemExit('unexpected stack report sections')
    graph=text.split('Call graph:',1)[1].split('Total number in stack',1)[0]
    case['call_graph_symbol_node_occurrences']={symbol:sum(symbol in line for line in graph.splitlines())
        for symbol in case['call_graph_symbol_node_occurrences']}
summary['symbol_count_method']='Count matching lines only between Call graph and Total number in stack; exclude all subsequent aggregation tables. These counts are nodes, not additive sample weights or CPU utilization.'
summary['derived_summary_revision']='Corrects symbol-node scope only; raw stacks, population, counters, latency and cleanup are unchanged. The prior derived summary is retained separately.'
args.output.write_text(json.dumps(summary,indent=2)+'\n')
