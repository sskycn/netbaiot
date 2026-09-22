#!/usr/bin/env python3
"""Retain macOS sample wall-stack counts, without mislabeling them CPU shares."""
import argparse
import json
import pathlib
import re


def profile(path):
    source=path.read_text()
    marker='Sort by top of stack, same collapsed (when >= 5):'
    if marker not in source:
        raise ValueError('missing collapsed sample section')
    section=source.split(marker,1)[1].split('Binary Images:',1)[0]
    rows=[]
    for line in section.splitlines():
        match=re.match(r'^\s+(.*?)\s+\(in (.*?)\)\s+(\d+)\s*$',line)
        if match:
            rows.append(dict(symbol=match[1],module=match[2],samples=int(match[3])))
    total=sum(int(m[1]) for m in re.finditer(r'^    (\d+) Thread_',source,re.M))
    park=sum(row['samples'] for row in rows if row['symbol'] in ('__psynch_cvwait','kevent'))
    nonpark=total-park
    groups={
        'mutex_wait':lambda r:r['symbol']=='__psynch_mutexwait',
        'send_recv':lambda r:r['symbol'] in ('__sendto','__recvfrom'),
        'getentropy':lambda r:r['symbol']=='getentropy',
        'allocation':lambda r:r['module']=='libsystem_malloc.dylib',
        'copy':lambda r:any(v in r['symbol'] for v in ('memcpy','memmove')),
        'json_decode_serialize':lambda r:'serde_json' in r['symbol'] or ('netbaiot_codecs' in r['symbol'] and 'JsonV1' in r['symbol']),
        'broker_message_clone':lambda r:'BrokerMessage' in r['symbol'] and 'clone' in r['symbol'],
    }
    counts={name:sum(r['samples'] for r in rows if predicate(r)) for name,predicate in groups.items()}
    return dict(total_wall_stack_samples=total,park_samples=park,nonpark_samples=nonpark,
                reported_collapsed_symbols=len(rows),top10_reported=rows[:10],
                grouped_reported_samples=counts,
                grouped_nonpark_percent={k:v/nonpark*100 if nonpark else None for k,v in counts.items()},
                note='Wall samples, not CPU-cycle shares. Symbols below sample reporting threshold are omitted; zero named samples does not mean zero cost. getentropy is not exclusive EventId attribution.')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root',type=pathlib.Path)
    parser.add_argument('--output',type=pathlib.Path,required=True)
    args=parser.parse_args()
    result={p.parent.name:profile(p) for p in sorted(args.root.glob('profile-*/server.sample.txt'))}
    args.output.write_text(json.dumps(result,indent=2,allow_nan=False)+'\n')
    print(json.dumps({k:{'nonpark':v['nonpark_samples'],'reported_symbols':v['reported_collapsed_symbols'],'groups':v['grouped_nonpark_percent']} for k,v in result.items()},indent=2))


if __name__=='__main__':main()
