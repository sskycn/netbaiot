#!/usr/bin/env python3
"""Summarize native sample call graphs without double-counting inclusive stacks.
Categories describe non-waiting wall-stack samples, NOT exact CPU percentages.
"""
import collections,hashlib,json,pathlib,re,sys
ROOT=pathlib.Path(__file__).resolve().parents[2];OUT=ROOT/'docs/performance'

def classify(stack):
    leaf=stack[-1]
    if any(word in leaf for word in ['__psynch_cvwait','__psynch_mutexwait','kevent','semaphore_wait','mach_msg','__ulock_wait','park_internal']):return 'parked_or_waiting'
    if any(word in leaf.lower() for word in ['malloc','realloc','free','memcpy','memmove','bzero']):return 'allocation_or_copy_leaf'
    for frame in reversed(stack):
        if 'tracing_subscriber' in frame or 'tracing_core' in frame:return 'diagnostic_logging_context'
        if 'sqlx_' in frame:return 'sqlx_context'
        if 'netbaiot_codecs' in frame or 'serde_json' in frame:return 'json_codec_or_serialization_context'
        if 'netbaiot_runtime4auth' in frame or 'sha2' in frame or 'hmac' in frame:return 'authentication_or_crypto_context'
        if 'mqtt6packet' in frame:return 'mqtt_packet_context'
        if 'mqtt6topics' in frame:return 'mqtt_topic_context'
        if 'netbaiot_runtime5quota' in frame:return 'admission_context'
        # Match crate/module names, never the "ring" inside String/Ordering.
        if 'rustls' in frame or re.search(r'(?:_4ring|\bring::|ring_core_)',frame):return 'tls_context'
    return 'runtime_application_or_syscall_other'

def analyze(path):
    raw=path.read_text();body=raw.split('Call graph:',1)[1].split('Total number in stack',1)[0]
    entries=[]
    for line in body.splitlines():
        m=re.match(r'^([ +!|:]*)(\d+) (.*)$',line)
        if m:entries.append((len(m[1]),int(m[2]),m[3]))
    stack=[];categories=collections.Counter();leaves=collections.Counter();examples={};roots=0
    for index,(depth,count,frame) in enumerate(entries):
        while stack and stack[-1][0]>=depth:stack.pop()
        stack.append((depth,frame))
        if frame.startswith('Thread_'):roots+=count
        if index+1<len(entries) and entries[index+1][0]>depth:continue
        frames=[v for _,v in stack];category=classify(frames);categories[category]+=count;leaves[frame]+=count
        if category!='parked_or_waiting' and (category not in examples or count>examples[category]['samples']):examples[category]=dict(samples=count,stack=frames)
    total=sum(categories.values());busy=total-categories['parked_or_waiting']
    return dict(source=path.name,sha256=hashlib.sha256(raw.encode()).hexdigest(),method='exclusive leaves of sample wall-time call graph; parked leaves separated; inlining and syscall attribution limit precision; not exact CPU percentages',root_thread_samples=roots,leaf_samples=total,tree_counts_match=total==roots,non_waiting_samples=busy,categories=dict(categories),non_waiting_shares_percent={k:round(v*100/busy,2) for k,v in categories.items() if k!='parked_or_waiting'} if busy else {},top_leaves=[dict(frame=k,samples=v) for k,v in leaves.most_common(30)],representative_stacks=examples)

if __name__=='__main__':
    paths=[pathlib.Path(v) for v in sys.argv[1:]] or sorted(OUT.glob('*.sample.txt'))
    results={p.stem:analyze(p) for p in paths}
    if sys.argv[1:]:print(json.dumps(results,indent=2))
    else:(OUT/'profiles-summary.json').write_text(json.dumps(results,indent=2)+'\n')
