#!/usr/bin/env python3
"""Annotate effective settings of earlier runner versions; never change measurements."""
import json,pathlib
root=pathlib.Path(__file__).resolve().parents[2]/'docs/performance'
for p in root.glob('*.json'):
    d=json.loads(p.read_text())
    if not isinstance(d,dict) or not d.get('ended_epoch') or 'spec' not in d:continue
    overrides=dict(credentials=d.get('credentials',d['spec'].get('credentials',100)),observer_credential=d.get('metrics_credential','a0'),observer_tenant_isolation=d.get('metrics_observer_isolated_tenant',False),quiesce_checkpoint='checkpoint_before_seconds' in d)
    if p.stem.startswith(('idle_tls_','ramp_tls_')) and 'cold' not in p.stem:
        overrides['load']=dict(d['spec']['load'],tls_resumption=True)
    if d['spec'].get('profile') and d['spec'].get('vmmap') and 'diagnostic_events' not in d:
        overrides['profile_vmmap_overlap']=True
    d['reproduction_overrides']=overrides
    p.write_text(json.dumps(d,separators=(',',':'))+'\n')
