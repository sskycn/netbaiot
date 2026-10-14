#!/usr/bin/env python3
"""Verify published evidence, historical raw fuzz references and source snapshots."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
parser=argparse.ArgumentParser()
parser.add_argument('--repo',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
if args.output.exists():raise SystemExit('preserve verification output')
repo=args.repo.resolve();root=repo/'docs/performance/strict-review-20261013'
manifest=json.loads((root/'manifest.json').read_text());seen=set()
for row in manifest['records']:
    name=row['file']
    if name in seen:raise RuntimeError('duplicate record '+name)
    seen.add(name)
    path=root/name
    if path.is_symlink() or root not in path.resolve().parents:raise RuntimeError('unowned evidence reference '+name)
    data=path.read_bytes()
    if len(data)!=row['bytes'] or hashlib.sha256(data).hexdigest()!=row['sha256']:
        raise RuntimeError('evidence bytes/hash mismatch '+name)
old=repo/'docs/performance/connection-density-20261013';original=json.loads((old/'manifest.json').read_text())
for row in original['records']:
    data=(old/row['file']).read_bytes()
    if len(data)!=row['bytes'] or hashlib.sha256(data).hexdigest()!=row['sha256']:
        raise RuntimeError('original evidence mismatch '+row['file'])
raw_fuzz_verified=0
raw_fuzz_unavailable=[]
for row in original['fuzz']:
    path=repo/'target/density-results'/row['target_log']
    if not path.exists():
        raw_fuzz_unavailable.append(row['target_log']);continue
    data=path.read_bytes()
    if hashlib.sha256(data).hexdigest()!=row['log_sha256']:
        raise RuntimeError('original raw fuzz mismatch '+row['target_log'])
    raw_fuzz_verified+=1

build=json.loads((root/'candidate-build-metadata-2.json').read_text());b4=manifest['candidate_source_commit'];fixture=manifest['final_fixture_ci']['source_commit'];diff=[]
for file,digest in build['source_files_sha256'].items():
    source=subprocess.check_output(['git','show',b4+':'+file],cwd=repo)
    if hashlib.sha256(source).hexdigest()!=digest:raise RuntimeError('b4 source mismatch '+file)
    source=subprocess.check_output(['git','show',fixture+':'+file],cwd=repo)
    if hashlib.sha256(source).hexdigest()!=digest:diff.append(file)
if sorted(diff)!=['apps/netbaiot-server/tests/common/mod.rs','apps/netbaiot-server/tests/server.rs']:
    raise RuntimeError('unexpected fixture source differences')
sys.path.insert(0,str(repo/'scripts'))
from scrub_paths import scrub_tree
if scrub_tree(root,repo_root=repo):raise RuntimeError('machine-local paths in evidence')
if subprocess.run(['git','diff','--check'],cwd=repo,check=False,stdout=subprocess.DEVNULL).returncode:
    raise RuntimeError('git whitespace check failed')
result={'verified_at_utc':datetime.now(timezone.utc).isoformat(),
        'strict_records_verified_before_adding_this_result':len(manifest['records']),
        'original_records_verified':len(original['records']),'original_raw_fuzz_verified_now':raw_fuzz_verified,
        'original_raw_fuzz_unavailable_now':raw_fuzz_unavailable,
        'b4_frozen_source_files_verified':len(build['source_files_sha256']),
        'fixture_source_mismatch_files':diff,'path_scrubbing_clean':True,'git_diff_check_clean':True,
        'scope':'Published evidence byte counts/SHA-256, available original raw fuzz references and exact committed source snapshot comparison; unavailable original raw files are explicitly listed. This does not reproduce missing raw data or rerun workloads.'}
args.output.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
