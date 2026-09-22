#!/usr/bin/env python3
"""Sequential fixed-window EventBus micro/slow-sink pairs with binary provenance."""
import argparse
import hashlib
import json
import pathlib
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--before', required=True)
parser.add_argument('--after', required=True)
parser.add_argument('--output', required=True)
args = parser.parse_args()
out = pathlib.Path(args.output)
out.mkdir(parents=True, exist_ok=True)
binaries = {side: str(pathlib.Path(getattr(args, side)).resolve()) for side in ('before', 'after')}
manifest = {'binaries': binaries, 'sha256': {s: hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest() for s, p in binaries.items()}, 'commands': []}
for i in range(1, 4):
    for side in (('before', 'after') if i % 2 else ('after', 'before')):
        path = out / f'{side}-micro-{i}.jsonl'
        print(path.name, flush=True)
        with path.open('w') as stream:
            subprocess.run([binaries[side]], stdout=stream, check=True, timeout=120)
        manifest['commands'].append({'command': [binaries[side]], 'output': path.name})
for side, binary in binaries.items():
    path = out / f'{side}-isolation.json'
    print(path.name, flush=True)
    with path.open('w') as stream:
        subprocess.run([binary, '--overload'], stdout=stream, check=True, timeout=30)
    manifest['commands'].append({'command': [binary, '--overload'], 'output': path.name})
(out / 'manifest-micro.json').write_text(json.dumps(manifest, indent=2) + '\n')
