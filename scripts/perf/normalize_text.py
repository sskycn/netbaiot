#!/usr/bin/env python3
"""Remove trailing padding/blank EOF lines from textual audit artifacts only.

Numeric data, leading stack indentation, JSON and binary files are unchanged.
Run after artifact generation when preparing a reviewable Git diff.
"""
from pathlib import Path

root = Path(__file__).resolve().parents[2] / 'docs/performance'
changed = 0
for path in root.rglob('*'):
    if path.suffix not in ('.log', '.txt', '.svg') or not path.is_file():
        continue
    original = path.read_text()
    lines = [line.rstrip(' \t\r') for line in original.splitlines()]
    while lines and not lines[-1]:
        lines.pop()
    normalized = '\n'.join(lines) + ('\n' if lines else '')
    if normalized != original:
        path.write_text(normalized)
        changed += 1
print(f'Normalized trailing padding in {changed} text artifacts')
