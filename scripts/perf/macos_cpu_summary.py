#!/usr/bin/env python3
"""Summarize an Instruments time-profile XML export, retaining no local paths.

Export only the time-profile table; do not publish the trace TOC/environment.
Weights describe sampled running stacks, not syscall kernel time or wall waits.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET


def summarize(path):
    root = ET.parse(path).getroot()
    ids = {e.attrib['id']: e for e in root.iter() if 'id' in e.attrib}

    def resolve(element):
        return ids[element.attrib['ref']] if 'ref' in element.attrib else element

    leaf, inclusive, states = Counter(), Counter(), Counter()
    active = missing = 0
    for row in root.iter('row'):
        state = resolve(row.find('thread-state')).text
        weight = int(resolve(row.find('weight')).text)
        states[state] += weight
        if state != 'Running':
            continue
        stack_element = row.find('tagged-backtrace')
        if stack_element is None:
            missing += weight
            continue
        stack = resolve(stack_element)
        names = [resolve(frame).attrib['name'] for frame in stack.findall('frame')]
        if not names:
            missing += weight
            continue
        active += weight
        leaf[names[0]] += weight
        for name in set(names):
            inclusive[name] += weight

    def top(counter):
        return [dict(symbol=name, ms=ns / 1e6, percent=100 * ns / active)
                for name, ns in counter.most_common(50)]

    return dict(instrument='Instruments Time Profiler',
                source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                active_sampled_ms=active / 1e6, missing_stack_ms=missing / 1e6,
                states_ns=dict(states),
                leaf=top(leaf), inclusive=top(inclusive))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    args.output.write_text(json.dumps(summarize(args.input), separators=(',', ':')) + '\n')


if __name__ == '__main__':
    main()
