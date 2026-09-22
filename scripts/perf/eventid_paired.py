#!/usr/bin/env python3
"""Alternate frozen binaries to check host drift after the primary matrix."""
import argparse
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--loadgen", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = pathlib.Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    for pair, order in enumerate((("before", "after"), ("after", "before"), ("before", "after")), 1):
        for phase in order:
            name = f"q1-20k-{pair}-{phase}"
            print(name, flush=True)
            with (root / f"{name}.json").open("w") as output:
                subprocess.run([sys.executable, str(ROOT / "scripts/perf/event_load.py"),
                                "--server-bin", getattr(args, phase), "--loadgen-bin", args.loadgen,
                                "--rate", "20000", "--qos", "1", "--connections", "64",
                                "--duration", "20", "--warmup", "5", "--sink-mode", "none"],
                               stdout=output, check=True, timeout=120, cwd=ROOT)


if __name__ == "__main__":
    main()
