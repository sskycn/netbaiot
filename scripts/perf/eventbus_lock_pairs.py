#!/usr/bin/env python3
"""Sequential paired EventBus tests; preserve exact commands and binary hashes."""
import argparse
import hashlib
import json
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
    parser.add_argument("--secondary", action="store_true")
    args = parser.parse_args()
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    binaries = {name: str(pathlib.Path(getattr(args, name)).resolve())
                for name in ("before", "after", "loadgen")}
    cases = [(f"q1-20k-{i}", 20000, 64, 20) for i in range(1, 4)]
    if args.secondary:
        cases = [("q1-25k", 25000, 64, 20), ("q1-30k", 30000, 64, 20)]
        cases += [(f"publishers-{n}", 20000, n, 10) for n in (1, 100, 1000)]
    manifest = {"binaries": binaries,
                "sha256": {k: hashlib.sha256(pathlib.Path(v).read_bytes()).hexdigest()
                           for k, v in binaries.items()}, "runs": []}
    for i, (name, rate, publishers, duration) in enumerate(cases):
        for side in (("before", "after") if i % 2 == 0 else ("after", "before")):
            command = [sys.executable, str(ROOT / "scripts/perf/event_load.py"),
                       "--server-bin", binaries[side], "--loadgen-bin", binaries["loadgen"],
                       "--rate", str(rate), "--qos", "1", "--connections", str(publishers),
                       "--duration", str(duration), "--warmup", "5", "--payload-bytes", "256",
                       "--sink-mode", "none"]
            filename = f"{side}-{name}.json"
            print(filename, flush=True)
            with (output / filename).open("w") as stream:
                subprocess.run(command, cwd=ROOT, stdout=stream, check=True, timeout=120)
            manifest["runs"].append({"file": filename, "command": command})
            suffix = "secondary" if args.secondary else "primary"
            (output / f"manifest-{suffix}.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
