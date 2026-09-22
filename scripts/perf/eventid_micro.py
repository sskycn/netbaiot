#!/usr/bin/env python3
"""Serial ID probes; child resource accounting, bounded collision/overlap memory."""
import argparse
import json
import pathlib
import resource
import select
import subprocess


def overlap(binary, count):
    children = [subprocess.Popen([binary, "emit", "1", str(count)], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
    try:
        for child in children:
            ready, _, _ = select.select([child.stderr], [], [], 30)
            if not ready or child.stderr.readline() != b"ready\n":
                raise RuntimeError("process did not reach overlap barrier")
        for child in children:
            child.stdin.write(b"go\n")
            child.stdin.close()
            child.stdin = None
        for child in children:
            data, error = child.communicate(timeout=30)
            if child.returncode or len(data) != count * 16:
                raise RuntimeError(error.decode())
            yield data
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stress", action="store_true")
    args = parser.parse_args()
    root = pathlib.Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    rows = []
    for threads in (1, 2, 4, 8, 10):
        for repetition in range(1, 4):
            before = resource.getrusage(resource.RUSAGE_CHILDREN)
            result = subprocess.run([args.binary, "bench", str(threads), "1000000"],
                                    capture_output=True, text=True, check=True, timeout=120)
            after = resource.getrusage(resource.RUSAGE_CHILDREN)
            row = json.loads(result.stdout)
            row["repetition"] = repetition
            row["cpu_seconds"] = (after.ru_utime + after.ru_stime) - (before.ru_utime + before.ru_stime)
            row["voluntary_context_switches"] = after.ru_nvcsw - before.ru_nvcsw
            row["involuntary_context_switches"] = after.ru_nivcsw - before.ru_nivcsw
            rows.append(row)
            (root / "micro.json").write_text(json.dumps(rows, indent=2))
            print(threads, repetition, round(row["wall_ns_per_id"], 2), flush=True)
    if args.stress:
        stress = []
        for threads in (1, 2, 4, 8, 10):
            # Ten million IDs in each case, <=320 MB transient raw storage.
            result = subprocess.run([args.binary, "unique", str(threads), str(10000000 // threads)],
                                    capture_output=True, text=True, check=True, timeout=180)
            stress.append(json.loads(result.stdout))
            (root / "collision.json").write_text(json.dumps(stress, indent=2))
        # One million IDs across 100 independent execs in barrier-released pairs.
        # A bounded set (~100 MB on this host), separate from the 10m sort cases.
        ids = set()
        for _ in range(50):
            for data in overlap(args.binary, 10000):
                for at in range(0, len(data), 16):
                    item = data[at:at+16]
                    if item in ids:
                        raise RuntimeError("cross-process duplicate")
                    ids.add(item)
        (root / "processes.json").write_text(json.dumps({
            "process_starts": 100, "overlapping_pairs": 50, "ids": len(ids), "duplicates": 0}))

        ids.clear()
        for data in overlap(args.binary, 1000000):
            for at in range(0, len(data), 16):
                item = data[at:at+16]
                if item in ids:
                    raise RuntimeError("large blue/green overlap duplicate")
                ids.add(item)
        (root / "overlap.json").write_text(json.dumps({
            "processes": 2, "ids": len(ids), "duplicates": 0}))


if __name__ == "__main__":
    main()
