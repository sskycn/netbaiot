#!/usr/bin/env python3
"""Sample a bounded 15-second direct EventId generation loop on macOS."""
import argparse
import pathlib
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    root = pathlib.Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    with (root / "direct-profile.json").open("w") as output:
        process = subprocess.Popen([args.binary, "profile", "1", "1000000"], stdout=output)
        try:
            time.sleep(1)
            subprocess.run(["sample", str(process.pid), "10", "1", "-file",
                            str(root / "direct.sample.txt")], check=True, timeout=30,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if process.wait(timeout=30):
                raise RuntimeError("direct profile probe failed")
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()


if __name__ == "__main__":
    main()
