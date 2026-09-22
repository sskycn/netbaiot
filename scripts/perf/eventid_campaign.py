#!/usr/bin/env python3
"""Sequential fixed-configuration EventId end-to-end comparison matrix."""
import argparse
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-bin", required=True)
    parser.add_argument("--loadgen-bin", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = pathlib.Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    cases = [(f"q1-20k-{i}",1,20000,20) for i in range(1,4)]
    cases += [("q1-25k",1,25000,15),("q1-30k",1,30000,15),
              ("q0-25k",0,25000,15),("q2-20k",2,20000,20),("profile-30k",1,30000,20)]
    for name,qos,rate,duration in cases:
        command = [sys.executable,str(ROOT/"scripts/perf/event_load.py"),"--server-bin",args.server_bin,
                   "--loadgen-bin",args.loadgen_bin,"--rate",str(rate),"--qos",str(qos),
                   "--connections","64","--duration",str(duration),"--warmup","5","--sink-mode","none"]
        if name.startswith("profile"):
            command += ["--sample-output",str(output/"profile.sample.txt"),"--sample-seconds","10"]
        print(name,flush=True)
        with (output/f"{name}.json").open("w") as result:
            subprocess.run(command,stdout=result,check=True,cwd=ROOT,timeout=120)

if __name__ == "__main__":
    main()
