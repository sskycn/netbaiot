#!/usr/bin/env python3
"""Sequential, same-binary EventBus experiment matrix; no concurrent load runs."""
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
    cases = [(f"q1-20k-{i}",1,20000,64,20) for i in range(1,4)]
    cases += [("q1-25k",1,25000,64,15),("q1-30k",1,30000,64,15),
              ("q0-25k",0,25000,64,15),("q2-20k",2,20000,64,20)]
    cases += [(f"publishers-{n}",1,20000,n,10) for n in (1,100,1000)]
    cases += [("overload-50k",1,50000,64,10),("profile-30k",1,30000,64,15)]
    for name,qos,rate,connections,duration in cases:
        command = [sys.executable,str(ROOT/"scripts/perf/event_load.py"),"--server-bin",args.server_bin,
                   "--loadgen-bin",args.loadgen_bin,"--rate",str(rate),"--qos",str(qos),
                   "--connections",str(connections),"--duration",str(duration),"--warmup","5","--sink-mode","none"]
        if name.startswith("profile"):
            command += ["--sample-output",str(output/"profile.sample.txt"),"--sample-seconds","10"]
        print(name,flush=True)
        with (output/f"{name}.json").open("w") as result:
            subprocess.run(command,stdout=result,check=True,cwd=ROOT,timeout=120)
    with (output/"fairness.json").open("w") as result:
        subprocess.run([sys.executable,str(ROOT/"scripts/perf/mixed_load.py"),"--scenario","route-fairness",
            "--duration","15","--warmup","5","--cooldown","2","--sample-every","1",
            "--server-bin",args.server_bin,"--loadgen-bin",args.loadgen_bin],stdout=result,check=True,cwd=ROOT,timeout=120)

if __name__ == "__main__":
    main()
