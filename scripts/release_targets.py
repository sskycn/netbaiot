#!/usr/bin/env python3
"""One release target policy shared by packaging, xtask and Actions matrices."""
import argparse
import json
import os
from pathlib import Path

POLICY = json.loads(Path(__file__).with_suffix(".json").read_text(encoding="utf-8"))
REQUIRED_TARGETS = tuple(item["target"] for item in POLICY["required"])
OPTIONAL_TARGETS = tuple(item["target"] for item in POLICY["optional"])
TARGETS = REQUIRED_TARGETS + OPTIONAL_TARGETS
DETAILS = {item["target"]: item for group in POLICY.values() for item in group}
if len(DETAILS) != len(TARGETS) or not REQUIRED_TARGETS or any(
    DETAILS[target]["format"] != "elf" for target in REQUIRED_TARGETS
):
    raise ValueError("invalid release target policy")

def matrix(group, platform="all"):
    items = POLICY[group]
    if platform != "all":
        family = "macho" if platform == "macos" else "pe"
        items = [item for item in items if item["format"] == family]
    return {"include": [{"target": item["target"], "runner": item["runner"]} for item in items]}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", choices=("required", "optional"), default="required")
    parser.add_argument("--platform", choices=("all", "macos", "windows"), default="all")
    parser.add_argument("--check-target")
    parser.add_argument("--tag", default="")
    parser.add_argument("--github-output", action="store_true")
    args = parser.parse_args()
    if args.group == "required" and args.platform != "all":
        parser.error("required Linux targets cannot be filtered by an optional platform")
    if args.check_target:
        if args.check_target not in TARGETS:
            parser.error(f"unsupported package target: {args.check_target}")
    elif args.github_output:
        from release_preflight import ROOT, check_tag, workspace_version
        tag = args.tag or f"v{workspace_version(ROOT)}"
        check_tag(tag, workspace_version(ROOT))
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"tag={tag}\nmatrix={json.dumps(matrix(args.group, args.platform), separators=(',', ':'))}\n")
    else:
        print(json.dumps(matrix(args.group, args.platform), separators=(",", ":")))

if __name__ == "__main__":
    main()
