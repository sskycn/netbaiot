#!/usr/bin/env python3
"""Run a CI command and expose its failure tail as a GitHub Check annotation."""

from __future__ import annotations

import argparse
import collections
import subprocess
import sys


MAX_ANNOTATION_CHARS = 3_200
TAIL_LINES = 160


def escape_workflow_data(value: str) -> str:
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--title", required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    command = arguments.command
    if command[:1] == ["--"]:
        command = command[1:]
    if not command:
        parser.error("a command is required after --")

    tail: collections.deque[str] = collections.deque(maxlen=TAIL_LINES)
    failures: collections.deque[str] = collections.deque(maxlen=20)
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            bufsize=1,
        )
    except OSError as error:
        message = f"failed to start command: {error}"
        print(message, file=sys.stderr)
        print(f"::error title={arguments.title}::{escape_workflow_data(message)}")
        return 127

    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="", flush=True)
        tail.append(line)
        if ": FAIL" in line:
            failures.append(line)
    return_code = process.wait()
    if return_code == 0:
        return 0

    # GitHub truncates long check annotations from the end. Put explicit test
    # failures first so they remain visible even when command output is large.
    failure_details = "".join(failures)
    if len(failure_details) >= MAX_ANNOTATION_CHARS:
        details = failure_details[:MAX_ANNOTATION_CHARS]
    else:
        details = failure_details + "".join(tail)[
            -(MAX_ANNOTATION_CHARS - len(failure_details)) :
        ]
    message = f"command exited with {return_code}\n{details}"
    print(f"::error title={arguments.title}::{escape_workflow_data(message)}")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
