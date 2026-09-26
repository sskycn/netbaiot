#!/usr/bin/env python3
"""Summarize bounded V3 experiment traces without storing application payloads."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import re


ID = re.compile(r"stream_id=(\d+)")
SIZE = re.compile(r"(?:body_bytes|payload_bytes)=(\d+)")
COMPLETION = re.compile(r"completion_us=(\d+)")


def quantile(values, percentage):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * percentage / 100))]


def analyze(directory, mode):
    directory = Path(directory)
    report = json.loads((directory / f"{mode}.json").read_text())
    log = directory / f"{mode}-gateway.log"
    events = {}
    rpc = {}
    selected_frames = []
    socket_bytes = 0
    socket_writes = 0
    send_buffer_bytes = None
    event_completion_ms = []
    for line in log.read_text().splitlines():
        if "business RPC socket send buffer" in line:
            match = re.search(r"send_buffer_bytes=(\d+)", line)
            if match:
                send_buffer_bytes = int(match.group(1))
        if "business RPC socket accepted" in line:
            match = re.search(r"bytes=(\d+)", line)
            if match:
                socket_bytes += int(match.group(1))
                socket_writes += 1
        if "business RPC V3" not in line:
            continue
        stream = ID.search(line)
        if stream is None:
            continue
        stream = int(stream.group(1))
        timestamp = datetime.fromisoformat(line.split(" ", 1)[0].replace("Z", "+00:00")).timestamp()
        if "body entered scheduler" in line:
            size = int(SIZE.search(line).group(1))
            if "class=Event" in line:
                events[stream] = {"total": size, "selected": 0, "queued_at": timestamp,
                                  "first_frame": None, "last_frame": None, "competing": []}
            else:
                rpc[stream] = {"queued_at": timestamp, "first_write": None}
                for event in events.values():
                    if event["selected"] < event["total"]:
                        event["competing"].append(stream)
        if "selected frame" in line and "frame_type=Data" in line:
            selected_frames.append((timestamp, stream))
            if stream in events:
                event = events[stream]
                event["selected"] += int(SIZE.search(line).group(1))
                if event["first_frame"] is None:
                    event["first_frame"] = timestamp
                event["last_frame"] = timestamp
        if "writer completed frame" in line and "frame_type=Data" in line and stream in rpc:
            if rpc[stream]["first_write"] is None:
                rpc[stream]["first_write"] = timestamp
        if "Event delivery acknowledged" in line:
            completion = COMPLETION.search(line)
            if completion:
                event_completion_ms.append(int(completion.group(1)) / 1000)

    opportunities = [event for event in events.values() if event["competing"]]
    interleaved = 0
    preemption_ms = []
    for event in opportunities:
        first, last = event["first_frame"], event["last_frame"]
        if first is None or last is None:
            continue
        if any(first < t < last and stream in event["competing"]
               and t >= rpc[stream]["queued_at"] for t, stream in selected_frames):
            interleaved += 1
        for stream in event["competing"]:
            completed = rpc[stream]["first_write"]
            if completed is not None:
                preemption_ms.append((completed - rpc[stream]["queued_at"]) * 1000)

    trace_path = directory / f"{mode}-1-down.jsonl"
    wire_interleaved = None
    if trace_path.exists():
        frames = [json.loads(line) for line in trace_path.read_text().splitlines()]
        wire_interleaved = 0
        for stream in events:
            positions = [index for index, frame in enumerate(frames)
                         if frame["stream_id"] == stream and frame["frame_type"] == 4]
            if len(positions) >= 2 and any(frame["frame_type"] == 4
                                           and frame["stream_id"] in rpc
                                           for frame in frames[positions[0] + 1:positions[-1]]):
                wire_interleaved += 1

    counts = report["counts"]
    config = report["config"]
    metrics_path = directory / f"{mode}-metrics.txt"
    metrics = {}
    if metrics_path.exists():
        for line in metrics_path.read_text().splitlines():
            if line.startswith("netbaiot_business_rpc_v3_") and "{" not in line:
                name, value = line.split(" ", 1)
                try:
                    metrics[name[len("netbaiot_"):]] = float(value)
                except ValueError:
                    pass
    return {
        "mode": mode,
        "source": str(directory),
        "sample_count": counts["requests"],
        "auth_latency_ms": counts["latency_ms"],
        "auth_per_second": report["requests_per_second"],
        "event_acks": counts["event_acks"],
        "event_throughput_payload_bytes_per_second": counts["event_acks"] * config["event_payload_bytes"] / config["duration_secs"],
        "event_completion_ms": {"count": len(event_completion_ms),
                                "p50": quantile(event_completion_ms, 50),
                                "p95": quantile(event_completion_ms, 95)},
        "cpu_percent": report["peaks"]["gateway_cpu"]["average_workload_percent"],
        "rss_peak_kb": report["peaks"]["rss_peak_kb"],
        "event_streams": len(events),
        "competing_event_streams": len(opportunities),
        "interleaved_event_streams": interleaved,
        "wire_interleaved_event_streams": wire_interleaved,
        "preemption_ms": {"count": len(preemption_ms),
                           "p50": quantile(preemption_ms, 50),
                           "p95": quantile(preemption_ms, 95),
                           "max": max(preemption_ms) if preemption_ms else None},
        "socket_send_buffer_bytes": send_buffer_bytes,
        "socket_accepted_bytes": socket_bytes,
        "socket_write_calls": socket_writes,
        "v3_metrics": metrics,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("mode")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = analyze(args.directory, args.mode)
    encoded = json.dumps(result, indent=2)
    if args.output:
        args.output.write_text(encoded + "\n")
    else:
        print(encoded)


if __name__ == "__main__":
    main()
