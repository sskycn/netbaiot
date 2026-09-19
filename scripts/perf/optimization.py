#!/usr/bin/env python3
"""Focused retained-load cases for the Admission/persistence optimization."""
import sys
from matrix import execute


def case(name, rate=0, duration=60, **extra):
    load = dict(
        connections=100,
        tenant_width=16,
        publish_rate=rate,
        qos=1,
        duration_secs=duration,
        warmup_secs=10,
        cooldown_secs=10,
        ramp_per_sec=20,
        report_every_secs=10,
    )
    load.update(extra.pop("load", {}))
    sample_secs = extra.pop("sample_secs", 2)
    cooldown_secs = extra.pop("cooldown_secs", 10)
    return execute(
        dict(
            name=name,
            load=load,
            sample_secs=sample_secs,
            cooldown_secs=cooldown_secs,
            **extra,
        )
    )


def capacity():
    for qos in (0, 1):
        for rate in (100, 250, 500, 1000):
            result = case(f"opt_cap_final_q{qos}_{rate}", rate=rate, load=dict(qos=qos))
            final = result.get("generator", [{}])[-1].get("stats", {}).get("counters", {})
            if final.get("client_errors", 0) or final.get("accepted", 0) < final.get("published", 1) * .99:
                break
    for rate in (10, 25, 50, 100):
        result = case(
            f"opt_cap_final_commands_{rate}",
            duration=60,
            load=dict(publish_rate=0, command_rate=rate, command_concurrency=8),
        )
        final = result.get("generator", [{}])[-1].get("stats", {}).get("counters", {})
        if final.get("client_errors", 0) or final.get("command_acks", 0) != final.get("commands_queued", -1):
            break


def arrival():
    for rate in (500, 1000):
        case(
            f"opt_after_burst_{rate}",
            duration=1,
            load=dict(phases=[dict(seconds=1, rate=rate)], retry_connections=True),
        )
    case(
        "opt_after_ramp",
        duration=100,
        load=dict(
            phases=[dict(seconds=20, rate=rate) for rate in (20, 50, 100, 250, 500)],
            retry_connections=True,
        ),
    )
    case(
        "opt_after_overload_recovery",
        duration=100,
        load=dict(
            phases=[
                dict(seconds=30, rate=100),
                dict(seconds=30, rate=2000),
                dict(seconds=40, rate=100),
            ],
            retry_connections=True,
        ),
    )


def recovery():
    case(
        "opt_after_db_recovery",
        rate=50,
        duration=90,
        rust_log="warn",
        load=dict(
            phases=[dict(seconds=25, rate=50), dict(seconds=20, rate=50), dict(seconds=45, rate=50)],
            retry_connections=True,
        ),
        events=[dict(at=30, kind="db_unavailable"), dict(at=42, kind="db_available")],
        extra_loads=[
            dict(
                transport="http",
                offset=112,
                connections=8,
                publish_rate=8,
                duration_secs=90,
                warmup_secs=10,
                cooldown_secs=10,
                retry_connections=True,
            )
        ],
    )


def fairness():
    healthy = dict(
        offset=64,
        connections=100,
        publish_rate=100,
        command_rate=2,
        command_concurrency=2,
        duration_secs=90,
        warmup_secs=10,
        cooldown_secs=10,
        ramp_per_sec=20,
        retry_connections=True,
    )
    for slow in (1, 10):
        case(
            f"opt_after_slow_consumers_{slow}",
            duration=90,
            load=dict(
                connections=slow,
                slow_fraction=1,
                publish_rate=0,
                command_rate=slow * 5,
                command_concurrency=8,
                command_padding=256,
            ),
            extra_loads=[healthy],
        )
    case(
        "opt_after_tenant_fairness",
        rate=1000,
        duration=90,
        load=dict(connections=16, tenant_width=16, retry_connections=True),
        extra_loads=[healthy],
        limits=dict(messages_per_device_second=64, messages_per_tenant_second=128),
    )


def retention():
    for indexed in (False, True):
        case(
            f"opt_after_retention_{'index' if indexed else 'baseline'}",
            rate=100,
            duration=180,
            preload=10_000,
            preload_commands=True,
            preload_age_ms=300_000,
            retention_index=indexed,
        )


def controlled_soak():
    case("opt_soak_telemetry", rate=5, duration=900)
    case(
        "opt_soak_commands",
        duration=600,
        load=dict(publish_rate=0, command_rate=1 / 30, command_concurrency=2),
    )
    case(
        "opt_soak_mixed",
        rate=5,
        duration=900,
        load=dict(command_rate=1 / 30, command_concurrency=2),
    )
    case(
        "opt_soak_reconnect",
        rate=5,
        duration=900,
        load=dict(reconnect_every_secs=30, reconnect_fraction=.1, retry_connections=True),
    )


def long_soak():
    case(
        "opt_soak_4h",
        rate=5,
        duration=14_400,
        tls=True,
        sample_secs=10,
        cooldown_secs=30,
        load=dict(
            # Equal-rate phases preserve the fixed workload while making the
            # load generator retain independent first/final-hour histograms.
            phases=[dict(seconds=3_600, rate=5) for _ in range(4)],
            command_rate=1 / 30,
            command_concurrency=2,
            reconnect_every_secs=30,
            reconnect_fraction=.1,
            retry_connections=True,
            report_every_secs=30,
            heartbeat_every=10,
            cooldown_secs=30,
        ),
    )


if __name__ == "__main__":
    {
        "capacity": capacity,
        "arrival": arrival,
        "recovery": recovery,
        "fairness": fairness,
        "retention": retention,
        "controlled_soak": controlled_soak,
        "long_soak": long_soak,
    }[sys.argv[1]]()
