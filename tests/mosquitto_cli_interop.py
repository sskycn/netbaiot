#!/usr/bin/env python3
"""Real MQTT 3.1.1 matrix using only mosquitto_pub/sub against NetbaIoT."""

import argparse
from collections import deque
import json
import os
import shutil
import subprocess
import sys
import time


PASSWORD = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
ROOT = "v1/t/demo/p/sensor/d/device-1"
TOPIC = f"{ROOT}/up"
VERIFY_TOPIC = f"{ROOT}/down"
MOSQUITTO_PUB = (
    os.environ.get("NETBAIOT_MOSQUITTO_PUB")
    or shutil.which("mosquitto_pub")
    or "/usr/local/bin/mosquitto_pub"
)
MOSQUITTO_SUB = (
    os.environ.get("NETBAIOT_MOSQUITTO_SUB")
    or shutil.which("mosquitto_sub")
    or "/usr/local/bin/mosquitto_sub"
)


def tool_version(executable):
    completed = subprocess.run(
        [executable, "--help"], capture_output=True, text=True, timeout=5
    )
    lines = (completed.stdout + completed.stderr).splitlines()
    return next((line.strip() for line in lines if " version " in line), lines[0].strip())


def redacted_command(command):
    output = []
    redact_next = False
    for argument in command:
        if redact_next:
            output.append("<redacted>")
            redact_next = False
        else:
            output.append(argument)
            redact_next = argument == "-P"
    return output


def record_stage(diagnostics, stage, command, completed):
    diagnostics.append(
        {
            "stage": stage,
            "command": redacted_command(command),
            "returncode": completed.returncode,
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
    )


def event(source, sequence):
    return json.dumps(
        {
            "schema_version": 1,
            "source_message_id": source,
            "kind": "heartbeat",
            "data": {"sequence": sequence},
        },
        separators=(",", ":"),
    )


class Matrix:
    def __init__(self, host, port):
        self.base = ["-h", host, "-p", str(port), "-V", "mqttv311", "-u", "demo-device", "-P", PASSWORD]
        self.sequence = 0
        self.clients = 0
        self.last_command = []
        self.packet_times = deque()

    def pace_protocol_packets(self):
        # The development broker admits 16 data packets per device per second.
        # This matrix checks interoperability, while dedicated tests check the
        # limit itself. Keep CLI SUBSCRIBE/PUBLISH bursts below that budget.
        while True:
            now = time.monotonic()
            while self.packet_times and now - self.packet_times[0] >= 1.0:
                self.packet_times.popleft()
            if len(self.packet_times) < 10:
                self.packet_times.append(now)
                return
            time.sleep(max(0.001, 1.01 - (now - self.packet_times[0])))

    def publish(self, topic=TOPIC, qos=1, payload=None, retain=False, check=True, extra=()):
        self.pace_protocol_packets()
        self.sequence += 1
        payload = payload if payload is not None else event(f"mosq-{self.sequence}", self.sequence)
        command = [MOSQUITTO_PUB, *self.base, "-i", f"mosq-pub-{self.sequence}", "-t", topic, "-q", str(qos), "-m", payload, *extra]
        if retain:
            command.append("-r")
        self.last_command = command
        completed = subprocess.run(command, capture_output=True, text=True, timeout=8)
        if check and completed.returncode != 0:
            raise AssertionError(
                f"Mosquitto publish failed: {redacted_command(command)}; "
                f"exit={completed.returncode}; stderr={completed.stderr}"
            )
        return completed

    def subscriber(self, topic, qos=2, count=1, client_id=None, persistent=False, extra=()):
        self.pace_protocol_packets()
        self.clients += 1
        client_id = client_id or f"mosq-sub-{self.clients}"
        command = [MOSQUITTO_SUB, *self.base, "-t", topic, "-q", str(qos), "-C", str(count), "-W", "5", "-N", *extra]
        command.extend(["-i", client_id])
        if persistent:
            command.append("-c")
        return subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def receive(self, subscriber, expected):
        stdout, stderr = subscriber.communicate(timeout=8)
        assert subscriber.returncode == 0, (stdout, stderr)
        lines = stdout.splitlines()
        assert lines == expected, (lines, expected, stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    args = parser.parse_args()
    matrix = Matrix(args.host, args.port)
    results = {}
    versions = {
        "mosquitto_pub": tool_version(MOSQUITTO_PUB),
        "mosquitto_sub": tool_version(MOSQUITTO_SUB),
    }

    matrix.publish(qos=1)
    bad = subprocess.run(
        [MOSQUITTO_PUB, "-h", args.host, "-p", str(args.port), "-V", "mqttv311", "-u", "demo-device", "-P", "wrong", "-i", "mosq-bad-auth", "-t", TOPIC, "-q", "1", "-m", event("bad-auth", 0)],
        capture_output=True,
        text=True,
        timeout=8,
    )
    assert bad.returncode != 0
    results["auth_success_and_bad_password"] = "pass"

    payloads = [event(f"mosq-qos-{qos}", 10 + qos) for qos in (0, 1, 2)]
    for qos, payload in zip((0, 1, 2), payloads):
        matrix.publish(qos=qos, payload=payload, retain=True)
        exact = matrix.subscriber(TOPIC, qos=2)
        matrix.receive(exact, [payload])
        matrix.publish(payload="", retain=True)
    results["qos0_qos1_qos2_exact"] = "pass"

    for name, topic_filter in (("plus", f"{ROOT}/+"), ("hash", f"{ROOT}/#")):
        payload = event(f"mosq-{name}", 20)
        matrix.publish(payload=payload, retain=True)
        subscriber = matrix.subscriber(topic_filter)
        matrix.receive(subscriber, [payload])
        matrix.publish(payload="", retain=True)
    results["wildcard_plus_and_hash"] = "pass"

    retained_one = event("mosq-retained-one", 30)
    retained_two = event("mosq-retained-two", 31)
    matrix.publish(payload=retained_one, retain=True)
    matrix.publish(payload=retained_two, retain=True)
    replay = matrix.subscriber(f"{ROOT}/#")
    matrix.receive(replay, [retained_two])
    matrix.publish(payload="", retain=True)
    empty = subprocess.run(
        [MOSQUITTO_SUB, *matrix.base, "-t", f"{ROOT}/#", "-q", "1", "-W", "1", "-N"],
        capture_output=True,
        text=True,
        timeout=4,
    )
    assert not empty.stdout
    results["retained_replace_wildcard_replay_delete"] = "pass"

    persistent_id = "mosq-persistent"
    initial = subprocess.run(
        [MOSQUITTO_SUB, *matrix.base, "-c", "-i", persistent_id, "-t", TOPIC, "-q", "2", "-E"],
        capture_output=True,
        text=True,
        timeout=8,
    )
    assert initial.returncode == 0, initial.stderr
    offline_one = event("mosq-offline-qos1", 40)
    offline_two = event("mosq-offline-qos2", 41)
    matrix.publish(qos=1, payload=offline_one)
    matrix.publish(qos=2, payload=offline_two)
    resumed = matrix.subscriber(TOPIC, count=2, client_id=persistent_id, persistent=True, extra=("-d",))
    stdout, stderr = resumed.communicate(timeout=8)
    assert resumed.returncode == 0, stderr
    assert offline_one in stdout and offline_two in stdout, (stdout, stderr)
    results["clean_session_0_session_present_offline_qos1_qos2"] = "pass"

    unsub_id = "mosq-unsubscribe"
    unsubscribe_diagnostics = []
    try:
        initial_command = [
            MOSQUITTO_SUB,
            *matrix.base,
            "-c",
            "-i",
            unsub_id,
            "-t",
            TOPIC,
            "-q",
            "1",
            "-E",
            "-d",
        ]
        initial = subprocess.run(
            initial_command, capture_output=True, text=True, timeout=8
        )
        record_stage(
            unsubscribe_diagnostics, "initial persistent subscription", initial_command, initial
        )
        assert initial.returncode == 0, initial.stderr
        assert "received SUBACK" in initial.stdout, initial.stdout

        # Mosquitto 2.0.x requires at least one -t even when -U is present. Subscribe to a
        # different allowed topic and wait for the debug trace to report UNSUBACK. The timeout
        # bounds the otherwise long-lived subscriber; it is not the correctness boundary.
        unsubscribe_command = [
            MOSQUITTO_SUB,
            *matrix.base,
            "-c",
            "-i",
            unsub_id,
            "-t",
            VERIFY_TOPIC,
            "-q",
            "1",
            "-U",
            TOPIC,
            "-W",
            "1",
            "-d",
        ]
        unsubscribed = subprocess.run(
            unsubscribe_command, capture_output=True, text=True, timeout=8
        )
        record_stage(
            unsubscribe_diagnostics, "persistent unsubscribe", unsubscribe_command, unsubscribed
        )
        assert "sending UNSUBSCRIBE" in unsubscribed.stdout, unsubscribed.stdout
        assert "received UNSUBACK" in unsubscribed.stdout, unsubscribed.stdout

        post_unsubscribe_payload = event("mosq-after-unsubscribe", 50)
        published = matrix.publish(payload=post_unsubscribe_payload)
        record_stage(
            unsubscribe_diagnostics,
            "post-unsubscribe publish",
            matrix.last_command,
            published,
        )
        assert published.returncode == 0, published.stderr

        verify_command = [
            MOSQUITTO_SUB,
            *matrix.base,
            "-c",
            "-i",
            unsub_id,
            "-t",
            VERIFY_TOPIC,
            "-q",
            "1",
            "-E",
            "-d",
        ]
        verified = subprocess.run(
            verify_command, capture_output=True, text=True, timeout=8
        )
        record_stage(
            unsubscribe_diagnostics, "resume without target resubscribe", verify_command, verified
        )
        assert verified.returncode == 0, verified.stderr
        # Mosquitto's v3.1.1 debug line prints the CONNACK return code, not the
        # Session Present bit. The raw control test asserts Session Present=1.
        assert "received CONNACK (0)" in verified.stdout, verified.stdout
        assert post_unsubscribe_payload not in verified.stdout, verified.stdout

        resubscribe_command = [
            MOSQUITTO_SUB,
            *matrix.base,
            "-c",
            "-i",
            unsub_id,
            "-t",
            TOPIC,
            "-q",
            "1",
            "-E",
            "-d",
        ]
        resubscribed = subprocess.run(
            resubscribe_command, capture_output=True, text=True, timeout=8
        )
        record_stage(
            unsubscribe_diagnostics, "explicit target resubscribe", resubscribe_command, resubscribed
        )
        assert resubscribed.returncode == 0, resubscribed.stderr
        assert "received SUBACK" in resubscribed.stdout, resubscribed.stdout

        fresh_payload = event("mosq-after-resubscribe", 51)
        published = matrix.publish(payload=fresh_payload)
        record_stage(
            unsubscribe_diagnostics,
            "post-resubscribe publish",
            matrix.last_command,
            published,
        )
        assert published.returncode == 0, published.stderr

        final_command = [
            MOSQUITTO_SUB,
            *matrix.base,
            "-c",
            "-i",
            unsub_id,
            "-t",
            VERIFY_TOPIC,
            "-q",
            "1",
            "-C",
            "1",
            "-W",
            "5",
            "-N",
        ]
        final = subprocess.run(final_command, capture_output=True, text=True, timeout=8)
        record_stage(
            unsubscribe_diagnostics, "fresh delivery after resubscribe", final_command, final
        )
        assert final.returncode == 0, final.stderr
        assert post_unsubscribe_payload not in final.stdout, final.stdout
        assert fresh_payload in final.stdout, final.stdout
    except Exception:
        print(
            json.dumps(
                {
                    "versions": versions,
                    "persistent_unsubscribe_diagnostics": unsubscribe_diagnostics,
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        raise
    results["MOSQUITTO-PERSISTENT-UNSUB-001"] = "pass"

    will_topic = TOPIC
    # The production source limiter is intentionally active during interop; start a fresh window
    # before the connection-heavy Will cases instead of treating throttling as protocol failure.
    time.sleep(1.1)
    for qos in (0, 1, 2):
        payload = event(f"mosq-will-{qos}", 60 + qos)
        writer = subprocess.Popen(
            [MOSQUITTO_PUB, *matrix.base, "-d", "-i", f"mosq-will-writer-{qos}", "-t", TOPIC, "-l", "--will-topic", will_topic, "--will-payload", payload, "--will-qos", str(qos), "--will-retain"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        writer.stdin.write(event(f"mosq-will-prime-{qos}", 80 + qos) + "\n")
        writer.stdin.flush()
        time.sleep(0.5)
        assert writer.poll() is None
        writer.kill()
        writer.wait(timeout=3)
        time.sleep(0.5)
        retained = matrix.subscriber(will_topic, qos=2)
        matrix.receive(retained, [payload])
        matrix.publish(topic=will_topic, payload="", retain=True)
        time.sleep(0.4)

    normal_payload = event("mosq-normal-disconnect-will", 70)
    normal = subprocess.run(
        [MOSQUITTO_PUB, *matrix.base, "-t", TOPIC, "-m", event("mosq-normal", 71), "--will-topic", will_topic, "--will-payload", normal_payload, "--will-qos", "2", "--will-retain"],
        capture_output=True,
        text=True,
        timeout=8,
    )
    assert normal.returncode == 0, normal.stderr
    no_will = subprocess.run(
        [MOSQUITTO_SUB, *matrix.base, "-t", will_topic, "-q", "2", "-W", "1", "-N"],
        capture_output=True,
        text=True,
        timeout=4,
    )
    assert not no_will.stdout
    results["will_qos0_qos1_qos2_retained_and_normal_suppression"] = "pass"

    print(json.dumps({"versions": versions, "results": results}, indent=2))
    print(
        "MOSQUITTO-PERSISTENT-UNSUB-001: PASS — "
        f"{versions['mosquitto_sub']}; target unsubscribe persisted without stale delivery"
    )


if __name__ == "__main__":
    main()
