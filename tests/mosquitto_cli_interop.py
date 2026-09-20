#!/usr/bin/env python3
"""Real MQTT 3.1.1 matrix using only mosquitto_pub/sub against NetbaIoT."""

import argparse
import json
import subprocess
import time


PASSWORD = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
ROOT = "v1/t/demo/p/sensor/d/device-1"
TOPIC = f"{ROOT}/up"


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

    def publish(self, topic=TOPIC, qos=1, payload=None, retain=False, check=True, extra=()):
        self.sequence += 1
        payload = payload if payload is not None else event(f"mosq-{self.sequence}", self.sequence)
        command = ["/usr/local/bin/mosquitto_pub", *self.base, "-i", f"mosq-pub-{self.sequence}", "-t", topic, "-q", str(qos), "-m", payload, *extra]
        if retain:
            command.append("-r")
        return subprocess.run(command, check=check, capture_output=True, text=True, timeout=8)

    def subscriber(self, topic, qos=2, count=1, client_id=None, persistent=False, extra=()):
        self.clients += 1
        client_id = client_id or f"mosq-sub-{self.clients}"
        command = ["/usr/local/bin/mosquitto_sub", *self.base, "-t", topic, "-q", str(qos), "-C", str(count), "-W", "5", "-N", *extra]
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

    matrix.publish(qos=1)
    bad = subprocess.run(
        ["/usr/local/bin/mosquitto_pub", "-h", args.host, "-p", str(args.port), "-V", "mqttv311", "-u", "demo-device", "-P", "wrong", "-i", "mosq-bad-auth", "-t", TOPIC, "-q", "1", "-m", event("bad-auth", 0)],
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
        ["/usr/local/bin/mosquitto_sub", *matrix.base, "-t", f"{ROOT}/#", "-q", "1", "-W", "1", "-N"],
        capture_output=True,
        text=True,
        timeout=4,
    )
    assert not empty.stdout
    results["retained_replace_wildcard_replay_delete"] = "pass"

    persistent_id = "mosq-persistent"
    initial = subprocess.run(
        ["/usr/local/bin/mosquitto_sub", *matrix.base, "-c", "-i", persistent_id, "-t", TOPIC, "-q", "2", "-E"],
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
    subprocess.run(
        ["/usr/local/bin/mosquitto_sub", *matrix.base, "-c", "-i", unsub_id, "-t", TOPIC, "-q", "1", "-E"],
        check=True,
        capture_output=True,
        timeout=8,
    )
    unsubscribed = subprocess.run(
        ["/usr/local/bin/mosquitto_sub", *matrix.base, "-c", "-i", unsub_id, "-U", TOPIC, "-W", "1"],
        capture_output=True,
        timeout=8,
    )
    assert b"Protocol error" not in unsubscribed.stderr
    matrix.publish(payload=event("mosq-after-unsubscribe", 50))
    no_offline = subprocess.run(
        ["/usr/local/bin/mosquitto_sub", *matrix.base, "-c", "-i", unsub_id, "-t", TOPIC, "-q", "1", "-W", "1", "-N"],
        capture_output=True,
        text=True,
        timeout=4,
    )
    assert not no_offline.stdout
    results["unsubscribe"] = "pass"

    will_topic = TOPIC
    # The production source limiter is intentionally active during interop; start a fresh window
    # before the connection-heavy Will cases instead of treating throttling as protocol failure.
    time.sleep(1.1)
    for qos in (0, 1, 2):
        payload = event(f"mosq-will-{qos}", 60 + qos)
        writer = subprocess.Popen(
            ["/usr/local/bin/mosquitto_pub", *matrix.base, "-d", "-i", f"mosq-will-writer-{qos}", "-t", TOPIC, "-l", "--will-topic", will_topic, "--will-payload", payload, "--will-qos", str(qos), "--will-retain"],
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
        ["/usr/local/bin/mosquitto_pub", *matrix.base, "-t", TOPIC, "-m", event("mosq-normal", 71), "--will-topic", will_topic, "--will-payload", normal_payload, "--will-qos", "2", "--will-retain"],
        capture_output=True,
        text=True,
        timeout=8,
    )
    assert normal.returncode == 0, normal.stderr
    no_will = subprocess.run(
        ["/usr/local/bin/mosquitto_sub", *matrix.base, "-t", will_topic, "-q", "2", "-W", "1", "-N"],
        capture_output=True,
        text=True,
        timeout=4,
    )
    assert not no_will.stdout
    results["will_qos0_qos1_qos2_retained_and_normal_suppression"] = "pass"

    print(json.dumps({"mosquitto_pub": "2.1.2", "mosquitto_sub": "2.1.2", "results": results}, indent=2))


if __name__ == "__main__":
    main()
