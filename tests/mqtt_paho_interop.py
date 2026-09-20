#!/usr/bin/env python3
"""Paho MQTT 3.1.1 interoperability probe for a running NetbaIoT broker."""

import argparse
import importlib.metadata
import json
import queue
import threading
import time

import paho.mqtt.client as mqtt


class Client:
    def __init__(self, args, client_id, clean, will=None):
        self.connected = threading.Event()
        self.subscribed = threading.Event()
        self.unsubscribed = threading.Event()
        self.messages = queue.Queue()
        self.session_present = False
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            clean_session=clean,
            protocol=mqtt.MQTTv311,
        )
        self.client.username_pw_set(args.username, args.password)
        if will:
            payload, qos, retain = will
            self.client.will_set(args.topic, payload, qos=qos, retain=retain)
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = lambda *_: self.subscribed.set()
        self.client.on_unsubscribe = lambda *_: self.unsubscribed.set()
        self.client.on_message = lambda _, __, message: self.messages.put(message)
        self.client.connect(args.host, args.port, keepalive=10)
        self.client.loop_start()
        assert self.connected.wait(5), "CONNECT timed out"

    def _on_connect(self, _, __, flags, reason_code, ___):
        assert not reason_code.is_failure, reason_code
        self.session_present = bool(flags.session_present)
        self.connected.set()

    def subscribe(self, topic, qos):
        self.subscribed.clear()
        result, _ = self.client.subscribe(topic, qos=qos)
        assert result == mqtt.MQTT_ERR_SUCCESS
        assert self.subscribed.wait(5), "SUBACK timed out"

    def unsubscribe(self, topic):
        self.unsubscribed.clear()
        result, _ = self.client.unsubscribe(topic)
        assert result == mqtt.MQTT_ERR_SUCCESS
        assert self.unsubscribed.wait(5), "UNSUBACK timed out"

    def publish(self, topic, payload, qos, retain=False):
        info = self.client.publish(topic, payload, qos=qos, retain=retain)
        info.wait_for_publish(5)
        assert info.is_published(), f"QoS{qos} publish did not complete"

    def message(self, timeout=5):
        return self.messages.get(timeout=timeout)

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()

    def abort(self):
        self.client._sock_close()
        time.sleep(0.2)
        self.client.loop_stop()


def event(source, sequence):
    return json.dumps(
        {
            "schema_version": 1,
            "source_message_id": source,
            "kind": "heartbeat",
            "data": {"sequence": sequence},
        },
        separators=(",", ":"),
    ).encode()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--username", default="demo-device")
    parser.add_argument(
        "--password",
        default="000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
    )
    parser.add_argument("--topic", default="v1/t/demo/p/sensor/d/device-1/up")
    parser.add_argument("--root", default="v1/t/demo/p/sensor/d/device-1")
    args = parser.parse_args()
    results = {}

    exact = Client(args, "paho-exact", True)
    exact.subscribe(args.topic, 2)
    for qos in (0, 1, 2):
        payload = event(f"paho-qos-{qos}", qos)
        exact.publish(args.topic, payload, qos)
        received = exact.message()
        assert received.payload == payload and received.qos == qos
    exact.unsubscribe(args.topic)
    results["qos0_qos1_qos2_exact_unsubscribe"] = "pass"
    exact.disconnect()

    for name, topic_filter in (("plus", f"{args.root}/+"), ("hash", f"{args.root}/#")):
        client = Client(args, f"paho-{name}", True)
        client.subscribe(topic_filter, 1)
        payload = event(f"paho-{name}", 10)
        client.publish(args.topic, payload, 1)
        assert client.message().payload == payload
        client.disconnect()
        results[f"wildcard_{name}"] = "pass"

    retained_payload = event("paho-retained", 20)
    writer = Client(args, "paho-retained-writer", True)
    writer.publish(args.topic, retained_payload, 1, retain=True)
    writer.disconnect()
    reader = Client(args, "paho-retained-reader", True)
    reader.subscribe(f"{args.root}/#", 1)
    retained = reader.message()
    assert retained.payload == retained_payload and retained.retain
    reader.publish(args.topic, b"", 1, retain=True)
    reader.message()  # live deletion publication, RETAIN is clear for an existing subscription
    reader.disconnect()
    empty = Client(args, "paho-retained-empty", True)
    empty.subscribe(f"{args.root}/#", 1)
    try:
        empty.message(0.5)
        raise AssertionError("deleted retained message was replayed")
    except queue.Empty:
        pass
    empty.disconnect()
    results["retained_store_wildcard_delete"] = "pass"

    offline = Client(args, "paho-persistent", False)
    assert not offline.session_present
    offline.subscribe(args.topic, 2)
    offline.disconnect()
    publisher = Client(args, "paho-offline-publisher", True)
    offline_qos1 = event("paho-offline-1", 31)
    offline_qos2 = event("paho-offline-2", 32)
    publisher.publish(args.topic, offline_qos1, 1)
    publisher.publish(args.topic, offline_qos2, 2)
    publisher.disconnect()
    offline = Client(args, "paho-persistent", False)
    assert offline.session_present
    observed = {offline.message().payload, offline.message().payload}
    assert observed == {offline_qos1, offline_qos2}
    offline.disconnect()
    results["persistent_session_offline_qos1_qos2"] = "pass"

    for qos in (0, 1, 2):
        abrupt_will = event(f"paho-will-abrupt-{qos}", 40 + qos)
        will_client = Client(
            args,
            f"paho-will-abrupt-{qos}",
            True,
            (abrupt_will, qos, True),
        )
        will_client.abort()
        will_reader = Client(args, f"paho-will-reader-{qos}", True)
        will_reader.subscribe(args.topic, 2)
        observed_will = will_reader.message()
        assert (
            observed_will.payload == abrupt_will
            and observed_will.qos == qos
            and observed_will.retain
        )
        will_reader.publish(args.topic, b"", 1, retain=True)
        will_reader.message()
        will_reader.disconnect()
    normal_will = event("paho-will-normal", 41)
    normal = Client(args, "paho-will-normal", True, (normal_will, 2, True))
    normal.disconnect()
    verify = Client(args, "paho-will-verify", True)
    verify.subscribe(args.topic, 2)
    try:
        verify.message(0.5)
        raise AssertionError("normal DISCONNECT published its retained Will")
    except queue.Empty:
        pass
    verify.disconnect()
    results["will_qos0_qos1_qos2_retain_and_disconnect_suppression"] = "pass"

    print(
        json.dumps(
            {
                "client": f"paho-mqtt {importlib.metadata.version('paho-mqtt')}",
                "results": results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
