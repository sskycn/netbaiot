"""Mosquitto CLI MQTT 5 interoperability against the embedded broker."""

from __future__ import annotations

import json
import pathlib
import shutil
import subprocess
import tempfile
import time

from common import PASSWORD, TOPIC_A, USERNAME_A, start_netbaiot


def main() -> None:
    publisher = shutil.which("mosquitto_pub")
    subscriber = shutil.which("mosquitto_sub")
    if not publisher or not subscriber:
        raise RuntimeError("mosquitto_pub and mosquitto_sub are required")
    with tempfile.TemporaryDirectory(prefix="netbaiot-mqtt5-mosquitto-") as temporary:
        broker = start_netbaiot(pathlib.Path(temporary))
        try:
            base = ["-h", "127.0.0.1", "-p", str(broker.port), "-V", "mqttv5",
                    "-u", USERNAME_A, "-P", PASSWORD]

            def message(sequence: int) -> str:
                return json.dumps({"schema_version": 1, "source_message_id": f"v5-mosq-{sequence}",
                                   "kind": "heartbeat", "data": {"sequence": sequence}}, separators=(",", ":"))

            def pub(sequence: int, qos: int, *, retained: bool = False, payload: str | None = None) -> None:
                args = [publisher, *base, "-i", f"v5-pub-{sequence}", "-t", TOPIC_A,
                        "-q", str(qos), "-m", message(sequence) if payload is None else payload]
                if retained:
                    args.append("-r")
                subprocess.run(args, check=True, capture_output=True, text=True, timeout=8)

            for qos in (0, 1, 2):
                pub(100 + qos, qos, retained=True)
                received = subprocess.run([subscriber, *base, "-i", f"v5-sub-{qos}",
                    "-t", TOPIC_A, "-q", "2", "-C", "1", "-W", "4", "-N"],
                    check=True, capture_output=True, text=True, timeout=8)
                assert received.stdout == message(100 + qos), (qos, received.stdout, received.stderr)
                pub(200 + qos, 1, retained=True, payload="")

            persistent = "v5-persistent-cli"
            subprocess.run([subscriber, *base, "-c", "-x", "60", "-i", persistent,
                "-t", TOPIC_A, "-q", "1", "-E"], check=True, capture_output=True, text=True, timeout=8)
            pub(300, 1)
            received = subprocess.run([subscriber, *base, "-c", "-x", "60", "-i", persistent,
                "-t", TOPIC_A, "-q", "1", "-C", "1", "-W", "4", "-N"],
                check=True, capture_output=True, text=True, timeout=8)
            assert received.stdout == message(300), received.stderr

            subprocess.run([publisher, *base, "-i", "v5-expiring-cli", "-t", TOPIC_A,
                "-q", "1", "-r", "-m", message(400), "-D", "PUBLISH",
                "message-expiry-interval", "1"], check=True,
                capture_output=True, text=True, timeout=8)
            time.sleep(2.1)
            expired = subprocess.run([subscriber, *base, "-i", "v5-expired-cli",
                "-t", TOPIC_A, "-q", "1", "-C", "1", "-W", "2", "-N"],
                capture_output=True, text=True, timeout=5)
            assert not expired.stdout, expired.stdout

            will = subprocess.Popen([subscriber, *base, "-c", "-x", "60", "-i", "v5-will-cli",
                "-t", TOPIC_A, "-q", "1", "-d", "--will-topic", TOPIC_A,
                "--will-payload", message(401), "--will-qos", "1", "--will-retain",
                "-D", "WILL", "will-delay-interval", "1"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                time.sleep(0.5)
                assert will.poll() is None, f"Mosquitto Will client exited: {will.communicate()}"
                will.kill()
                will.wait(timeout=3)
            finally:
                if will.poll() is None:
                    will.kill()
                    will.wait(timeout=3)
                will.stderr.close()
                will.stdout.close()
            time.sleep(2.1)
            received = subprocess.run([subscriber, *base, "-i", "v5-will-observer-cli",
                "-t", TOPIC_A, "-q", "1", "-C", "1", "-W", "4", "-N"],
                check=True, capture_output=True, text=True, timeout=8)
            assert received.stdout == message(401), received.stderr
            print("Mosquitto mqttv5 QoS0/1/2, retained, offline, expiry and delayed Will: PASS")
        finally:
            broker.stop()


if __name__ == "__main__":
    main()
