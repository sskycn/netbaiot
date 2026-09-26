#!/usr/bin/env python3
"""Experiment only: measure loopback socket admission under send-buffer options."""
import json
import platform
import selectors
import socket
import threading
import time


def probe(label, send_buffer=None, notsent=None):
    result = {"case": label, "requested_send_buffer": send_buffer,
              "requested_tcp_notsent_lowat": notsent}
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    sender = socket.socket()
    try:
        sender.connect(listener.getsockname())
        receiver, _ = listener.accept()
        with receiver:
            receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            if send_buffer is not None:
                sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buffer)
            result["actual_send_buffer"] = sender.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)
            if notsent is not None:
                option = getattr(socket, "TCP_NOTSENT_LOWAT", None)
                if option is None:
                    result["status"] = "NOT SUPPORTED"
                    return result
                try:
                    sender.setsockopt(socket.IPPROTO_TCP, option, notsent)
                    result["actual_tcp_notsent_lowat"] = sender.getsockopt(socket.IPPROTO_TCP, option)
                except OSError as error:
                    result["status"] = f"NOT SUPPORTED: {error}"
                    return result

            consumed = [0]
            def drain():
                time.sleep(0.2)
                receiver.settimeout(0.25)
                until = time.monotonic() + 1.3
                while time.monotonic() < until:
                    try:
                        consumed[0] += len(receiver.recv(4096))
                    except (OSError, socket.timeout):
                        break
                    time.sleep(0.125)
            thread = threading.Thread(target=drain)
            thread.start()
            sender.setblocking(False)
            selector = selectors.DefaultSelector()
            selector.register(sender, selectors.EVENT_WRITE)
            payload = bytes(16 * 1024)
            accepted = 0
            started = time.monotonic()
            result["accepted_before_receiver_read"] = None
            try:
                while time.monotonic() - started < 1.5 and accepted < 1024 * 1024:
                    if not selector.select(timeout=0.01):
                        if result["accepted_before_receiver_read"] is None and time.monotonic() - started >= 0.2:
                            result["accepted_before_receiver_read"] = accepted
                        continue
                    try:
                        accepted += sender.send(payload[:min(len(payload), 1024 * 1024 - accepted)])
                    except BlockingIOError:
                        pass
                    if result["accepted_before_receiver_read"] is None and time.monotonic() - started >= 0.2:
                        result["accepted_before_receiver_read"] = accepted
            finally:
                selector.close()
                thread.join(timeout=2)
            result["accepted_after_1_5s"] = accepted
            result["receiver_consumed"] = consumed[0]
            result["status"] = "PASS"
            return result
    finally:
        sender.close()
        listener.close()


def main():
    cases = [("default", None, None), ("small_sndbuf", 4096, None)]
    cases += [(f"notsent_{value}", None, value) for value in (8192, 16384, 32768)]
    print(json.dumps({"platform": platform.platform(), "arch": platform.machine(),
                      "receiver_read_delay_ms": 200, "receiver_read_bytes": 4096,
                      "receiver_read_interval_ms": 125,
                      "cases": [probe(*case) for case in cases]}, indent=2))


if __name__ == "__main__":
    main()
