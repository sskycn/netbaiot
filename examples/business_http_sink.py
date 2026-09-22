#!/usr/bin/env python3
"""Dependency-free tutorial webhook: print, deduplicate, then acknowledge events."""

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class EventHandler(BaseHTTPRequestHandler):
    seen = set()

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler API
        if self.path != "/events":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > 1_048_576:
                raise ValueError("invalid body length")
            event = json.loads(self.rfile.read(length))
            event_id = event["event_id"]
        except (KeyError, ValueError, json.JSONDecodeError):
            self.send_error(400)
            return

        duplicate = event_id in self.seen
        if not duplicate:
            # A real consumer commits its business update and event_id atomically here.
            self.seen.add(event_id)
        print(json.dumps({"duplicate": duplicate, "event": event}, ensure_ascii=False), flush=True)
        self.send_response(204)  # Any 2xx is ConsumerAccepted.
        self.end_headers()

    def log_message(self, format, *args):  # noqa: A002 - inherited API
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.listen, args.port), EventHandler)
    print(f"business webhook listening on http://{args.listen}:{args.port}/events", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
