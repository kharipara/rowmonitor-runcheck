#!/usr/bin/env python3
"""Record webhook POST bodies, one JSON line per request.

Example:
    python3 scripts/e2e/webhook_catcher.py --port 9099 --out /tmp/runcheck-hooks.jsonl
"""

from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def main() -> None:
    parser = argparse.ArgumentParser(description="Append POSTed webhook bodies to a file.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9099)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *log_args: object) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length else b""
            with open(args.out, "a", encoding="utf-8") as handle:
                handle.write(raw.decode("utf-8", errors="replace"))
                handle.write("\n")
            body = b'{"ok":true}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"listening on http://{args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
