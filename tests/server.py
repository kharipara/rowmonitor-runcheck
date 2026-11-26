"""Local HTTP server for CLI tests. Not a dependency of the package."""

from __future__ import annotations

import json
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

Dispatch = Callable[[BaseHTTPRequestHandler, str, bytes], None]


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: AppServer

    def log_message(self, format: str, *args: object) -> None:
        return

    def do_GET(self) -> None:
        self.server.dispatch(self, "GET", b"")

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length else b""
        self.server.dispatch(self, "POST", body)


class AppServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, dispatch: Dispatch) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self._dispatch = dispatch
        self.requests: list[dict[str, str]] = []
        self._stopped = False
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)

    def dispatch(self, request: BaseHTTPRequestHandler, method: str, body: bytes) -> None:
        parsed = urlsplit(request.path)
        self.requests.append(
            {
                "method": method,
                "path": parsed.path,
                "query": parsed.query,
                "authorization": request.headers.get("Authorization") or "",
                "body": body.decode("utf-8", errors="replace"),
            }
        )
        self._dispatch(request, method, body)

    def start(self) -> str:
        self._thread.start()
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.shutdown()
        self.server_close()


def send_json(request: BaseHTTPRequestHandler, status: int, payload: object) -> None:
    raw = json.dumps(payload).encode("utf-8")
    request.send_response(status)
    request.send_header("Content-Type", "application/json")
    request.send_header("Content-Length", str(len(raw)))
    request.end_headers()
    request.wfile.write(raw)
