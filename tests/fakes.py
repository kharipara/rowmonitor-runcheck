"""In-process Airflow API used by CLI tests."""

from __future__ import annotations

import base64
import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, unquote, urlsplit

from tests.server import send_json


class AirflowFake:
    def __init__(
        self,
        *,
        version: str = "v2",
        dags: dict[str, object] | None = None,
        username: str = "runcheck-user",
        password: str = "s3cret-password",
        token: str = "jwt-token-value",
        accept_password: bool = True,
        webhook_status: int = 200,
    ) -> None:
        self.version = version
        self.dags = dags or {}
        self.username = username
        self.password = password
        self.token = token
        self.accept_password = accept_password
        self.webhook_status = webhook_status

    def __call__(self, request: BaseHTTPRequestHandler, method: str, body: bytes) -> None:
        path = urlsplit(request.path).path
        query = parse_qs(urlsplit(request.path).query)
        if path == "/api/v2/version":
            if self.version == "v2":
                send_json(request, 200, {"version": "3.4.0", "git_version": None})
            else:
                send_json(request, 404, {"detail": "Not Found"})
            return
        if path == "/api/v1/version":
            if self.version != "v1":
                send_json(request, 404, {"detail": "Not Found"})
                return
            if not request.headers.get("Authorization"):
                send_json(request, 401, {"detail": "unauthorized"})
                return
            send_json(request, 200, {"version": "2.10.5", "git_version": None})
            return
        if path == "/auth/token" and method == "POST":
            self._token(request, body)
            return
        if path.startswith("/hooks/"):
            send_json(request, self.webhook_status, {"ok": self.webhook_status < 300})
            return
        parts = path.split("/")
        # /api/{version}/dags/{dag_id}/dagRuns
        if len(parts) == 6 and parts[1] == "api" and parts[3] == "dags" and parts[5] == "dagRuns":
            self._dag_runs(request, parts[2], unquote(parts[4]), query)
            return
        send_json(request, 404, {"detail": "unexpected path"})

    def _token(self, request: BaseHTTPRequestHandler, body: bytes) -> None:
        try:
            data = json.loads(body.decode() or "{}")
        except json.JSONDecodeError:
            send_json(request, 400, {"detail": "bad json"})
            return
        if (
            not self.accept_password
            or data.get("username") != self.username
            or data.get("password") != self.password
        ):
            send_json(request, 401, {"detail": self.password})
            return
        send_json(request, 201, {"access_token": self.token})

    def _dag_runs(
        self,
        request: BaseHTTPRequestHandler,
        api_version: str,
        dag_id: str,
        query: dict[str, list[str]],
    ) -> None:
        if not self._authorized(request, api_version):
            send_json(request, 401, {"detail": self.password})
            return
        spec = self.dags.get(dag_id)
        if spec is None:
            send_json(
                request,
                404,
                {"detail": f"DAG {dag_id} not found", "status": 404, "title": "DAG not found"},
            )
            return
        if isinstance(spec, int):
            send_json(request, spec, {"detail": "server error"})
            return
        if not isinstance(spec, dict):
            send_json(request, 500, {"detail": "bad fixture"})
            return
        state = (query.get("state") or [None])[0]
        payload = spec["success"] if state == "success" else spec["latest"]
        send_json(request, 200, payload)

    def _authorized(self, request: BaseHTTPRequestHandler, api_version: str) -> bool:
        header = request.headers.get("Authorization") or ""
        if api_version == "v1":
            expected = (
                "Basic " + base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
            )
            return header == expected
        return header == f"Bearer {self.token}"


def collection(dag_id: str, state: str, run_id: str, start: str | None, end: str | None) -> dict:
    return {
        "dag_runs": [
            {
                "dag_id": dag_id,
                "dag_run_id": run_id,
                "logical_date": start,
                "start_date": start,
                "end_date": end,
                "state": state,
            }
        ],
        "total_entries": 1,
    }


def empty_collection() -> dict:
    return {"dag_runs": [], "total_entries": 0}
