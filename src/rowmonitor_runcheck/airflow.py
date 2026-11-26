"""Airflow REST last-run checks for API v1 (basic auth) and API v2 (JWT)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from urllib.parse import quote, urlsplit, urlunsplit

import requests

from rowmonitor_runcheck import __version__
from rowmonitor_runcheck.errors import AirflowAPIError, ConfigError
from rowmonitor_runcheck.problems import Problem
from rowmonitor_runcheck.timeutil import format_age, isoformat, parse_datetime

_USER_AGENT = f"rowmonitor-runcheck/{__version__}"


@dataclass(frozen=True)
class DagRun:
    dag_id: str
    dag_run_id: str
    state: str
    start: datetime | None
    end: datetime | None
    logical: datetime | None


def validate_base_url(url: str) -> str:
    parts = urlsplit(url.strip())
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ConfigError("--url must be an http or https URL")
    if parts.username or parts.password:
        raise ConfigError(
            "do not put credentials in --url; use AIRFLOW_USERNAME, "
            "AIRFLOW_PASSWORD, or AIRFLOW_TOKEN"
        )
    path = parts.path.rstrip("/")
    for suffix in ("/api/v1", "/api/v2"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    netloc = parts.hostname
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, path, "", ""))


def normalize_api_version(value: str) -> str:
    lowered = value.strip().lower()
    if lowered in {"1", "v1"}:
        return "v1"
    if lowered in {"2", "v2"}:
        return "v2"
    if lowered == "auto":
        return "auto"
    raise ConfigError("--api-version must be auto, v1, or v2")


def validate_dag_id(dag_id: str) -> str:
    if not dag_id or any(char.isspace() or ord(char) < 32 for char in dag_id):
        raise ConfigError(f"invalid DAG id: {dag_id!r}")
    return dag_id


def parse_dag_run(item: dict[str, object]) -> DagRun:
    logical_raw = item.get("logical_date")
    if logical_raw is None:
        logical_raw = item.get("execution_date")
    run_id = item.get("dag_run_id")
    if run_id is None:
        run_id = item.get("run_id")
    return DagRun(
        dag_id=str(item.get("dag_id") or ""),
        dag_run_id=str(run_id or ""),
        state=str(item.get("state") or "").strip().lower(),
        start=parse_datetime(item.get("start_date")),
        end=parse_datetime(item.get("end_date")),
        logical=parse_datetime(logical_raw),
    )


def parse_dag_run_collection(payload: dict[str, object]) -> list[DagRun]:
    runs = payload.get("dag_runs")
    if not isinstance(runs, list):
        raise ValueError("missing dag_runs")
    parsed: list[DagRun] = []
    for item in runs:
        if not isinstance(item, dict):
            raise ValueError("dag run is not an object")
        parsed.append(parse_dag_run(item))
    return parsed


def evaluate_dag(
    dag_id: str,
    latest: DagRun | None,
    last_success: DagRun | None,
    *,
    max_age: timedelta | None,
    max_age_label: str | None,
    now: datetime,
    base_url: str,
) -> list[Problem]:
    problems: list[Problem] = []
    if latest is not None and latest.state == "failed":
        problems.append(_failed_problem(dag_id, latest, base_url))
    if max_age is None:
        return problems
    if last_success is None:
        problems.append(
            Problem(
                code="no_success",
                summary=f"{dag_id}: no successful run",
                check="airflow",
                fingerprint=_fingerprint("no_success", base_url, dag_id, "", max_age_label or ""),
                details={
                    "dag_id": dag_id,
                    "max_age_seconds": int(max_age.total_seconds()),
                },
            )
        )
        return problems
    stamp = last_success.end or last_success.start or last_success.logical
    if stamp is None:
        age = None
        too_old = True
    else:
        age = now - stamp
        too_old = age.total_seconds() > max_age.total_seconds()
    if not too_old:
        return problems
    run_ref = last_success.dag_run_id or "unknown"
    if age is None:
        summary = f"{dag_id}: last success has no timestamp (run {run_ref})"
    else:
        summary = (
            f"{dag_id}: last success is {format_age(age)}, "
            f"older than {max_age_label} (run {run_ref})"
        )
    problems.append(
        Problem(
            code="success_too_old",
            summary=summary,
            check="airflow",
            fingerprint=_fingerprint(
                "success_too_old",
                base_url,
                dag_id,
                last_success.dag_run_id,
                max_age_label or "",
            ),
            details={
                "dag_id": dag_id,
                "dag_run_id": last_success.dag_run_id or None,
                "end_date": isoformat(last_success.end),
                "logical_date": isoformat(last_success.logical),
                "age_seconds": None if age is None else int(age.total_seconds()),
                "max_age_seconds": int(max_age.total_seconds()),
            },
        )
    )
    return problems


class AirflowClient:
    """Small REST client. Pass a session only from tests; the CLI uses a new one."""

    def __init__(
        self,
        base_url: str,
        api_version: str,
        username: str | None,
        password: str | None,
        token: str | None,
        timeout: float,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.api_version = api_version
        self.username = username
        self.password = password
        self.token = token
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": _USER_AGENT})
        self.resolved_version: str | None = None

    def resolve_version(self) -> str:
        if self.api_version != "auto":
            self.resolved_version = self.api_version
            return self.api_version
        v2_status = self._probe("/api/v2/version")
        if v2_status == "ok":
            self.resolved_version = "v2"
            return "v2"
        v1_status = self._probe("/api/v1/version")
        if v1_status == "ok" or (v1_status == "auth" and v2_status == "missing"):
            self.resolved_version = "v1"
            return "v1"
        if v2_status == "auth" and v1_status == "missing":
            self.resolved_version = "v2"
            return "v2"
        raise AirflowAPIError(
            "api_error",
            "could not detect the Airflow API version; pass --api-version v1 or v2",
        )

    def authenticate(self) -> None:
        version = self.resolved_version or self.api_version
        if version == "v1":
            if not self.username or not self.password:
                raise ConfigError(
                    "AIRFLOW_USERNAME and AIRFLOW_PASSWORD are required for Airflow API v1"
                )
            self.session.auth = (self.username, self.password)
            return
        if version != "v2":
            raise ConfigError("Airflow API version is not resolved")
        if self.token:
            self.session.headers["Authorization"] = f"Bearer {self.token}"
            return
        if not self.username or not self.password:
            raise ConfigError(
                "AIRFLOW_TOKEN, or AIRFLOW_USERNAME and AIRFLOW_PASSWORD, "
                "is required for Airflow API v2"
            )
        token = self._fetch_token()
        self.token = token
        self.session.headers["Authorization"] = f"Bearer {token}"

    def latest_run(self, dag_id: str) -> DagRun | None:
        payload = self._get_runs(dag_id, [("limit", "1"), ("order_by", "-start_date")])
        runs = self._parse(payload, dag_id)
        return runs[0] if runs else None

    def last_success(self, dag_id: str) -> DagRun | None:
        payload = self._get_runs(
            dag_id,
            [("limit", "1"), ("order_by", "-end_date"), ("state", "success")],
        )
        runs = [run for run in self._parse(payload, dag_id) if run.state == "success"]
        return runs[0] if runs else None

    def _probe(self, path: str) -> str:
        try:
            response = self.session.get(self.base_url + path, timeout=self.timeout)
        except requests.RequestException:
            raise AirflowAPIError("api_unreachable", "Airflow API unreachable") from None
        if response.status_code == 200:
            try:
                data = response.json()
            except ValueError:
                return "other"
            if isinstance(data, dict) and data.get("version"):
                return "ok"
            return "other"
        if response.status_code in (401, 403):
            return "auth"
        if response.status_code == 404:
            return "missing"
        return "other"

    def _fetch_token(self) -> str:
        try:
            response = self.session.post(
                self.base_url + "/auth/token",
                json={"username": self.username, "password": self.password},
                timeout=self.timeout,
                headers={"Content-Type": "application/json"},
            )
        except requests.RequestException:
            raise AirflowAPIError("api_unreachable", "Airflow API unreachable") from None
        if response.status_code in (401, 403):
            raise AirflowAPIError("auth_failed", "Airflow authentication failed")
        if response.status_code == 404:
            raise AirflowAPIError(
                "api_error",
                "Airflow /auth/token was not found; check --api-version and the base URL",
            )
        if response.status_code < 200 or response.status_code >= 300:
            raise AirflowAPIError("auth_failed", "Airflow authentication failed")
        try:
            data = response.json()
        except ValueError:
            raise AirflowAPIError("auth_failed", "Airflow authentication failed") from None
        token = data.get("access_token") if isinstance(data, dict) else None
        if not isinstance(token, str) or not token:
            raise AirflowAPIError("auth_failed", "Airflow authentication failed")
        return token

    def _get_runs(self, dag_id: str, params: list[tuple[str, str]]) -> dict[str, object]:
        version = self.resolved_version or self.api_version
        url = f"{self.base_url}/api/{version}/dags/{quote(dag_id, safe='')}/dagRuns"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
        except requests.RequestException:
            raise AirflowAPIError("api_unreachable", "Airflow API unreachable") from None
        if response.status_code in (401, 403):
            raise AirflowAPIError("auth_failed", "Airflow authentication failed")
        if response.status_code == 404:
            raise AirflowAPIError("dag_not_found", f"{dag_id}: DAG not found", dag_id=dag_id)
        if response.status_code >= 400:
            raise AirflowAPIError(
                "api_error",
                f"Airflow API returned HTTP {response.status_code} for DAG {dag_id}",
                dag_id=dag_id,
            )
        try:
            data = response.json()
        except ValueError:
            raise AirflowAPIError(
                "api_error",
                f"Airflow API returned a non-JSON body for DAG {dag_id}",
                dag_id=dag_id,
            ) from None
        if not isinstance(data, dict):
            raise AirflowAPIError(
                "api_error",
                f"Airflow API returned an unexpected payload for DAG {dag_id}",
                dag_id=dag_id,
            )
        return data

    def _parse(self, payload: dict[str, object], dag_id: str) -> list[DagRun]:
        try:
            return parse_dag_run_collection(payload)
        except ValueError:
            raise AirflowAPIError(
                "api_error",
                f"Airflow API returned an unexpected payload for DAG {dag_id}",
                dag_id=dag_id,
            ) from None


def check_airflow(
    client: AirflowClient,
    dag_ids: list[str],
    *,
    max_age: timedelta | None,
    max_age_label: str | None,
    now: datetime,
) -> tuple[list[Problem], str | None]:
    try:
        version = client.resolve_version()
        client.authenticate()
    except AirflowAPIError as exc:
        return [access_problem(exc, client.base_url)], client.resolved_version
    problems: list[Problem] = []
    for dag_id in dag_ids:
        try:
            latest = client.latest_run(dag_id)
            last_success = client.last_success(dag_id) if max_age is not None else None
        except AirflowAPIError as exc:
            problems.append(access_problem(exc, client.base_url))
            if exc.code in {"api_unreachable", "auth_failed"}:
                break
            continue
        problems.extend(
            evaluate_dag(
                dag_id,
                latest,
                last_success,
                max_age=max_age,
                max_age_label=max_age_label,
                now=now,
                base_url=client.base_url,
            )
        )
    return problems, version


def access_problem(exc: AirflowAPIError, base_url: str) -> Problem:
    details: dict[str, object] = {}
    if exc.dag_id:
        details["dag_id"] = exc.dag_id
    return Problem(
        code=exc.code,
        summary=exc.summary,
        check="airflow",
        fingerprint=_fingerprint(exc.code, base_url, exc.dag_id or "", "", ""),
        details=details,
    )


def _failed_problem(dag_id: str, latest: DagRun, base_url: str) -> Problem:
    run_ref = latest.dag_run_id or "unknown"
    summary = f"{dag_id}: latest run failed (run {run_ref}"
    if latest.end is not None:
        summary += f", ended {isoformat(latest.end)}"
    elif latest.start is not None or latest.logical is not None:
        summary += f", at {isoformat(latest.start or latest.logical)}"
    summary += ")"
    return Problem(
        code="latest_failed",
        summary=summary,
        check="airflow",
        fingerprint=_fingerprint("latest_failed", base_url, dag_id, latest.dag_run_id, ""),
        details={
            "dag_id": dag_id,
            "dag_run_id": latest.dag_run_id or None,
            "state": "failed",
            "start_date": isoformat(latest.start),
            "end_date": isoformat(latest.end),
            "logical_date": isoformat(latest.logical),
        },
    )


def _fingerprint(code: str, base_url: str, dag_id: str, dag_run_id: str, max_age: str) -> str:
    return f"airflow|{code}|{base_url}|{dag_id}|{dag_run_id}|{max_age}"
