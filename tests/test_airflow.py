"""Airflow response parsing, max-age, and the REST client."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from rowmonitor_runcheck.airflow import (
    DagRun,
    evaluate_dag,
    parse_dag_run,
    parse_dag_run_collection,
    validate_base_url,
)
from rowmonitor_runcheck.cli import main
from rowmonitor_runcheck.errors import ConfigError
from rowmonitor_runcheck.notify import AIRFLOW_UPGRADE_URL, SIGNUP_URL
from rowmonitor_runcheck.timeutil import parse_max_age
from tests.fakes import AirflowFake, collection, empty_collection

FIXTURES = Path(__file__).parent / "fixtures" / "airflow"
NOW = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
BASE = "http://airflow.example"
USER = "runcheck-user"
PASSWORD = "s3cret-password"
TOKEN = "jwt-token-value"


def _run(
    state: str,
    end: datetime | None,
    *,
    run_id: str = "run-1",
    start: datetime | None = None,
    logical: datetime | None = None,
) -> DagRun:
    return DagRun("orders_daily", run_id, state, start, end, logical)


def _codes(problems: list) -> list[str]:
    return [problem.code for problem in problems]


def test_parse_airflow_v1_fixture() -> None:
    payload = json.loads((FIXTURES / "v1_dag_runs.json").read_text(encoding="utf-8"))
    runs = parse_dag_run_collection(payload)
    assert len(runs) == 1
    assert runs[0].dag_id == "orders_daily"
    assert runs[0].dag_run_id == "scheduled__2026-11-24T00:00:00+00:00"
    assert runs[0].state == "failed"
    assert runs[0].end == datetime(2026, 11, 24, 0, 12, tzinfo=timezone.utc)
    assert runs[0].logical == datetime(2026, 11, 24, 0, 0, tzinfo=timezone.utc)
    assert "execution_date" not in runs[0].__dict__


def test_parse_airflow_v2_fixture_without_execution_date() -> None:
    payload = json.loads((FIXTURES / "v2_dag_runs.json").read_text(encoding="utf-8"))
    assert "execution_date" not in payload["dag_runs"][0]
    runs = parse_dag_run_collection(payload)
    assert runs[0].state == "failed"
    assert runs[0].logical == datetime(2026, 11, 24, 0, 0, tzinfo=timezone.utc)
    assert runs[0].end == datetime(2026, 11, 24, 0, 5, tzinfo=timezone.utc)
    assert runs[0].dag_run_id.startswith("manual__")


def test_execution_date_is_used_when_logical_date_is_absent() -> None:
    run = parse_dag_run(
        {
            "dag_id": "orders_daily",
            "dag_run_id": "scheduled__2026-11-01T00:00:00+00:00",
            "execution_date": "2026-11-01T00:00:00Z",
            "state": "success",
        }
    )
    assert run.logical == datetime(2026, 11, 1, tzinfo=timezone.utc)


def test_invalid_timestamp_and_missing_collection_are_rejected() -> None:
    with pytest.raises(ValueError):
        parse_dag_run({"state": "failed", "end_date": "yesterday"})
    with pytest.raises(ValueError):
        parse_dag_run_collection({"total_entries": 0})
    with pytest.raises(ValueError):
        parse_dag_run_collection({"dag_runs": ["nope"]})


def test_max_age_boundary() -> None:
    window, label = parse_max_age("26h")
    fresh_end = NOW - window
    assert (
        evaluate_dag(
            "orders_daily",
            _run("success", fresh_end, run_id="ok"),
            _run("success", fresh_end, run_id="ok"),
            max_age=window,
            max_age_label=label,
            now=NOW,
            base_url=BASE,
        )
        == []
    )
    stale_end = fresh_end - timedelta(seconds=1)
    problems = evaluate_dag(
        "orders_daily",
        _run("success", stale_end, run_id="old"),
        _run("success", stale_end, run_id="old"),
        max_age=window,
        max_age_label=label,
        now=NOW,
        base_url=BASE,
    )
    assert _codes(problems) == ["success_too_old"]
    assert problems[0].details["age_seconds"] == int(window.total_seconds()) + 1
    assert "older than 26h" in problems[0].summary


def test_failed_latest_with_and_without_a_recent_success() -> None:
    window, label = parse_max_age("26h")
    failed = _run("failed", NOW - timedelta(minutes=5), run_id="failed-1")
    recent = _run("success", NOW - timedelta(hours=1), run_id="ok-1")
    only_failed = evaluate_dag(
        "orders_daily",
        failed,
        recent,
        max_age=window,
        max_age_label=label,
        now=NOW,
        base_url=BASE,
    )
    assert _codes(only_failed) == ["latest_failed"]
    assert only_failed[0].details["state"] == "failed"
    assert only_failed[0].details["dag_run_id"] == "failed-1"

    both = evaluate_dag(
        "orders_daily",
        failed,
        None,
        max_age=window,
        max_age_label=label,
        now=NOW,
        base_url=BASE,
    )
    assert _codes(both) == ["latest_failed", "no_success"]


def test_running_or_missing_runs_without_max_age_are_quiet() -> None:
    running = _run("running", None, start=NOW, run_id="live")
    assert (
        evaluate_dag(
            "orders_daily",
            running,
            None,
            max_age=None,
            max_age_label=None,
            now=NOW,
            base_url=BASE,
        )
        == []
    )
    assert (
        evaluate_dag(
            "orders_daily",
            None,
            None,
            max_age=None,
            max_age_label=None,
            now=NOW,
            base_url=BASE,
        )
        == []
    )


def test_success_without_a_timestamp_is_too_old() -> None:
    window, label = parse_max_age("1h")
    success = _run("success", None, run_id="blank")
    problems = evaluate_dag(
        "orders_daily",
        success,
        success,
        max_age=window,
        max_age_label=label,
        now=NOW,
        base_url=BASE,
    )
    assert _codes(problems) == ["success_too_old"]
    assert problems[0].details["age_seconds"] is None


def test_max_age_rejects_junk() -> None:
    with pytest.raises(ConfigError):
        parse_max_age("26 hours")
    with pytest.raises(ConfigError):
        parse_max_age("0h")
    with pytest.raises(ConfigError):
        parse_max_age("-1h")


def test_base_url_strips_api_prefix_and_rejects_userinfo() -> None:
    assert validate_base_url("https://airflow.example/api/v1/") == "https://airflow.example"
    assert validate_base_url("https://airflow.example/api/v2") == "https://airflow.example"
    with pytest.raises(ConfigError):
        validate_base_url("https://user:secret@airflow.example")
    with pytest.raises(ConfigError):
        validate_base_url("ftp://airflow.example")


def _failed_dag(dag_id: str = "orders_daily") -> dict:
    return {
        "latest": collection(
            dag_id,
            "failed",
            "scheduled__2026-11-24T00:00:00+00:00",
            "2026-11-24T00:00:02Z",
            "2026-11-24T00:12:00Z",
        ),
        "success": empty_collection(),
    }


def _secrets(*chunks: str) -> None:
    for chunk in chunks:
        assert PASSWORD not in chunk
        assert TOKEN not in chunk
        assert "secret-token" not in chunk


def test_airflow_v2_jwt_flow_and_payloads(serve, capsys, monkeypatch) -> None:
    fake = AirflowFake(version="v2", dags={"orders_daily": _failed_dag()})
    base, server = serve(fake)
    monkeypatch.setenv("AIRFLOW_USERNAME", USER)
    monkeypatch.setenv("AIRFLOW_PASSWORD", PASSWORD)
    slack = f"{base}/hooks/slack"
    hook = f"{base}/hooks/secret-token"
    code = main(
        [
            "airflow",
            "--url",
            base,
            "--dag",
            "orders_daily",
            "--max-age",
            "26h",
            "--slack-webhook",
            slack,
            "--webhook",
            hook,
        ]
    )
    output, errors = capsys.readouterr()
    assert code == 1
    assert errors == ""
    _secrets(output, errors)
    posts = [item for item in server.requests if item["method"] == "POST"]
    token_posts = [item for item in posts if item["path"] == "/auth/token"]
    assert len(token_posts) == 1
    assert json.loads(token_posts[0]["body"]) == {"username": USER, "password": PASSWORD}
    dag_gets = [item for item in server.requests if item["path"].endswith("/dagRuns")]
    assert dag_gets
    assert all(item["authorization"] == f"Bearer {TOKEN}" for item in dag_gets)
    assert all(item["path"].startswith("/api/v2/") for item in dag_gets)
    assert any("order_by=-start_date" in item["query"] for item in dag_gets)
    assert any(
        "state=success" in item["query"] and "order_by=-end_date" in item["query"]
        for item in dag_gets
    )
    slack_posts = [item for item in posts if item["path"] == "/hooks/slack"]
    hook_posts = [item for item in posts if item["path"] == "/hooks/secret-token"]
    assert len(slack_posts) == 1
    assert len(hook_posts) == 1
    assert slack_posts[0]["authorization"] == ""
    slack_body = json.loads(slack_posts[0]["body"])
    assert set(slack_body) == {"text"}
    assert "orders_daily: latest run failed" in slack_body["text"]
    assert "orders_daily: no successful run" in slack_body["text"]
    assert AIRFLOW_UPGRADE_URL in slack_body["text"]
    assert SIGNUP_URL in slack_body["text"]
    assert "utm_medium=readme" not in slack_body["text"]
    webhook_body = json.loads(hook_posts[0]["body"])
    assert webhook_body["source"] == "rowmonitor-runcheck"
    assert webhook_body["check"] == "airflow"
    assert webhook_body["ok"] is False
    assert webhook_body["api_version"] == "v2"
    assert webhook_body["base_url"] == base
    codes = [item["code"] for item in webhook_body["problems"]]
    assert codes == ["latest_failed", "no_success"]
    assert "fingerprint" not in webhook_body["problems"][0]
    assert PASSWORD not in hook_posts[0]["body"]


def test_dry_run_prints_payload_and_does_not_post(serve, capsys, monkeypatch) -> None:
    fake = AirflowFake(version="v2", dags={"orders_daily": _failed_dag()})
    base, server = serve(fake)
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    code = main(
        [
            "airflow",
            "--url",
            base,
            "--api-version",
            "v2",
            "--dag",
            "orders_daily",
            "--max-age",
            "26h",
            "--slack-webhook",
            f"{base}/hooks/slack",
            "--dry-run",
        ]
    )
    output, errors = capsys.readouterr()
    assert code == 1
    assert errors == ""
    _secrets(output)
    document = json.loads(output)
    assert document["would_post"] is True
    assert document["deduped"] is False
    assert document["slack"]["text"].startswith("rowmonitor-runcheck: 2 problems")
    assert document["webhook"] is None
    assert document["preview"]["problems"][0]["code"] == "latest_failed"
    assert [item["path"] for item in server.requests if item["method"] == "POST"] == []
    assert not any(item["path"] == "/auth/token" for item in server.requests)


def test_airflow_v1_basic_auth_auto_detect(serve, capsys, monkeypatch) -> None:
    fresh = {
        "latest": collection(
            "orders_daily",
            "success",
            "manual__2099-01-01T00:00:00+00:00",
            "2099-01-01T00:00:00Z",
            "2099-01-01T00:05:00Z",
        ),
        "success": collection(
            "orders_daily",
            "success",
            "manual__2099-01-01T00:00:00+00:00",
            "2099-01-01T00:00:00Z",
            "2099-01-01T00:05:00Z",
        ),
    }
    fake = AirflowFake(version="v1", dags={"orders_daily": fresh})
    base, server = serve(fake)
    monkeypatch.setenv("AIRFLOW_USERNAME", USER)
    monkeypatch.setenv("AIRFLOW_PASSWORD", PASSWORD)
    code = main(["airflow", "--url", base, "--dag", "orders_daily", "--max-age", "26h"])
    output, errors = capsys.readouterr()
    assert code == 0
    assert output.strip() == "ok: checked 1 DAG"
    assert errors == ""
    paths = [item["path"] for item in server.requests]
    assert paths[0] == "/api/v2/version"
    assert paths[1] == "/api/v1/version"
    assert "/auth/token" not in paths
    dag_gets = [item for item in server.requests if item["path"].endswith("/dagRuns")]
    assert dag_gets
    assert all(item["path"].startswith("/api/v1/") for item in dag_gets)
    assert all(item["authorization"].startswith("Basic ") for item in dag_gets)
    _secrets(output, errors)


def test_preissued_token_skips_token_endpoint(serve, monkeypatch) -> None:
    fake = AirflowFake(version="v2", dags={"orders_daily": _failed_dag()})
    base, server = serve(fake)
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    monkeypatch.setenv("AIRFLOW_PASSWORD", PASSWORD)
    code = main(["airflow", "--url", base, "--api-version", "2", "--dag", "orders_daily"])
    assert code == 1
    assert not any(item["path"] == "/auth/token" for item in server.requests)


def test_auth_failure_does_not_leak_the_password(serve, capsys, monkeypatch) -> None:
    fake = AirflowFake(version="v2", accept_password=False, dags={"orders_daily": _failed_dag()})
    base, server = serve(fake)
    monkeypatch.setenv("AIRFLOW_USERNAME", USER)
    monkeypatch.setenv("AIRFLOW_PASSWORD", PASSWORD)
    code = main(["airflow", "--url", base, "--dag", "orders_daily", "--dag", "billing_hourly"])
    output, errors = capsys.readouterr()
    assert code == 1
    assert "Airflow authentication failed" in output
    _secrets(output, errors)
    assert not any(item["path"].endswith("/dagRuns") for item in server.requests)


def test_dag_not_found_does_not_stop_the_other_dag(serve, capsys, monkeypatch) -> None:
    fake = AirflowFake(version="v2", dags={"orders_daily": _failed_dag()})
    base, _server = serve(fake)
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    code = main(
        [
            "airflow",
            "--url",
            base,
            "--api-version",
            "v2",
            "--dag",
            "missing_dag",
            "--dag",
            "orders_daily",
        ]
    )
    output, _errors = capsys.readouterr()
    assert code == 1
    assert "missing_dag: DAG not found" in output
    assert "orders_daily: latest run failed" in output


def test_http_500_is_an_api_problem(serve, capsys, monkeypatch) -> None:
    fake = AirflowFake(version="v2", dags={"orders_daily": 500})
    base, _server = serve(fake)
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    code = main(["airflow", "--url", base, "--api-version", "v2", "--dag", "orders_daily"])
    output, _errors = capsys.readouterr()
    assert code == 1
    assert "HTTP 500" in output


def test_unreachable_api_alerts(serve, capsys, monkeypatch) -> None:
    base, server = serve(AirflowFake())
    server.stop()
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    code = main(
        [
            "airflow",
            "--url",
            base,
            "--api-version",
            "v2",
            "--dag",
            "orders_daily",
            "--timeout",
            "1",
        ]
    )
    output, errors = capsys.readouterr()
    assert code == 1
    assert output.strip() == "Airflow API unreachable"
    _secrets(output, errors)


def test_stale_success_over_http(serve, capsys, monkeypatch) -> None:
    stale = {
        "latest": collection(
            "billing_hourly",
            "success",
            "scheduled__2020-01-01T00:00:00+00:00",
            "2020-01-01T00:00:00Z",
            "2020-01-01T01:00:00Z",
        ),
        "success": collection(
            "billing_hourly",
            "success",
            "scheduled__2020-01-01T00:00:00+00:00",
            "2020-01-01T00:00:00Z",
            "2020-01-01T01:00:00Z",
        ),
    }
    base, _server = serve(AirflowFake(version="v2", dags={"billing_hourly": stale}))
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    code = main(
        [
            "airflow",
            "--url",
            base,
            "--api-version",
            "v2",
            "--dag",
            "billing_hourly",
            "--max-age",
            "26h",
            "--dry-run",
        ]
    )
    output, _errors = capsys.readouterr()
    assert code == 1
    document = json.loads(output)
    assert [item["code"] for item in document["preview"]["problems"]] == ["success_too_old"]
    assert document["preview"]["problems"][0]["max_age_seconds"] == 26 * 3600


def test_duplicate_dag_is_checked_once(serve, monkeypatch) -> None:
    base, server = serve(AirflowFake(version="v2", dags={"orders_daily": _failed_dag()}))
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    code = main(
        [
            "airflow",
            "--url",
            base,
            "--api-version",
            "v2",
            "--dag",
            "orders_daily",
            "--dag",
            "orders_daily",
            "--max-age",
            "26h",
        ]
    )
    assert code == 1
    dag_gets = [item for item in server.requests if item["path"].endswith("/dagRuns")]
    assert len(dag_gets) == 2


def test_missing_v1_credentials_are_a_usage_error(serve, capsys) -> None:
    base, server = serve(AirflowFake(version="v1"))
    code = main(["airflow", "--url", base, "--api-version", "v1", "--dag", "orders_daily"])
    _output, errors = capsys.readouterr()
    assert code == 2
    assert "AIRFLOW_USERNAME" in errors
    assert server.requests == []


def test_bad_max_age_and_credentials_in_the_url_exit_2(capsys) -> None:
    assert (
        main(["airflow", "--url", "http://airflow.example", "--dag", "orders", "--max-age", "soon"])
        == 2
    )
    assert main(["airflow", "--url", "http://ada:secret@airflow.example", "--dag", "orders"]) == 2
    _output, errors = capsys.readouterr()
    assert "ada:secret" not in errors
    assert "ada:secret" not in errors


def test_dedupe_suppresses_the_same_alert_and_sends_again_after_recovery(
    serve, capsys, monkeypatch, tmp_path
) -> None:
    state = tmp_path / "state.json"
    failed = AirflowFake(version="v2", dags={"orders_daily": _failed_dag()})
    base, server = serve(failed)
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    hook = f"{base}/hooks/secret-token"
    args = [
        "airflow",
        "--url",
        base,
        "--api-version",
        "v2",
        "--dag",
        "orders_daily",
        "--webhook",
        hook,
        "--state-file",
        str(state),
    ]
    assert main(args) == 1
    capsys.readouterr()
    assert len([item for item in server.requests if item["path"] == "/hooks/secret-token"]) == 1
    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["fingerprints"]
    assert PASSWORD not in state.read_text(encoding="utf-8")
    assert state.stat().st_mode & 0o777 == 0o600

    assert main(args) == 1
    _output, errors = capsys.readouterr()
    assert "alert suppressed" in errors
    assert len([item for item in server.requests if item["path"] == "/hooks/secret-token"]) == 1

    failed.dags["orders_daily"] = {
        "latest": collection(
            "orders_daily",
            "success",
            "manual__2099-01-02T00:00:00+00:00",
            "2099-01-02T00:00:00Z",
            "2099-01-02T00:05:00Z",
        ),
        "success": collection(
            "orders_daily",
            "success",
            "manual__2099-01-02T00:00:00+00:00",
            "2099-01-02T00:00:00Z",
            "2099-01-02T00:05:00Z",
        ),
    }
    assert main(args + ["--max-age", "26h"]) == 0
    assert json.loads(state.read_text(encoding="utf-8"))["fingerprints"] == []

    failed.dags["orders_daily"] = _failed_dag()
    assert main(args) == 1
    assert len([item for item in server.requests if item["path"] == "/hooks/secret-token"]) == 2


def test_webhook_delivery_failure_exits_2_without_leaking_the_url(
    serve, capsys, monkeypatch
) -> None:
    fake = AirflowFake(version="v2", dags={"orders_daily": _failed_dag()}, webhook_status=500)
    base, _server = serve(fake)
    monkeypatch.setenv("AIRFLOW_TOKEN", TOKEN)
    code = main(
        [
            "airflow",
            "--url",
            base,
            "--api-version",
            "v2",
            "--dag",
            "orders_daily",
            "--webhook",
            f"{base}/hooks/secret-token",
        ]
    )
    output, errors = capsys.readouterr()
    assert code == 2
    assert "webhook returned HTTP 500" in errors
    assert "secret-token" not in output
    assert "secret-token" not in errors
    _secrets(output, errors)
