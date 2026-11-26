"""dbt Core run_results parsing. The fixture is a real dbt-duckdb artifact."""

from __future__ import annotations

import json
from pathlib import Path

from rowmonitor_runcheck.cli import main
from rowmonitor_runcheck.dbt_results import evaluate_dbt, load_run_results

FIXTURE = Path(__file__).parent / "fixtures" / "dbt" / "run_results.json"
PASSING_SQL = "where id is null"


def test_fixture_was_produced_by_dbt_core() -> None:
    data = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert data["metadata"]["dbt_schema_version"].endswith("/run-results/v6.json")
    assert data["metadata"]["dbt_version"]
    assert data["metadata"]["invocation_id"]
    statuses = {item["status"] for item in data["results"]}
    assert {"error", "fail", "warn", "pass", "success", "skipped"} <= statuses


def test_real_run_results_alert_on_error_fail_and_upstream_skip() -> None:
    invocation_id, results = load_run_results(FIXTURE)
    problems = evaluate_dbt(results, invocation_id=invocation_id, fail_on_warn=False)
    codes = [(problem.details["unique_id"], problem.code) for problem in problems]
    assert codes == [
        ("model.runcheck_fixture.broken", "dbt_failed"),
        ("model.runcheck_fixture.after_broken", "dbt_skipped_upstream"),
        (
            "test.runcheck_fixture.accepted_values_orders_status__shipped.443decadd5",
            "dbt_failed",
        ),
    ]
    assert all(problem.details["status"] != "warn" for problem in problems)
    rendered = json.dumps([problem.to_dict() for problem in problems])
    assert PASSING_SQL not in rendered
    assert "select 1 as id" not in rendered
    assert invocation_id


def test_fail_on_warn_includes_the_warning() -> None:
    invocation_id, results = load_run_results(FIXTURE)
    problems = evaluate_dbt(results, invocation_id=invocation_id, fail_on_warn=True)
    warned = [problem for problem in problems if problem.code == "dbt_warn"]
    assert len(warned) == 1
    assert warned[0].details["unique_id"].endswith("orders_status_warn_unless_shipped.d15982b55a")
    assert "configured to warn" in warned[0].summary


def test_other_skip_messages_are_not_upstream_failures() -> None:
    problems = evaluate_dbt(
        [
            {
                "unique_id": "model.proj.optional",
                "status": "skipped",
                "message": "Skipped by a selector",
                "compiled": False,
            },
            {
                "unique_id": "model.proj.compiled_skip",
                "status": "skipped",
                "message": None,
                "compiled": True,
            },
            {
                "unique_id": "model.proj.fast",
                "status": "skipped",
                "message": "Skipping due to fail_fast",
                "compiled": False,
            },
        ],
        invocation_id="inv",
        fail_on_warn=False,
    )
    assert [problem.details["unique_id"] for problem in problems] == ["model.proj.fast"]


def test_dbt_cli_dry_run_and_dedupe(capsys, tmp_path: Path) -> None:
    state = tmp_path / "state.json"
    code = main(
        [
            "dbt",
            "--run-results",
            str(FIXTURE),
            "--dry-run",
            "--state-file",
            str(state),
        ]
    )
    output, errors = capsys.readouterr()
    assert code == 1
    assert errors == ""
    document = json.loads(output)
    assert document["would_post"] is False
    assert document["preview"]["check"] == "dbt"
    assert document["preview"]["invocation_id"]
    assert document["preview"]["ok"] is False
    assert [item["code"] for item in document["preview"]["problems"]] == [
        "dbt_failed",
        "dbt_skipped_upstream",
        "dbt_failed",
    ]
    assert PASSING_SQL not in output
    assert not state.exists()

    code = main(
        [
            "dbt",
            "--run-results",
            str(FIXTURE),
            "--fail-on",
            "warn",
            "--state-file",
            str(state),
            "--dry-run",
            "--slack-webhook",
            "https://hooks.example/services/secret-token",
        ]
    )
    output, _errors = capsys.readouterr()
    assert code == 1
    document = json.loads(output)
    assert document["slack"]["text"].count("dbt_warn") == 0
    assert "orders_status_warn_unless_shipped" in document["slack"]["text"]
    assert (
        "https://rowmonitor.com/airflow?utm_source=runcheck&utm_medium=cli"
        in document["slack"]["text"]
    )
    assert "secret-token" not in output


def test_dbt_cli_posts_once_per_distinct_failure(serve, capsys, tmp_path: Path) -> None:
    from tests.fakes import AirflowFake

    base, server = serve(AirflowFake())
    state = tmp_path / "state.json"
    args = [
        "dbt",
        "--run-results",
        str(FIXTURE),
        "--webhook",
        f"{base}/hooks/secret-token",
        "--state-file",
        str(state),
    ]
    assert main(args) == 1
    output, errors = capsys.readouterr()
    assert errors == ""
    assert "model.runcheck_fixture.broken" in output
    posts = [item for item in server.requests if item["path"] == "/hooks/secret-token"]
    assert len(posts) == 1
    body = json.loads(posts[0]["body"])
    assert body["check"] == "dbt"
    assert body["source"] == "rowmonitor-runcheck"
    assert PASSING_SQL not in posts[0]["body"]
    assert main(args) == 1
    _output, errors = capsys.readouterr()
    assert "alert suppressed" in errors
    assert len([item for item in server.requests if item["path"] == "/hooks/secret-token"]) == 1


def test_missing_and_invalid_run_results_exit_2(capsys, tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    assert main(["dbt", "--run-results", str(missing)]) == 2
    invalid = tmp_path / "nope.json"
    invalid.write_text("{", encoding="utf-8")
    assert main(["dbt", "--run-results", str(invalid)]) == 2
    wrong = tmp_path / "list.json"
    wrong.write_text("[]", encoding="utf-8")
    assert main(["dbt", "--run-results", str(wrong)]) == 2
    _output, errors = capsys.readouterr()
    assert "error:" in errors


def test_clean_run_results_exit_0(capsys, tmp_path: Path) -> None:
    path = tmp_path / "run_results.json"
    path.write_text(
        json.dumps(
            {
                "metadata": {"invocation_id": "clean"},
                "results": [
                    {"unique_id": "model.proj.ok", "status": "success", "message": "OK"},
                    {"unique_id": "test.proj.ok", "status": "pass", "message": None},
                ],
            }
        ),
        encoding="utf-8",
    )
    assert main(["dbt", "--run-results", str(path)]) == 0
    output, _errors = capsys.readouterr()
    assert output.startswith("ok:")
