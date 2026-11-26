"""Command line for `rowmonitor-runcheck`."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

from rowmonitor_runcheck import __version__
from rowmonitor_runcheck.airflow import (
    AirflowClient,
    check_airflow,
    normalize_api_version,
    validate_base_url,
    validate_dag_id,
)
from rowmonitor_runcheck.dbt_results import evaluate_dbt, load_run_results
from rowmonitor_runcheck.errors import ConfigError, WebhookError
from rowmonitor_runcheck.notify import build_payload, emit
from rowmonitor_runcheck.timeutil import parse_max_age, utcnow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rowmonitor-runcheck",
        description=(
            "Check the latest Airflow DAG run or a dbt Core run_results.json "
            "and post a short alert. No account and no telemetry."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"rowmonitor-runcheck {__version__}",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    airflow = commands.add_parser(
        "airflow",
        help="Check the latest run of one or more Airflow DAGs",
    )
    airflow.add_argument(
        "--url",
        required=True,
        help="Airflow base URL, without /api/v1 or /api/v2",
    )
    airflow.add_argument(
        "--dag",
        action="append",
        dest="dags",
        required=True,
        help="DAG id to check. Repeat for each DAG.",
    )
    airflow.add_argument(
        "--max-age",
        default=None,
        help=(
            "Alert when the last success is older than this, or there has "
            "never been one. Examples: 30s, 45m, 26h, 2d."
        ),
    )
    airflow.add_argument(
        "--api-version",
        default="auto",
        help="auto (default), v1 (Airflow 2.6+ basic auth), or v2 (Airflow 3 JWT)",
    )
    _add_output_arguments(airflow)
    airflow.set_defaults(handler=_cmd_airflow)

    dbt = commands.add_parser(
        "dbt",
        help="Read a dbt Core run_results.json and alert on failed nodes",
    )
    dbt.add_argument(
        "--run-results",
        required=True,
        help="Path to target/run_results.json from dbt Core",
    )
    dbt.add_argument(
        "--fail-on",
        action="append",
        default=None,
        choices=["warn"],
        help="Also alert on this status. Currently: warn.",
    )
    _add_output_arguments(dbt)
    dbt.set_defaults(handler=_cmd_dbt)
    return parser


def _add_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--slack-webhook",
        default=None,
        help="Slack incoming webhook. Overrides SLACK_WEBHOOK_URL.",
    )
    parser.add_argument(
        "--webhook",
        default=None,
        help="Generic JSON webhook. Overrides RUNCHECK_WEBHOOK_URL.",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help=(
            "Remember the last alert so the same problems are not posted again. "
            "Overrides RUNCHECK_STATE_FILE. This is not a run history."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="HTTP timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the alert payload instead of sending it.",
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        return 0 if exc.code is None else 2
    try:
        return int(args.handler(args))
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except WebhookError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except BrokenPipeError:
        return 0


def _cmd_airflow(args: argparse.Namespace) -> int:
    timeout = _timeout(args.timeout)
    base_url = validate_base_url(args.url)
    api_version = normalize_api_version(args.api_version)
    dag_ids = _unique_dags(args.dags)
    max_age = None
    max_age_label = None
    if args.max_age is not None:
        max_age, max_age_label = parse_max_age(args.max_age)
    slack_url, webhook_url, state_path = _destinations(args)
    now = utcnow()
    client = AirflowClient(
        base_url=base_url,
        api_version=api_version,
        username=_env("AIRFLOW_USERNAME"),
        password=_env("AIRFLOW_PASSWORD"),
        token=_env("AIRFLOW_TOKEN"),
        timeout=timeout,
    )
    problems, resolved = check_airflow(
        client,
        dag_ids,
        max_age=max_age,
        max_age_label=max_age_label,
        now=now,
    )
    noun = "DAG" if len(dag_ids) == 1 else "DAGs"
    payload = build_payload(
        check="airflow",
        problems=problems,
        generated_at=now,
        extra={"base_url": base_url, "api_version": resolved},
    )
    return emit(
        problems=problems,
        payload=payload,
        ok_line=f"ok: checked {len(dag_ids)} {noun}",
        slack_url=slack_url,
        webhook_url=webhook_url,
        state_path=state_path,
        dry_run=args.dry_run,
        timeout=timeout,
    )


def _cmd_dbt(args: argparse.Namespace) -> int:
    timeout = _timeout(args.timeout)
    path = Path(args.run_results)
    invocation_id, results = load_run_results(path)
    fail_on_warn = "warn" in (args.fail_on or [])
    problems = evaluate_dbt(
        results,
        invocation_id=invocation_id,
        fail_on_warn=fail_on_warn,
    )
    slack_url, webhook_url, state_path = _destinations(args)
    now = utcnow()
    payload = build_payload(
        check="dbt",
        problems=problems,
        generated_at=now,
        extra={
            "run_results": str(path),
            "invocation_id": invocation_id,
        },
    )
    return emit(
        problems=problems,
        payload=payload,
        ok_line=f"ok: {path} has no failing nodes",
        slack_url=slack_url,
        webhook_url=webhook_url,
        state_path=state_path,
        dry_run=args.dry_run,
        timeout=timeout,
    )


def _destinations(
    args: argparse.Namespace,
) -> tuple[str | None, str | None, Path | None]:
    slack_url = _validate_webhook(
        _flag_or_env(args.slack_webhook, "SLACK_WEBHOOK_URL"),
        "Slack webhook",
    )
    webhook_url = _validate_webhook(
        _flag_or_env(args.webhook, "RUNCHECK_WEBHOOK_URL"),
        "webhook",
    )
    state_value = _flag_or_env(args.state_file, "RUNCHECK_STATE_FILE")
    state_path = Path(state_value).expanduser() if state_value else None
    return slack_url, webhook_url, state_path


def _validate_webhook(url: str | None, label: str) -> str | None:
    if url is None:
        return None
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ConfigError(f"{label} must be an http or https URL")
    return url


def _timeout(value: float) -> float:
    if value <= 0:
        raise ConfigError("--timeout must be greater than zero")
    return value


def _unique_dags(dag_ids: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for dag_id in dag_ids:
        validated = validate_dag_id(dag_id)
        if validated in seen:
            continue
        seen.add(validated)
        unique.append(validated)
    if not unique:
        raise ConfigError("pass at least one --dag")
    return unique


def _flag_or_env(flag: str | None, env_name: str) -> str | None:
    if flag is not None and flag.strip():
        return flag.strip()
    return _env(env_name)


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
