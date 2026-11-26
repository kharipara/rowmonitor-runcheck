"""Read a dbt Core target/run_results.json. dbt Cloud is not supported."""

from __future__ import annotations

import json
from pathlib import Path

from rowmonitor_runcheck.errors import ConfigError
from rowmonitor_runcheck.problems import Problem

_FAILURE_STATUSES = {"error", "fail", "runtime error"}


def load_run_results(path: Path) -> tuple[str | None, list[dict[str, object]]]:
    if path.is_dir():
        raise ConfigError(f"run results path is a directory: {path}")
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read run results: {exc.strerror or exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"run results are not valid JSON ({exc.msg})") from exc
    if not isinstance(data, dict) or not isinstance(data.get("results"), list):
        raise ConfigError("run results must be a JSON object with a results array")
    invocation_id = None
    metadata = data.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("invocation_id"), str):
        invocation_id = metadata["invocation_id"]
    results: list[dict[str, object]] = []
    for item in data["results"]:
        if isinstance(item, dict):
            results.append(item)
    return invocation_id, results


def evaluate_dbt(
    results: list[dict[str, object]],
    *,
    invocation_id: str | None,
    fail_on_warn: bool,
) -> list[Problem]:
    problems: list[Problem] = []
    for item in results:
        status = str(item.get("status") or "").strip().lower()
        unique_id = str(item.get("unique_id") or "unknown")
        message = item.get("message")
        message_text = message.strip() if isinstance(message, str) else ""
        code = _code_for(item, status, message_text, fail_on_warn=fail_on_warn)
        if code is None:
            continue
        problems.append(
            Problem(
                code=code,
                summary=_summary(unique_id, status, message_text, code),
                check="dbt",
                fingerprint=f"dbt|{invocation_id or ''}|{unique_id}|{status}",
                details={
                    "unique_id": unique_id,
                    "status": status,
                    "message": message_text or None,
                },
            )
        )
    return problems


def _code_for(
    item: dict[str, object],
    status: str,
    message: str,
    *,
    fail_on_warn: bool,
) -> str | None:
    if status in _FAILURE_STATUSES:
        return "dbt_failed"
    if status == "warn" and fail_on_warn:
        return "dbt_warn"
    if status == "skipped" and _upstream_skip(item, message):
        return "dbt_skipped_upstream"
    return None


def _upstream_skip(item: dict[str, object], message: str) -> bool:
    """dbt records an upstream skip in two shapes.

    Older artifacts, ephemeral failures, and ``--fail-fast`` put the reason in
    ``message``. dbt 1.12 leaves ``message`` empty and ``compiled`` false.
    A skip that carries some other message is not treated as an upstream failure.
    """
    lowered = message.lower()
    if "fail_fast" in lowered or "fail fast" in lowered:
        return True
    if "upstream" in lowered and ("fail" in lowered or "error" in lowered):
        return True
    if "ephemeral" in lowered and ("fail" in lowered or "error" in lowered):
        return True
    if message:
        return False
    # Empty message: dbt 1.12's upstream skip. A compiled node was not skipped
    # before it ran, so require compiled to be missing or false.
    return item.get("compiled") is not True


def _summary(unique_id: str, status: str, message: str, code: str) -> str:
    if code == "dbt_skipped_upstream":
        text = f"{unique_id}: skipped because an upstream node failed"
    else:
        text = f"{unique_id}: {status}"
    if message:
        clipped = " ".join(message.split())
        if len(clipped) > 300:
            clipped = clipped[:297] + "..."
        text = f"{text} ({clipped})"
    return text
