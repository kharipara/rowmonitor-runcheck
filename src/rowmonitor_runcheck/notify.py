"""Alert bodies and webhook delivery.

Slack receives `{"text": ...}`. A generic webhook receives the canonical JSON
document. Credentials and webhook URLs are never included in errors or payloads.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from rowmonitor_runcheck import __version__
from rowmonitor_runcheck.dedupe import load_fingerprints, save_fingerprints
from rowmonitor_runcheck.errors import WebhookError
from rowmonitor_runcheck.problems import Problem

AIRFLOW_UPGRADE_URL = "https://rowmonitor.com/airflow?utm_source=runcheck&utm_medium=cli"
SIGNUP_URL = "https://rowmonitor.com/signup?utm_source=runcheck&utm_medium=cli"
FOOTER = (
    "Want run history, late vs never-arrived detection and table-level checks? "
    "Row Monitor watches the data itself: "
    f"{AIRFLOW_UPGRADE_URL}"
)

_USER_AGENT = f"rowmonitor-runcheck/{__version__}"


def slack_text(problems: list[Problem]) -> str:
    noun = "problem" if len(problems) == 1 else "problems"
    lines = [f"rowmonitor-runcheck: {len(problems)} {noun}"]
    lines.extend(f"• {problem.summary}" for problem in problems)
    lines.append("")
    lines.append(FOOTER)
    lines.append(f"Sign up: {SIGNUP_URL}")
    return "\n".join(lines)


def build_payload(
    *,
    check: str,
    problems: list[Problem],
    generated_at: datetime,
    extra: dict[str, Any],
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "source": "rowmonitor-runcheck",
        "version": __version__,
        "check": check,
        "ok": not problems,
        "generated_at": generated_at.isoformat(),
        "problems": [problem.to_dict() for problem in problems],
    }
    body.update(extra)
    return body


def post_json(url: str, payload: dict[str, Any], timeout: float) -> None:
    try:
        response = requests.post(
            url,
            json=payload,
            timeout=timeout,
            allow_redirects=False,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "User-Agent": _USER_AGENT,
            },
        )
    except requests.RequestException:
        raise WebhookError("webhook request failed") from None
    if response.status_code < 200 or response.status_code >= 300:
        raise WebhookError(f"webhook returned HTTP {response.status_code}")


def emit(
    *,
    problems: list[Problem],
    payload: dict[str, Any],
    ok_line: str,
    slack_url: str | None,
    webhook_url: str | None,
    state_path: Path | None,
    dry_run: bool,
    timeout: float,
) -> int:
    """Print or deliver alerts. Returns the process exit code."""
    current = {problem.fingerprint for problem in problems}
    previous: set[str] = set()
    if state_path is not None:
        previous = load_fingerprints(state_path)
    deduped = bool(problems) and state_path is not None and current == previous
    would_post = bool(problems) and not deduped and bool(slack_url or webhook_url)
    slack_body = {"text": slack_text(problems)} if problems and slack_url else None
    webhook_body = payload if problems and webhook_url else None

    if dry_run:
        document = {
            "ok": not problems,
            "deduped": deduped,
            "would_post": would_post,
            "slack": slack_body,
            "webhook": webhook_body,
            "preview": payload,
        }
        json.dump(document, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 1 if problems else 0

    if not problems:
        print(ok_line)
        if state_path is not None:
            save_fingerprints(state_path, set())
        return 0

    for problem in problems:
        print(problem.summary)
    if deduped:
        print("alert suppressed: same problems as the last alert", file=sys.stderr)
        return 1
    if not slack_url and not webhook_url:
        print("no webhook configured; not sending", file=sys.stderr)
        return 1

    if slack_body is not None and slack_url is not None:
        post_json(slack_url, slack_body, timeout)
    if webhook_body is not None and webhook_url is not None:
        post_json(webhook_url, webhook_body, timeout)
    if state_path is not None:
        save_fingerprints(state_path, current)
    return 1
