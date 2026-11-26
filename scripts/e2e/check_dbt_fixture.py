#!/usr/bin/env python3
"""Run the CLI against the committed dbt Core run_results.json."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "dbt" / "run_results.json"


def run(extra: list[str]) -> tuple[int, str, str]:
    command = [
        sys.executable,
        "-m",
        "rowmonitor_runcheck",
        "dbt",
        "--run-results",
        str(FIXTURE),
        *extra,
    ]
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    return completed.returncode, completed.stdout, completed.stderr


def main() -> int:
    if not FIXTURE.is_file():
        print(f"missing fixture {FIXTURE}", file=sys.stderr)
        return 2
    code, stdout, stderr = run(["--dry-run"])
    if code != 1:
        print(f"dbt fixture exit {code}, expected 1", file=sys.stderr)
        print(stdout, file=sys.stderr)
        print(stderr, file=sys.stderr)
        return 1
    document = json.loads(stdout)
    problems = document["preview"]["problems"]
    ids = [str(item["unique_id"]) for item in problems]
    required = (
        "model.runcheck_fixture.broken",
        "model.runcheck_fixture.after_broken",
        "accepted_values_orders_status__shipped",
    )
    missing = [item for item in required if not any(item in unique_id for unique_id in ids)]
    if missing:
        print(f"dbt fixture payload missing {missing}: {ids}", file=sys.stderr)
        return 1
    passing = ("not_null_orders_id", "unique_orders_id")
    if any(name in unique_id for unique_id in ids for name in passing):
        print(f"passing test was alerted: {ids}", file=sys.stderr)
        return 1
    if any(item.get("status") == "warn" for item in problems):
        print("warn was alerted without --fail-on warn", file=sys.stderr)
        return 1
    print("e2e: dbt fixture exit 1 names broken, accepted_values fail, after_broken skip")

    code, stdout, stderr = run(["--dry-run", "--fail-on", "warn"])
    if code != 1:
        print(f"dbt fixture --fail-on warn exit {code}, expected 1", file=sys.stderr)
        print(stdout, file=sys.stderr)
        print(stderr, file=sys.stderr)
        return 1
    warned = json.loads(stdout)["preview"]["problems"]
    if not any(item.get("status") == "warn" for item in warned):
        print("warn was not alerted with --fail-on warn", file=sys.stderr)
        return 1
    print("e2e: dbt fixture --fail-on warn includes warn")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
