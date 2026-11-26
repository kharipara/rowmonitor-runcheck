#!/bin/sh
# Regenerate tests/fixtures/dbt/run_results.json from this tiny dbt-duckdb project.
# `dbt build` is expected to exit 1: broken errors, after_broken is skipped,
# one test fails, and one test warns.
set -eu
cd "$(dirname "$0")"
export DO_NOT_TRACK=1
export DBT_SEND_ANONYMOUS_USAGE_STATS=false
set +e
dbt build --profiles-dir . --project-dir .
status=$?
set -e
if [ "$status" -ne 1 ]; then
  echo "expected dbt build to exit 1 because the fixture project fails; got $status" >&2
  exit "$status"
fi
python3 - <<'PY'
import json
from pathlib import Path

source = Path("target/run_results.json")
destination = Path("../../tests/fixtures/dbt/run_results.json")
data = json.loads(source.read_text(encoding="utf-8"))
destination.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
print(f"wrote {destination}")
PY
