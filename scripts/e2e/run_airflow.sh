#!/usr/bin/env bash
# Live check of rowmonitor-runcheck against official Apache Airflow images.
#
# Starts Airflow standalone, triggers runcheck_ok and runcheck_fail, then runs
# the CLI with --dry-run and against scripts/e2e/webhook_catcher.py.
#
#   ./scripts/e2e/run_airflow.sh        # Airflow 2.10.5, then Airflow 3.4.0
#   ./scripts/e2e/run_airflow.sh 2      # apache/airflow:2.10.5 only
#   ./scripts/e2e/run_airflow.sh 3      # apache/airflow:3.4.0 only
#
# Requires Docker. This script is not part of CI.
# Override images with AIRFLOW2_IMAGE and AIRFLOW3_IMAGE.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DAG_DIR="$ROOT/scripts/e2e/dags"
AIRFLOW2_IMAGE="${AIRFLOW2_IMAGE:-apache/airflow:2.10.5}"
AIRFLOW3_IMAGE="${AIRFLOW3_IMAGE:-apache/airflow:3.4.0}"
HOST_PORT="${HOST_PORT:-18080}"
WEBHOOK_PORT="${WEBHOOK_PORT:-19099}"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not available, so this live Airflow check cannot run." >&2
  exit 2
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required." >&2
  exit 2
fi
if ! python3 -c "import requests" >/dev/null 2>&1; then
  echo "python3 cannot import requests. Install the package first: pip install -e ." >&2
  exit 2
fi

CLI=(python3 -m rowmonitor_runcheck)
NAME=""
CATCHER_PID=""

cleanup() {
  if [ -n "$CATCHER_PID" ]; then
    kill "$CATCHER_PID" >/dev/null 2>&1 || true
    wait "$CATCHER_PID" 2>/dev/null || true
    CATCHER_PID=""
  fi
  if [ -n "$NAME" ]; then
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    NAME=""
  fi
}
trap cleanup EXIT

wait_http() {
  local url="$1"
  local extra=()
  if [ "$#" -gt 1 ]; then
    extra=("${@:2}")
  fi
  local attempt
  for attempt in $(seq 1 90); do
    if curl -sf "${extra[@]}" "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $url" >&2
  docker logs --tail 50 "$NAME" >&2 || true
  return 1
}

trigger_and_wait() {
  local api="$1"
  local auth_header=()
  if [ "$api" = "v1" ]; then
    auth_header=(-u "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD")
  else
    auth_header=(-H "Authorization: Bearer $AIRFLOW_TOKEN")
  fi
  local logical dag
  logical="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for dag in runcheck_ok runcheck_fail; do
    local attempt
    for attempt in $(seq 1 60); do
      local code
      code="$(curl -s -o /tmp/runcheck-dag.json -w "%{http_code}" \
        "${auth_header[@]}" \
        -X PATCH "http://127.0.0.1:${HOST_PORT}/api/${api}/dags/${dag}" \
        -H "Content-Type: application/json" \
        -d '{"is_paused": false}')"
      if [ "$code" = "200" ]; then
        break
      fi
      sleep 2
    done
    if [ "$code" != "200" ]; then
      echo "DAG $dag was not found (HTTP $code). Is the DAG folder mounted?" >&2
      cat /tmp/runcheck-dag.json >&2 || true
      return 1
    fi
    curl -sf "${auth_header[@]}" \
      -X POST "http://127.0.0.1:${HOST_PORT}/api/${api}/dags/${dag}/dagRuns" \
      -H "Content-Type: application/json" \
      -d "{\"logical_date\": \"${logical}\"}" >/dev/null
  done
  for dag in runcheck_ok runcheck_fail; do
    local state=""
    local attempt
    for attempt in $(seq 1 90); do
      state="$(curl -sf "${auth_header[@]}" \
        "http://127.0.0.1:${HOST_PORT}/api/${api}/dags/${dag}/dagRuns?limit=1&order_by=-start_date" \
        | python3 -c 'import json,sys; runs=json.load(sys.stdin).get("dag_runs") or []; print(runs[0]["state"] if runs else "")')"
      if [ "$state" = "success" ] || [ "$state" = "failed" ]; then
        break
      fi
      sleep 2
    done
    echo "$dag finished as ${state:-unknown}"
    if [ "$dag" = "runcheck_ok" ] && [ "$state" != "success" ]; then
      return 1
    fi
    if [ "$dag" = "runcheck_fail" ] && [ "$state" != "failed" ]; then
      return 1
    fi
  done
}

run_cli_checks() {
  local api_flag="$1"
  local out="$2"
  local hooks="$3"
  set +e
  AIRFLOW_USERNAME="${AIRFLOW_USERNAME:-}" \
  AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-}" \
  AIRFLOW_TOKEN="${AIRFLOW_TOKEN:-}" \
    "${CLI[@]}" airflow \
      --url "http://127.0.0.1:${HOST_PORT}" \
      --api-version "$api_flag" \
      --dag runcheck_ok \
      --dag runcheck_fail \
      --max-age 26h \
      --dry-run \
      >"$out"
  local dry=$?
  set -e
  if [ "$dry" -ne 1 ]; then
    echo "dry-run exit $dry, expected 1" >&2
    cat "$out" >&2
    return 1
  fi
  python3 - "$out" <<'PY'
import json
import sys

document = json.load(open(sys.argv[1], encoding="utf-8"))
codes = {}
for problem in document["preview"]["problems"]:
    codes.setdefault(problem.get("dag_id"), []).append(problem["code"])
if "latest_failed" not in codes.get("runcheck_fail", []):
    raise SystemExit(f"failing DAG was not reported: {codes}")
if "latest_failed" in codes.get("runcheck_ok", []):
    raise SystemExit(f"passing DAG was reported as failed: {codes}")
print("dry-run ok:", {dag: items for dag, items in codes.items()})
PY

  : >"$hooks"
  python3 "$ROOT/scripts/e2e/webhook_catcher.py" --port "$WEBHOOK_PORT" --out "$hooks" &
  CATCHER_PID=$!
  sleep 0.5
  local state="$out.state.json"
  rm -f "$state"
  set +e
  AIRFLOW_USERNAME="${AIRFLOW_USERNAME:-}" \
  AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-}" \
  AIRFLOW_TOKEN="${AIRFLOW_TOKEN:-}" \
    "${CLI[@]}" airflow \
      --url "http://127.0.0.1:${HOST_PORT}" \
      --api-version "$api_flag" \
      --dag runcheck_ok \
      --dag runcheck_fail \
      --max-age 26h \
      --webhook "http://127.0.0.1:${WEBHOOK_PORT}/hook" \
      --state-file "$state"
  local live=$?
  set -e
  if [ "$live" -ne 1 ]; then
    echo "live exit $live, expected 1" >&2
    return 1
  fi
  python3 - "$hooks" <<'PY'
import json
import sys

lines = [line for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if len(lines) != 1:
    raise SystemExit(f"expected 1 webhook body, got {len(lines)}")
body = json.loads(lines[0])
if body.get("check") != "airflow" or body.get("ok") is not False:
    raise SystemExit(f"unexpected webhook body: {body}")
failed = [item for item in body["problems"] if item.get("dag_id") == "runcheck_fail" and item["code"] == "latest_failed"]
if not failed:
    raise SystemExit("webhook did not include the failing DAG")
print("webhook ok")
PY
  # The same failure must not be posted a second time.
  set +e
  AIRFLOW_USERNAME="${AIRFLOW_USERNAME:-}" \
  AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-}" \
  AIRFLOW_TOKEN="${AIRFLOW_TOKEN:-}" \
    "${CLI[@]}" airflow \
      --url "http://127.0.0.1:${HOST_PORT}" \
      --api-version "$api_flag" \
      --dag runcheck_ok \
      --dag runcheck_fail \
      --max-age 26h \
      --webhook "http://127.0.0.1:${WEBHOOK_PORT}/hook" \
      --state-file "$state"
  local again=$?
  set -e
  if [ "$again" -ne 1 ]; then
    echo "deduped run exit $again, expected 1" >&2
    return 1
  fi
  python3 - "$hooks" <<'PY'
import sys
lines = [line for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if len(lines) != 1:
    raise SystemExit(f"dedupe posted again; webhook lines={len(lines)}")
print("dedupe ok")
PY
  kill "$CATCHER_PID" >/dev/null 2>&1 || true
  wait "$CATCHER_PID" 2>/dev/null || true
  CATCHER_PID=""
}

run_airflow_2() {
  NAME="runcheck-airflow2-$$"
  echo "starting $AIRFLOW2_IMAGE"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" -p "${HOST_PORT}:8080" \
    -v "$DAG_DIR:/opt/airflow/dags:ro" \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
    -e AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false \
    -e AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth \
    "$AIRFLOW2_IMAGE" standalone >/dev/null
  local attempt password=""
  for attempt in $(seq 1 60); do
    if docker exec "$NAME" test -s /opt/airflow/standalone_admin_password.txt; then
      password="$(docker exec "$NAME" cat /opt/airflow/standalone_admin_password.txt | tr -d '[:space:]')"
      break
    fi
    sleep 2
  done
  if [ -z "$password" ]; then
    echo "Airflow 2 did not write standalone_admin_password.txt" >&2
    docker logs --tail 40 "$NAME" >&2 || true
    return 1
  fi
  export AIRFLOW_USERNAME=admin
  export AIRFLOW_PASSWORD="$password"
  unset AIRFLOW_TOKEN || true
  wait_http "http://127.0.0.1:${HOST_PORT}/health"
  trigger_and_wait v1
  local out
  out="$(mktemp)"
  run_cli_checks v1 "$out" "$(mktemp)"
  echo "Airflow 2.10 check passed ($AIRFLOW2_IMAGE)"
  docker rm -f "$NAME" >/dev/null
  NAME=""
  unset AIRFLOW_PASSWORD
}

run_airflow_3() {
  NAME="runcheck-airflow3-$$"
  local passdir
  passdir="$(mktemp -d)"
  printf '%s\n' '{"admin":"admin"}' >"$passdir/passwords.json"
  chmod 666 "$passdir/passwords.json"
  echo "starting $AIRFLOW3_IMAGE"
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" -p "${HOST_PORT}:8080" \
    -v "$DAG_DIR:/opt/airflow/dags:ro" \
    -v "$passdir/passwords.json:/opt/airflow/runcheck/passwords.json" \
    -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
    -e AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false \
    -e AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin \
    -e AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/opt/airflow/runcheck/passwords.json \
    "$AIRFLOW3_IMAGE" standalone >/dev/null
  export AIRFLOW_USERNAME=admin
  export AIRFLOW_PASSWORD=admin
  unset AIRFLOW_TOKEN || true
  wait_http "http://127.0.0.1:${HOST_PORT}/api/v2/version"
  local token
  token="$(curl -sf -X POST "http://127.0.0.1:${HOST_PORT}/auth/token" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}' \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')"
  export AIRFLOW_TOKEN="$token"
  trigger_and_wait v2
  local out
  out="$(mktemp)"
  run_cli_checks v2 "$out" "$(mktemp)"
  echo "Airflow 3.x check passed ($AIRFLOW3_IMAGE)"
  docker rm -f "$NAME" >/dev/null
  NAME=""
  unset AIRFLOW_TOKEN AIRFLOW_PASSWORD
}

target="${1:-all}"
case "$target" in
  2) run_airflow_2 ;;
  3) run_airflow_3 ;;
  all)
    run_airflow_2
    run_airflow_3
    ;;
  *)
    echo "usage: $0 [2|3|all]" >&2
    exit 2
    ;;
esac
