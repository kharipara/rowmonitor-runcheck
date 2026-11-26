#!/usr/bin/env bash
# Live check of rowmonitor-runcheck against official Apache Airflow images.
#
# Starts Airflow standalone, triggers runcheck_ok and runcheck_fail, then:
#   - dry-run of both DAGs exits 1 and names the failing DAG
#   - dry-run of the failing DAG alone exits 1 and names it
#   - dry-run of the passing DAG alone exits 0
#   - a live post to the local webhook catcher, then a deduped second run
#   - a wrong password reports an authentication failure
#   - a stopped server reports the API as unreachable
#
#   ./scripts/e2e/run_airflow.sh        # 2.10.5, then 3.4.0
#   ./scripts/e2e/run_airflow.sh 2
#   ./scripts/e2e/run_airflow.sh 3
#
# Requires Docker. Pull requests run this via .github/workflows/e2e.yml.
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

if python3 -c "import requests" >/dev/null 2>&1; then
  PY=(python3)
elif python -c "import requests" >/dev/null 2>&1; then
  PY=(python)
else
  echo "Install the package first so requests is importable: pip install -e ." >&2
  exit 2
fi
CLI=("${PY[@]}" -m rowmonitor_runcheck)

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

on_exit() {
  local status=$?
  if [ "$status" -ne 0 ] && [ -n "${NAME:-}" ]; then
    echo "---- last 120 log lines from $NAME ----" >&2
    docker logs --tail 120 "$NAME" 2>&1 \
      | sed -E 's/([Pp]assword[^:]*:).*/\1 [redacted]/' >&2 || true
  fi
  cleanup
}
trap on_exit EXIT

base_url() {
  echo "http://127.0.0.1:${HOST_PORT}"
}

wait_http() {
  local url="$1"
  shift || true
  local attempt
  for attempt in $(seq 1 90); do
    if curl -sf "$@" "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $url" >&2
  return 1
}

trigger_and_wait() {
  local api="$1"
  local -a auth_header=()
  if [ "$api" = "v1" ]; then
    auth_header=(-u "$AIRFLOW_USERNAME:$AIRFLOW_PASSWORD")
  else
    auth_header=(-H "Authorization: Bearer $AIRFLOW_TOKEN")
  fi
  local logical dag attempt code
  logical="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for dag in runcheck_ok runcheck_fail; do
    code=""
    for attempt in $(seq 1 90); do
      code="$(curl -s -o /tmp/runcheck-dag.json -w "%{http_code}" \
        "${auth_header[@]}" \
        -X PATCH "$(base_url)/api/${api}/dags/${dag}" \
        -H "Content-Type: application/json" \
        -d '{"is_paused": false}' || true)"
      if [ "$code" = "200" ] || [ "$code" = "204" ]; then
        break
      fi
      if [ $((attempt % 10)) -eq 0 ]; then
        echo "still waiting for DAG $dag (HTTP ${code:-none})"
      fi
      sleep 2
    done
    if [ "$code" != "200" ] && [ "$code" != "204" ]; then
      echo "DAG $dag was not found (HTTP ${code:-none})." >&2
      cat /tmp/runcheck-dag.json >&2 || true
      return 1
    fi
    local trigger_body="/tmp/runcheck-trigger-${dag}.json"
    local trigger_code
    trigger_code="$(curl -s -o "$trigger_body" -w "%{http_code}" \
      "${auth_header[@]}" \
      -X POST "$(base_url)/api/${api}/dags/${dag}/dagRuns" \
      -H "Content-Type: application/json" \
      -d "{\"logical_date\": \"${logical}\"}" || true)"
    if [ "$trigger_code" != "200" ] && [ "$trigger_code" != "201" ]; then
      echo "trigger $dag failed (HTTP ${trigger_code:-none})" >&2
      cat "$trigger_body" >&2 || true
      return 1
    fi
    echo "triggered $dag"
  done

  for dag in runcheck_ok runcheck_fail; do
    local state="" attempt
    for attempt in $(seq 1 120); do
      state="$("${PY[@]}" - "$api" "$dag" "$(base_url)" <<'PY' || true
import json, sys, urllib.request
api, dag, base = sys.argv[1:]
url = f"{base}/api/{api}/dags/{dag}/dagRuns?limit=5&order_by=-start_date"
request = urllib.request.Request(url)
user = __import__("os").environ.get("AIRFLOW_USERNAME", "")
password = __import__("os").environ.get("AIRFLOW_PASSWORD", "")
token = __import__("os").environ.get("AIRFLOW_TOKEN", "")
if api == "v1":
    import base64
    basic = base64.b64encode(f"{user}:{password}".encode()).decode()
    request.add_header("Authorization", f"Basic {basic}")
else:
    request.add_header("Authorization", f"Bearer {token}")
try:
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = json.load(response)
except Exception:
    print("")
    raise SystemExit(0)
runs = payload.get("dag_runs") or []
print(runs[0].get("state", "") if runs else "")
PY
)"
      if [ "$state" = "success" ] || [ "$state" = "failed" ]; then
        break
      fi
      if [ $((attempt % 10)) -eq 0 ]; then
        echo "still waiting for $dag (state ${state:-none})"
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

run_cli() {
  # Usage: run_cli <outfile> <errfile> -- airflow-args...
  local out="$1"
  local err="$2"
  shift 2
  if [ "${1:-}" != "--" ]; then
    echo "run_cli internal error" >&2
    return 2
  fi
  shift
  set +e
  AIRFLOW_USERNAME="${AIRFLOW_USERNAME:-}" \
  AIRFLOW_PASSWORD="${AIRFLOW_PASSWORD:-}" \
  AIRFLOW_TOKEN="${AIRFLOW_TOKEN:-}" \
    "${CLI[@]}" airflow "$@" >"$out" 2>"$err"
  local code=$?
  set -e
  return "$code"
}

assert_fail_payload() {
  local out="$1"
  "${PY[@]}" - "$out" <<'PY'
import json
import sys

document = json.load(open(sys.argv[1], encoding="utf-8"))
problems = document["preview"]["problems"]
failed = [item for item in problems if item.get("dag_id") == "runcheck_fail" and item.get("code") == "latest_failed"]
if not failed:
    raise SystemExit(f"failing DAG was not named in the payload: {problems}")
ok_failed = [item for item in problems if item.get("dag_id") == "runcheck_ok" and item.get("code") == "latest_failed"]
if ok_failed:
    raise SystemExit(f"passing DAG was reported as failed: {problems}")
text = json.dumps(document)
if "runcheck_fail" not in text:
    raise SystemExit("payload text did not name runcheck_fail")
print("e2e: runcheck_fail named in payload")
PY
}

run_cli_checks() {
  local api_flag="$1"
  local out hooks
  out="$(mktemp)"
  hooks="$(mktemp)"
  local err
  err="$(mktemp)"
  local dry=0
  run_cli "$out" "$err" -- \
    --url "$(base_url)" \
    --api-version "$api_flag" \
    --dag runcheck_ok \
    --dag runcheck_fail \
    --max-age 26h \
    --dry-run \
    || dry=$?
  if [ "$dry" -ne 1 ]; then
    echo "combined dry-run exit $dry, expected 1" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  assert_fail_payload "$out"
  echo "e2e: both DAGs dry-run exit 1"

  dry=0
  run_cli "$out" "$err" -- \
    --url "$(base_url)" \
    --api-version "$api_flag" \
    --dag runcheck_fail \
    --max-age 26h \
    --dry-run \
    || dry=$?
  if [ "$dry" -ne 1 ]; then
    echo "failing DAG exit $dry, expected 1" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  assert_fail_payload "$out"
  echo "e2e: runcheck_fail exit 1"

  dry=0
  run_cli "$out" "$err" -- \
    --url "$(base_url)" \
    --api-version "$api_flag" \
    --dag runcheck_ok \
    --max-age 26h \
    --dry-run \
    || dry=$?
  if [ "$dry" -ne 0 ]; then
    echo "passing DAG exit $dry, expected 0" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  "${PY[@]}" - "$out" <<'PY'
import json
import sys

document = json.load(open(sys.argv[1], encoding="utf-8"))
problems = document["preview"]["problems"]
if document.get("ok") is not True or problems:
    raise SystemExit(f"passing DAG payload was not clean: {document}")
print("e2e: runcheck_ok exit 0")
PY

  : >"$hooks"
  "${PY[@]}" "$ROOT/scripts/e2e/webhook_catcher.py" --port "$WEBHOOK_PORT" --out "$hooks" &
  CATCHER_PID=$!
  local attempt
  for attempt in $(seq 1 50); do
    if "${PY[@]}" - "$WEBHOOK_PORT" <<'PY'
import socket, sys
port = int(sys.argv[1])
try:
    socket.create_connection(("127.0.0.1", port), 0.2).close()
except OSError:
    raise SystemExit(1)
PY
    then
      break
    fi
    sleep 0.2
  done

  local state="$out.state.json"
  rm -f "$state"
  local live=0
  run_cli "$out" "$err" -- \
    --url "$(base_url)" \
    --api-version "$api_flag" \
    --dag runcheck_ok \
    --dag runcheck_fail \
    --max-age 26h \
    --webhook "http://127.0.0.1:${WEBHOOK_PORT}/hook" \
    --state-file "$state" \
    || live=$?
  if [ "$live" -ne 1 ]; then
    echo "live webhook exit $live, expected 1" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  "${PY[@]}" - "$hooks" <<'PY'
import json
import sys

lines = [line for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if len(lines) != 1:
    raise SystemExit(f"expected 1 webhook body, got {len(lines)}")
body = json.loads(lines[0])
if body.get("check") != "airflow" or body.get("ok") is not False:
    raise SystemExit(f"unexpected webhook body: {body}")
failed = [
    item for item in body["problems"]
    if item.get("dag_id") == "runcheck_fail" and item.get("code") == "latest_failed"
]
if not failed:
    raise SystemExit("webhook did not name runcheck_fail")
print("e2e: webhook catcher received runcheck_fail latest_failed")
PY

  live=0
  run_cli "$out" "$err" -- \
    --url "$(base_url)" \
    --api-version "$api_flag" \
    --dag runcheck_ok \
    --dag runcheck_fail \
    --max-age 26h \
    --webhook "http://127.0.0.1:${WEBHOOK_PORT}/hook" \
    --state-file "$state" \
    || live=$?
  if [ "$live" -ne 1 ]; then
    echo "deduped run exit $live, expected 1" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  "${PY[@]}" - "$hooks" <<'PY'
import sys
lines = [line for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if len(lines) != 1:
    raise SystemExit(f"dedupe posted again; webhook lines={len(lines)}")
print("e2e: dedupe suppressed the second webhook")
PY
  kill "$CATCHER_PID" >/dev/null 2>&1 || true
  wait "$CATCHER_PID" 2>/dev/null || true
  CATCHER_PID=""

  local saved_password="${AIRFLOW_PASSWORD:-}"
  AIRFLOW_PASSWORD="definitely-wrong"
  AIRFLOW_TOKEN=""
  local auth=0
  run_cli "$out" "$err" -- \
    --url "$(base_url)" \
    --api-version "$api_flag" \
    --dag runcheck_ok \
    --timeout 20 \
    || auth=$?
  AIRFLOW_PASSWORD="$saved_password"
  if [ "$auth" -ne 1 ]; then
    echo "wrong password exit $auth, expected 1" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  if ! grep -q "Airflow authentication failed" "$out" "$err"; then
    echo "wrong password did not report an auth error" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  if grep -q "definitely-wrong" "$out" "$err"; then
    echo "wrong password was written to the output" >&2
    return 1
  fi
  echo "e2e: wrong password -> Airflow authentication failed"

  echo "stopping $NAME"
  docker stop "$NAME" >/dev/null
  AIRFLOW_TOKEN=""
  local down=0
  run_cli "$out" "$err" -- \
    --url "$(base_url)" \
    --api-version "$api_flag" \
    --dag runcheck_ok \
    --timeout 5 \
    || down=$?
  if [ "$down" -ne 1 ]; then
    echo "stopped server exit $down, expected 1" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  if ! grep -q "Airflow API unreachable" "$out" "$err"; then
    echo "stopped server did not report unreachable" >&2
    cat "$out" "$err" >&2
    return 1
  fi
  echo "e2e: stopped server -> Airflow API unreachable"
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
  for attempt in $(seq 1 90); do
    if docker exec "$NAME" test -s /opt/airflow/standalone_admin_password.txt; then
      password="$(docker exec "$NAME" cat /opt/airflow/standalone_admin_password.txt | tr -d '[:space:]')"
      break
    fi
    sleep 2
  done
  if [ -z "$password" ]; then
    echo "Airflow 2 did not write standalone_admin_password.txt" >&2
    return 1
  fi
  export AIRFLOW_USERNAME=admin
  export AIRFLOW_PASSWORD="$password"
  unset AIRFLOW_TOKEN || true
  wait_http "$(base_url)/health"
  trigger_and_wait v1
  run_cli_checks v1
  echo "e2e: Airflow 2.10.5 passed ($AIRFLOW2_IMAGE)"
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
  wait_http "$(base_url)/api/v2/version"
  local token_body="/tmp/runcheck-token.json"
  local token_code
  token_code="$(curl -s -o "$token_body" -w "%{http_code}" \
    -X POST "$(base_url)/auth/token" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin"}' || true)"
  if [ "$token_code" != "200" ] && [ "$token_code" != "201" ]; then
    echo "Airflow 3 token request failed (HTTP ${token_code:-none})" >&2
    cat "$token_body" >&2 || true
    return 1
  fi
  AIRFLOW_TOKEN="$("${PY[@]}" -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["access_token"])' "$token_body")"
  export AIRFLOW_TOKEN
  trigger_and_wait v2
  run_cli_checks v2
  echo "e2e: Airflow 3.4.0 passed ($AIRFLOW3_IMAGE)"
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
