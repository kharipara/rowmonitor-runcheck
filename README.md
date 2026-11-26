# rowmonitor-runcheck

A free, open-source Python CLI that answers one question: did my last pipeline run work?

It checks the latest run of named Airflow DAGs over the Airflow REST API, or reads a dbt Core `target/run_results.json`, and can post a short alert to a Slack incoming webhook or any endpoint that accepts a JSON POST.

There is no account and no telemetry. Network calls go only to the Airflow base URL you pass and the webhooks you configure. An optional local state file remembers the last alert so the same problem is not posted on every run. That file is dedupe state, not a run history.

## Install

Python 3.9 or newer. The only runtime dependency is `requests`.

```bash
pip install .
rowmonitor-runcheck --help
```

Install from a checkout of this repository. Publishing to PyPI is a separate manual workflow and is not part of CI.

## Check Airflow

```bash
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="..."
rowmonitor-runcheck airflow \
  --url https://airflow.example.com \
  --dag orders_daily \
  --dag billing_hourly \
  --max-age 26h \
  --slack-webhook "$SLACK_WEBHOOK_URL"
```

The command alerts when:

- the latest DAG run has state `failed`
- `--max-age` is set and the last successful run is older than that cutoff, or the DAG has never succeeded
- the API is unreachable, authentication fails, or the DAG is not found

`--max-age` takes one cutoff such as `30s`, `45m`, `26h`, or `2d`. A run that finished exactly that long ago is still inside the window. Omit `--max-age` to alert only on a failed latest run, an unreachable API, an authentication failure, or a missing DAG.

### Airflow 2.6 and newer

Uses `GET /api/v1/dags/{dag_id}/dagRuns` with HTTP basic auth from `AIRFLOW_USERNAME` and `AIRFLOW_PASSWORD`.

The Airflow API has to accept basic auth:

```text
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
```

```bash
rowmonitor-runcheck airflow \
  --url https://airflow.example.com \
  --api-version v1 \
  --dag orders_daily \
  --max-age 26h
```

### Airflow 3

Uses `GET /api/v2/dags/{dag_id}/dagRuns` with a bearer JWT.

With a username and password, the CLI calls `POST /auth/token` and sends the returned `access_token`. With `AIRFLOW_TOKEN` already set, that token is sent as `Authorization: Bearer` and `/auth/token` is not called. When both are set, the pre-issued token wins.

```bash
export AIRFLOW_TOKEN="..."
rowmonitor-runcheck airflow \
  --url https://airflow.example.com \
  --api-version v2 \
  --dag orders_daily \
  --max-age 26h \
  --webhook "$RUNCHECK_WEBHOOK_URL"
```

`--api-version` is `auto` by default, or `v1` / `v2` (the leading `v` is optional). Auto-detect requests `/api/v2/version`, then `/api/v1/version`. A v1 server that answers 401 on the version route and has no v2 version route is treated as v1.

Pass the webserver origin as `--url`. A trailing `/api/v1` or `/api/v2` is stripped. Credentials belong in the environment, not in the URL. The CLI does not write the username, password, token, or webhook URL into logs, alert bodies, or the state file.

### Managed Airflow

These products use their own login. This CLI does not call AWS, Google, or Astronomer APIs. If you already hold a bearer token, pass it as `AIRFLOW_TOKEN`.

**Amazon MWAA.** MWAA does not accept Airflow basic auth or `POST /auth/token` with an Airflow username and password. Access is the `airflow:InvokeRestApi` AWS API, or a short-lived web login token exchanged for a session JWT. Airflow 3 on MWAA expects `Authorization: Bearer <jwt>` on `/api/v2`. Mint that token with AWS, then pass it as `AIRFLOW_TOKEN` and set `--api-version v2`.

**Google Cloud Composer.** Composer uses its own auth backend. Airflow 2 speaks REST v1 and Airflow 3 speaks REST v2, and the credential is a Google OAuth or OIDC bearer token. Pass a token you already have as `AIRFLOW_TOKEN` for an Airflow 3 environment. Composer 2's default auth is not Airflow basic auth.

**Astronomer.** Astronomer Software and Astro use a deployment API key or token. Airflow 3 on Astro is `/api/v2`. Pass that token as `AIRFLOW_TOKEN`.

Self-hosted Airflow 2.6 or newer with basic auth, and self-hosted Airflow 3 whose auth manager implements `POST /auth/token`, are the setups this command is built for.

## Check dbt Core

dbt Cloud is not supported. The command reads a `run_results.json` that dbt Core has already written:

```bash
rowmonitor-runcheck dbt \
  --run-results target/run_results.json \
  --slack-webhook "$SLACK_WEBHOOK_URL"
```

A node whose status is `error`, `fail`, or `runtime error` raises an alert. `--fail-on warn` also alerts on `warn`.

A node skipped because an upstream node failed is included. dbt Core 1.12 records that skip with status `skipped`, an empty `message`, and `compiled: false`. Older artifacts that describe the upstream failure in `message`, including an ephemeral-model failure, are included too, as is a `--fail-fast` skip (`Skipping due to fail_fast`). A skip that carries some other message is left alone. `pass`, `success`, and `no-op` are quiet.

Compiled SQL is not copied into the alert. The node id, status, and dbt's message are.

### Run it after dbt build

dbt's `on-run-end` hook runs SQL inside the warehouse. It cannot start this process. The working pattern is a shell wrapper immediately after `dbt build`, `dbt run`, or `dbt test`.

`dbt build && rowmonitor-runcheck ...` skips the alert whenever dbt exits non-zero, which is the run you most want to hear about. dbt still writes `run_results.json` when a model or test fails, as long as the invocation gets far enough to execute nodes. If dbt cannot start, there may be no new file and this CLI exits 2.

```bash
#!/bin/sh
set -u
dbt build "$@"
dbt_status=$?
rowmonitor-runcheck dbt --run-results target/run_results.json
check_status=$?
if [ "$dbt_status" -ne 0 ]; then
  exit "$dbt_status"
fi
exit "$check_status"
```

`tests/fixtures/dbt/run_results.json` is a real artifact from dbt Core 1.12.7 and dbt-duckdb 1.11.1, produced by `scripts/dbt-fixture`. That project has a model that errors, a downstream model skipped because of it, a failing `accepted_values` test, a warning test, and two passing tests. Regenerate the fixture with `scripts/dbt-fixture/generate.sh` when those packages are installed. `dbt build` there exits 1 on purpose.

## Alerts

`--dry-run` prints one JSON document and does not POST. `slack` is the exact `{"text": ...}` body when a Slack webhook is configured and a problem was found, otherwise null. `webhook` is the exact generic body when a generic webhook is configured and a problem was found, otherwise null. `preview` is always the generic body, so a dry run with no destination still shows the alert. `would_post` is true only when a real run would POST. `deduped` means the state file already holds this problem set, so a real run would not POST.

A real run prints one summary line per problem. When every check passes it prints a short ok line and does not POST. A success is not sent as a recovery message.

Slack text from the CLI looks like this:

```text
rowmonitor-runcheck: 2 problems
• orders_daily: latest run failed (run scheduled__2026-11-24T00:00:00+00:00, ended 2026-11-24T00:12:00+00:00)
• billing_hourly: no successful run

Want run history, late vs never-arrived detection and table-level checks? Row Monitor watches the data itself: https://rowmonitor.com/airflow?utm_source=runcheck&utm_medium=cli
Sign up: https://rowmonitor.com/signup?utm_source=runcheck&utm_medium=cli
```

The generic webhook receives:

```json
{
  "source": "rowmonitor-runcheck",
  "version": "0.1.0",
  "check": "airflow",
  "ok": false,
  "generated_at": "2026-11-25T12:00:00+00:00",
  "base_url": "https://airflow.example.com",
  "api_version": "v2",
  "problems": [
    {
      "code": "latest_failed",
      "summary": "orders_daily: latest run failed (run scheduled__2026-11-24T00:00:00+00:00, ended 2026-11-24T00:12:00+00:00)",
      "check": "airflow",
      "dag_id": "orders_daily",
      "dag_run_id": "scheduled__2026-11-24T00:00:00+00:00",
      "state": "failed",
      "start_date": "2026-11-24T00:00:02+00:00",
      "end_date": "2026-11-24T00:12:00+00:00",
      "logical_date": "2026-11-24T00:00:00+00:00"
    }
  ]
}
```

A dbt problem carries `unique_id`, `status`, and `message`, plus `run_results` and `invocation_id` on the document. Problem codes are `latest_failed`, `success_too_old`, `no_success`, `api_unreachable`, `auth_failed`, `dag_not_found`, `api_error`, `dbt_failed`, `dbt_warn`, and `dbt_skipped_upstream`.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | Every check passed. |
| 1 | One or more problems were found. The alert was sent, suppressed as a duplicate, or skipped because no webhook was configured. |
| 2 | Usage or configuration is wrong, or a configured webhook could not be delivered. |

## Environment

| Variable | Purpose |
| --- | --- |
| `AIRFLOW_USERNAME` | Basic auth on Airflow 2, or the `/auth/token` exchange on Airflow 3. |
| `AIRFLOW_PASSWORD` | Paired with the username. |
| `AIRFLOW_TOKEN` | Pre-issued bearer token for Airflow 3. |
| `SLACK_WEBHOOK_URL` | Used when `--slack-webhook` is omitted. |
| `RUNCHECK_WEBHOOK_URL` | Used when `--webhook` is omitted. |
| `RUNCHECK_STATE_FILE` | Used when `--state-file` is omitted. |

A flag wins over the matching environment variable. `--timeout` defaults to 30 seconds.

## Dedupe

`--state-file path` writes the fingerprints of the last alert to a JSON file created with mode `0600`. The same set on the next run is not posted again. The process still exits 1, and stderr says the alert was suppressed. A later clean run clears the file. Delete the file to force the next problem to send.

The file stores those fingerprints only. It does not store credentials, webhook URLs, or a history of runs.

## Outside this tool

Run history, late versus never-arrived detection, slow-run baselines, table or row checks, a dashboard, team alert routing, DAG-to-table mapping, and plain-English explanations are outside this CLI.

Want run history, late vs never-arrived detection and table-level checks? Row Monitor watches the data itself: https://rowmonitor.com/airflow?utm_source=runcheck&utm_medium=readme

Sign up: https://rowmonitor.com/signup?utm_source=runcheck&utm_medium=readme

## Live Airflow check

Pull requests run `scripts/e2e/run_airflow.sh` on GitHub-hosted runners, which have Docker. The matrix is the official `apache/airflow:2.10.5` and `apache/airflow:3.4.0` images, each started with `airflow standalone`. The script triggers `runcheck_ok` and `runcheck_fail`, then checks this CLI: the failing DAG exits 1 and names that DAG, the passing DAG exits 0, a wrong password reports an authentication failure, and a stopped server reports the API as unreachable. It also posts once to a local webhook catcher and checks that a second run is deduped. The same workflow runs `scripts/e2e/check_dbt_fixture.py` against `tests/fixtures/dbt/run_results.json`.

Run the same Airflow check on your own machine with Docker:

```bash
pip install -e .
bash scripts/e2e/run_airflow.sh
```

Override the images with `AIRFLOW2_IMAGE` and `AIRFLOW3_IMAGE`.

## Development

```bash
pip install -e ".[dev]"
ruff check .
ruff format --check .
pytest
```

## License

MIT. See [LICENSE](LICENSE).
