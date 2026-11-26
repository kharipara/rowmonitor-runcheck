"""Passing DAG for scripts/e2e/run_airflow.sh. Imports work on Airflow 2.10 and 3.x."""

from datetime import datetime

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task


@dag(dag_id="runcheck_ok", start_date=datetime(2024, 1, 1), schedule=None, catchup=False)
def runcheck_ok():
    @task
    def succeed():
        return "ok"

    succeed()


runcheck_ok()
