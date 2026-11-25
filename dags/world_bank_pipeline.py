"""The single local writer for World Bank pipeline runs."""
from datetime import datetime, timezone
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from world_bank_pipeline_support.gate import gate
from world_bank_pipeline_support.runtime import execute_job, task_failure

with DAG(
    dag_id="world_bank_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=os.environ.get("WORLD_BANK_PIPELINE_SCHEDULE") or None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": task_failure},
    is_paused_upon_creation=False,
) as dag:
    gate_task = PythonOperator(task_id="gate", python_callable=gate)
    previous = gate_task
    for job in ("ingest", "stage", "consumer", "curated", "dq", "export"):
        task = PythonOperator(task_id=job, python_callable=execute_job,
                              op_kwargs={"job": job}, do_xcom_push=False)
        previous >> task
        previous = task
