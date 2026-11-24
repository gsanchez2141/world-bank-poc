"""Manual parent for a single writer backfill run."""
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from world_bank_pipeline_support.backfill import trigger_and_wait

with DAG(
    dag_id="world_bank_backfill",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    is_paused_upon_creation=False,
) as dag:
    PythonOperator(task_id="trigger_and_wait", python_callable=trigger_and_wait, do_xcom_push=False)
