from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from cdm_courier_ledger_dag.cdm_loader import load_courier_ledger


with DAG(
    dag_id="cdm_courier_ledger",
    description="Build monthly courier payout ledger in CDM",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 1 * *",
    catchup=False,
    max_active_runs=1,
    tags=["cdm", "courier"],
) as dag:

    load_courier_ledger_task = PythonOperator(
        task_id="load_courier_ledger",
        python_callable=load_courier_ledger,
    )
