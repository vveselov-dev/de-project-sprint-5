from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib.pg_connect import ConnectionBuilder
from stg.courier_api_dag.api_reader import ApiReader
from stg.courier_api_dag.courier_loader import CourierLoader
from stg.courier_api_dag.pg_saver import PgSaver
from stg.courier_api_dag.dds_loader import (
    load_couriers_to_dds,
    load_deliveries_to_dds,
)

with DAG(
    dag_id="stg_courier_api_load",
    description="Load couriers and recent deliveries from Courier API into STG and DDS",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["stg", "dds", "courier"],
) as dag:

    def load_courier_data() -> dict:
        import logging

        logger = logging.getLogger("courier_api_loader")

        api_reader = ApiReader(logger)
        pg_dest = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
        pg_saver = PgSaver()

        loader = CourierLoader(
            api_reader=api_reader,
            pg_dest=pg_dest,
            pg_saver=pg_saver,
            logger=logger,
        )

        return loader.run_copy()

    load_courier_data_task = PythonOperator(
        task_id="load_courier_data",
        python_callable=load_courier_data,
    )

    load_couriers_to_dds_task = PythonOperator(
        task_id="load_couriers_to_dds",
        python_callable=load_couriers_to_dds,
    )

    load_deliveries_to_dds_task = PythonOperator(
        task_id="load_deliveries_to_dds",
        python_callable=load_deliveries_to_dds,
    )

    load_courier_data_task >> load_couriers_to_dds_task >> load_deliveries_to_dds_task

