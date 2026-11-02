from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def load_ranks():
    source_hook = PostgresHook(
        postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
    )

    warehouse_hook = PostgresHook(
        postgres_conn_id='PG_WAREHOUSE_CONNECTION'
    )

    # Читаем полное состояние ranks из источника
    ranks = source_hook.get_records(
        sql='''
            select
                id,
                name,
                bonus_percent,
                min_payment_threshold
            from ranks
        '''
    )

    # Полностью заменяем содержимое STG
    warehouse_hook.run(
        'truncate table stg.bonussystem_ranks'
    )

    # Загружаем данные
    warehouse_hook.insert_rows(
        table='stg.bonussystem_ranks',
        rows=ranks,
        target_fields=[
            'id',
            'name',
            'bonus_percent',
            'min_payment_threshold'
        ]
    )


with DAG(
    dag_id='load_ranks',
    start_date=datetime(2026, 9, 1),
    schedule_interval='*/15 * * * *',
    catchup=False,
) as dag:

    load_ranks_task = PythonOperator(
        task_id='load_ranks',
        python_callable=load_ranks
    )