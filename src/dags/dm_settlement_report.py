from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

from datetime import datetime


def load_dm_settlement_report():
    pg_hook = PostgresHook(
        postgres_conn_id="PG_WAREHOUSE_CONNECTION"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:

            # Удаляем месяцы, которые есть в DDS.
            # При повторном запуске они будут пересчитаны заново.
            cur.execute(
                """
                delete from cdm.dm_settlement_report
                where settlement_date in (
                    select distinct
                        date_trunc('month', t.date)::date
                    from dds.dm_orders o
                    join dds.dm_timestamps t
                        on t.id = o.timestamp_id
                    where o.order_status = 'CLOSED'
                );
                """
            )

            cur.execute(
                """
                insert into cdm.dm_settlement_report (
                    restaurant_id,
                    restaurant_name,
                    settlement_date,
                    orders_count,
                    orders_total_sum,
                    orders_bonus_payment_sum,
                    orders_bonus_granted_sum,
                    order_processing_fee,
                    restaurant_reward_sum
                )
                select
                    r.restaurant_id,
                    r.restaurant_name,
                    date_trunc('month', t.date)::date
                        as settlement_date,

                    count(distinct o.id)::integer
                        as orders_count,

                    coalesce(sum(f.total_sum), 0)::numeric
                        as orders_total_sum,

                    coalesce(sum(f.bonus_payment), 0)::numeric
                        as orders_bonus_payment_sum,

                    coalesce(sum(f.bonus_grant), 0)::numeric
                        as orders_bonus_granted_sum,

                    (
                        coalesce(sum(f.total_sum), 0) * 0.25
                    )::numeric
                        as order_processing_fee,

                    (
                        coalesce(sum(f.total_sum), 0)
                        - coalesce(sum(f.bonus_payment), 0)
                        - (
                            coalesce(sum(f.total_sum), 0) * 0.25
                        )
                    )::numeric
                        as restaurant_reward_sum

                from dds.dm_orders o

                join dds.dm_timestamps t
                    on t.id = o.timestamp_id

                join dds.dm_restaurants r
                    on r.id = o.restaurant_id

                join dds.fct_product_sales f
                    on f.order_id = o.id

                where o.order_status = 'CLOSED'

                group by
                    r.restaurant_id,
                    r.restaurant_name,
                    date_trunc('month', t.date)::date;
                """
            )

        conn.commit()


with DAG(
    dag_id="dm_settlement_report",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    load_report = PythonOperator(
        task_id="load_dm_settlement_report",
        python_callable=load_dm_settlement_report,
    )

    load_report