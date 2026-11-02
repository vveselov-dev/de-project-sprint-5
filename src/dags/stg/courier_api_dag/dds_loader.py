from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook


PG_CONNECTION = "PG_WAREHOUSE_CONNECTION"


def load_couriers_to_dds() -> None:
    sql_path = Path(__file__).with_name("load_couriers_to_dds.sql")
    sql = sql_path.read_text(encoding="utf-8")

    hook = PostgresHook(postgres_conn_id=PG_CONNECTION)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            print(f"Courier rows inserted or updated: {cur.rowcount}")

        conn.commit()

    print("DDS courier load completed.")


def load_deliveries_to_dds() -> None:
    sql_path = Path(__file__).with_name("load_deliveries_to_dds.sql")
    sql = sql_path.read_text(encoding="utf-8")

    hook = PostgresHook(postgres_conn_id=PG_CONNECTION)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            print(f"Delivery rows inserted or updated: {cur.rowcount}")

        conn.commit()

    print("DDS deliveries load completed.")