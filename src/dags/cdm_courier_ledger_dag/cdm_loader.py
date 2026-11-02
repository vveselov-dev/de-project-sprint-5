from pathlib import Path

from airflow.providers.postgres.hooks.postgres import PostgresHook


PG_CONNECTION = "PG_WAREHOUSE_CONNECTION"


def load_courier_ledger() -> None:
    sql_path = Path(__file__).with_name("load_courier_ledger.sql")
    sql = sql_path.read_text(encoding="utf-8")

    hook = PostgresHook(postgres_conn_id=PG_CONNECTION)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            print(f"Courier ledger rows inserted or updated: {cur.rowcount}")

        conn.commit()

    print("CDM courier ledger load completed.")
