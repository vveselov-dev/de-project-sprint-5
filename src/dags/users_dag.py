from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


PG_CONNECTION = "PG_WAREHOUSE_CONNECTION"

WORKFLOW_KEY = "dds_dm_users_load"

DEFAULT_LAST_LOADED_TS = "2022-01-01T00:00:00"

PRODUCTS_WORKFLOW_KEY = "dds_dm_products_load"


def load_dm_users():
    hook = PostgresHook(postgres_conn_id=PG_CONNECTION)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            # 1. Получаем последний успешно загруженный timestamp
            cur.execute(
                """
                select workflow_settings::json ->> 'last_loaded_ts'
                from dds.srv_wf_settings
                where workflow_key = %s
                order by id desc
                limit 1;
                """,
                (WORKFLOW_KEY,)
            )

            result = cur.fetchone()

            if result is None or result[0] is None:
                last_loaded_ts = DEFAULT_LAST_LOADED_TS
            else:
                last_loaded_ts = result[0]

            print(f"Last loaded timestamp: {last_loaded_ts}")

            # 2. Загружаем пользователей из STG в DDS
            cur.execute(
                """
                insert into dds.dm_users (
                    user_id,
                    user_name,
                    user_login
                )
                select
                    s.object_id,
                    s.object_value::json ->> 'name',
                    s.object_value::json ->> 'login'
                from stg.ordersystem_users s
                where s.update_ts > %s::timestamp
                  and not exists (
                      select 1
                      from dds.dm_users d
                      where d.user_id = s.object_id
                  );
                """,
                (last_loaded_ts,)
            )

            inserted_rows = cur.rowcount

            print(f"Inserted rows: {inserted_rows}")

            # 3. Получаем максимальный timestamp из обработанных данных
            cur.execute(
                """
                select max(update_ts)
                from stg.ordersystem_users
                where update_ts > %s::timestamp;
                """,
                (last_loaded_ts,)
            )

            result = cur.fetchone()
            max_update_ts = result[0]

            # Если новых данных не было, cursor не меняем
            if max_update_ts is None:
                print("No new users found.")
                conn.commit()
                return

            print(f"New last loaded timestamp: {max_update_ts}")

            # 4. Сохраняем новый cursor
            workflow_settings = (
                '{"last_loaded_ts": "' +
                max_update_ts.isoformat() +
                '"}'
            )

            cur.execute(
                """
                insert into dds.srv_wf_settings (
                    workflow_key,
                    workflow_settings
                )
                values (%s, %s::json)
                on conflict (workflow_key)
                do update set
                workflow_settings = excluded.workflow_settings;
                """,
                (
                    WORKFLOW_KEY,
                    workflow_settings
                )
            )

        conn.commit()

        print("DDS dm_users load completed successfully.")

def load_dm_restaurants():
    hook = PostgresHook(postgres_conn_id=PG_CONNECTION)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            # 1. Получаем последний загруженный timestamp
            cur.execute(
                """
                select workflow_settings::json ->> 'last_loaded_ts'
                from dds.srv_wf_settings
                where workflow_key = %s
                order by id desc
                limit 1;
                """,
                ("dds_dm_restaurants_load",)
            )

            result = cur.fetchone()

            if result is None or result[0] is None:
                last_loaded_ts = DEFAULT_LAST_LOADED_TS
            else:
                last_loaded_ts = result[0]

            print(f"Last loaded timestamp for restaurants: {last_loaded_ts}")

            # 2. Загружаем новые рестораны
            cur.execute(
                """
                insert into dds.dm_restaurants (
                    restaurant_id,
                    restaurant_name,
                    active_from,
                    active_to
                )
                select
                    s.object_id,
                    s.object_value::json ->> 'name',
                    '2022-01-01 00:00:00'::timestamp,
                    '2099-12-31 00:00:00'::timestamp
                from stg.ordersystem_restaurants s
                where s.update_ts > %s::timestamp
                  and not exists (
                      select 1
                      from dds.dm_restaurants d
                      where d.restaurant_id = s.object_id
                  );
                """,
                (last_loaded_ts,)
            )

            inserted_rows = cur.rowcount

            print(f"Inserted restaurants: {inserted_rows}")

            # 3. Получаем максимальный update_ts
            cur.execute(
                """
                select max(update_ts)
                from stg.ordersystem_restaurants
                where update_ts > %s::timestamp;
                """,
                (last_loaded_ts,)
            )

            result = cur.fetchone()
            max_update_ts = result[0]

            if max_update_ts is None:
                print("No new restaurants found.")
                conn.commit()
                return

            print(f"New restaurant last loaded timestamp: {max_update_ts}")

            # 4. Сохраняем cursor
            workflow_settings = (
                '{"last_loaded_ts": "' +
                max_update_ts.isoformat() +
                '"}'
            )

            cur.execute(
                """
                insert into dds.srv_wf_settings (
                    workflow_key,
                    workflow_settings
                )
                values (%s, %s::json)
                on conflict (workflow_key)
                do update set
                workflow_settings = excluded.workflow_settings;
                """,
                (
                    "dds_dm_restaurants_load",
                    workflow_settings
                )
            )

        conn.commit()

        print("DDS dm_restaurants load completed successfully.")


def load_dm_timestamps():
    hook = PostgresHook(
        postgres_conn_id=PG_CONNECTION
    )

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                select
                    workflow_settings::json ->> 'last_loaded_ts'
                from dds.srv_wf_settings
                where workflow_key = %s
                order by id desc
                limit 1;
            """, ("dds_dm_timestamps_load",))

            result = cur.fetchone()

            if result is None or result[0] is None:
                last_loaded_ts = DEFAULT_LAST_LOADED_TS
            else:
                last_loaded_ts = result[0]

            print(f"Last loaded timestamp: {last_loaded_ts}")

            cur.execute("""
                insert into dds.dm_timestamps (
                    ts,
                    year,
                    month,
                    day,
                    date,
                    time
                )
                select distinct
                    (status_item ->> 'dttm')::timestamp as ts,
                    extract(
                        year from (status_item ->> 'dttm')::timestamp
                    )::int as year,
                    extract(
                        month from (status_item ->> 'dttm')::timestamp
                    )::int as month,
                    extract(
                        day from (status_item ->> 'dttm')::timestamp
                    )::int as day,
                    (status_item ->> 'dttm')::timestamp::date as date,
                    (status_item ->> 'dttm')::timestamp::time as time
                from stg.ordersystem_orders s
                cross join lateral json_array_elements(
                    s.object_value::json -> 'statuses'
                ) as status_item
                where status_item ->> 'status' in ('CLOSED', 'CANCELLED')
                  and (status_item ->> 'dttm')::timestamp
                      > %s::timestamp;
            """, (last_loaded_ts,))

            inserted_rows = cur.rowcount

            print(f"Inserted timestamps: {inserted_rows}")

            cur.execute("""
                select max(
                    (status_item ->> 'dttm')::timestamp
                )
                from stg.ordersystem_orders s
                cross join lateral json_array_elements(
                    s.object_value::json -> 'statuses'
                ) as status_item
                where status_item ->> 'status' in ('CLOSED', 'CANCELLED')
                  and (status_item ->> 'dttm')::timestamp
                      > %s::timestamp;
            """, (last_loaded_ts,))

            result = cur.fetchone()
            max_update_ts = result[0]

            if max_update_ts is None:
                print("No new timestamps found.")
                conn.commit()
                return

            print(
                f"New timestamps last loaded timestamp: "
                f"{max_update_ts}"
            )

            workflow_settings = (
                '{"last_loaded_ts": "'
                + max_update_ts.isoformat()
                + '"}'
            )

            cur.execute("""
                insert into dds.srv_wf_settings (
                    workflow_key,
                    workflow_settings
                )
                values (%s, %s::json)
                on conflict (workflow_key)
                do update set
                workflow_settings = excluded.workflow_settings;
            """, (
                "dds_dm_timestamps_load",
                workflow_settings
            ))

        conn.commit()

        print("DDS dm_timestamps load completed successfully.")

def load_dm_products():
    hook = PostgresHook(postgres_conn_id=PG_CONNECTION)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                select workflow_settings::json ->> 'last_loaded_ts'
                from dds.srv_wf_settings
                where workflow_key = %s
                order by id desc
                limit 1;
                """,
                (PRODUCTS_WORKFLOW_KEY,)
            )

            result = cur.fetchone()

            if result is None or result[0] is None:
                last_loaded_ts = DEFAULT_LAST_LOADED_TS
            else:
                last_loaded_ts = result[0]

            cur.execute(
                """
                insert into dds.dm_products (
                    product_id,
                    product_name,
                    product_price,
                    active_from,
                    active_to,
                    restaurant_id
                )
                select
                    product_id,
                    product_name,
                    product_price,
                    active_from,
                    '2099-12-31 00:00:00'::timestamp,
                    restaurant_id
                from (
                    select
                        item ->> 'id' as product_id,
                        item ->> 'name' as product_name,
                        (item ->> 'price')::numeric as product_price,
                        s.update_ts as active_from,
                        r.id as restaurant_id,
                        row_number() over (
                            partition by item ->> 'id'
                            order by s.update_ts
                        ) as rn
                    from stg.ordersystem_orders s
                    cross join lateral json_array_elements(
                        s.object_value::json -> 'order_items'
                    ) as item
                    join dds.dm_restaurants r
                        on r.restaurant_id =
                        s.object_value::json -> 'restaurant' ->> 'id'
                    where s.update_ts > %s::timestamp
                ) t
                where rn = 1;
                """,
                (last_loaded_ts,)
            )

            cur.execute(
                """
                select max(update_ts)
                from stg.ordersystem_orders
                where update_ts > %s::timestamp;
                """,
                (last_loaded_ts,)
            )

            result = cur.fetchone()

            if result is not None and result[0] is not None:
                new_last_loaded_ts = result[0]

                cur.execute(
                    """
                    insert into dds.srv_wf_settings (
                        workflow_key,
                        workflow_settings
                    )
                    values (
                        %s,
                        %s::json
                    )
                    on conflict (workflow_key)
                    do update set
                    workflow_settings = excluded.workflow_settings;
                    """,
                    (
                        PRODUCTS_WORKFLOW_KEY,
                        '{"last_loaded_ts": "' +
                        new_last_loaded_ts.isoformat() +
                        '"}'
                    )
                )

        conn.commit()

def load_dm_orders(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select workflow_settings
                from dds.srv_wf_settings
                where workflow_key = 'dds_dm_orders_load'
                order by id desc
                limit 1
                """
            )

            row = cur.fetchone()

            if row:
                last_loaded_ts = row[0]["last_loaded_ts"]
            else:
                last_loaded_ts = "2022-01-01T00:00:00"

            cur.execute(
                """
                insert into dds.dm_orders (
                    order_key,
                    order_status,
                    restaurant_id,
                    timestamp_id,
                    user_id
                )
                select
                    s.object_value::json ->> '_id',
                    s.object_value::json ->> 'final_status',
                    r.id,
                    t.id,
                    u.id
                from stg.ordersystem_orders s
                join dds.dm_restaurants r
                    on r.restaurant_id =
                       s.object_value::json -> 'restaurant' ->> 'id'
                    and r.active_from <=
                        (s.object_value::json ->> 'date')::timestamp
                    and (s.object_value::json ->> 'date')::timestamp <
                        r.active_to
                join dds.dm_users u
                    on u.user_id =
                       s.object_value::json -> 'user' ->> 'id'
                join dds.dm_timestamps t
                    on t.ts =
                       (s.object_value::json ->> 'date')::timestamp
                where s.update_ts > %s::timestamp;
                """,
                (last_loaded_ts,)
            )

            cur.execute(
                """
                select max(update_ts)
                from stg.ordersystem_orders
                where update_ts > %s::timestamp
                """,
                (last_loaded_ts,)
            )

            max_ts = cur.fetchone()[0]

            if max_ts:
                cur.execute(
                    """
                    insert into dds.srv_wf_settings (
                        workflow_key,
                        workflow_settings
                    )
                    values (
                        'dds_dm_orders_load',
                        json_build_object(
                            'last_loaded_ts',
                            %s
                        )
                    )
                    on conflict (workflow_key)
                    do update set
                    workflow_settings = excluded.workflow_settings;
                    """,
                    (max_ts.isoformat(),)
                )

        conn.commit()

def load_fct_product_sales(**kwargs):
    pg_hook = PostgresHook(
        postgres_conn_id="PG_WAREHOUSE_CONNECTION"
    )

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                select workflow_settings
                from dds.srv_wf_settings
                where workflow_key = 'dds_fct_product_sales_load'
                order by id desc
                limit 1
            """)

            row = cur.fetchone()

            if row:
                last_loaded_ts = row[0]["last_loaded_ts"]
            else:
                last_loaded_ts = "2022-01-01T00:00:00"

            cur.execute("""
                insert into dds.fct_product_sales (
                    product_id,
                    order_id,
                    count,
                    price,
                    total_sum,
                    bonus_payment,
                    bonus_grant
                )
                select
                    p.id as product_id,
                    o.id as order_id,
                    (item ->> 'quantity')::int as count,
                    (item ->> 'price')::numeric as price,
                    (
                        (item ->> 'price')::numeric
                        * (item ->> 'quantity')::int
                    )::numeric as total_sum,
                    coalesce(bp.bonus_payment, 0)::numeric as bonus_payment,
                    coalesce(bp.bonus_grant, 0)::numeric as bonus_grant
                from stg.ordersystem_orders s

                join dds.dm_orders o
                    on o.order_key =
                    s.object_value::json ->> '_id'

                cross join lateral json_array_elements(
                    s.object_value::json -> 'order_items'
                ) as item

                join lateral (
                    select p.*
                    from dds.dm_products p
                    where p.product_id = item ->> 'id'
                    and date_trunc(
                        'second',
                        p.active_from
                    ) <= date_trunc(
                        'second',
                        (s.object_value::json ->> 'date')::timestamp
                    )
                    and date_trunc(
                        'second',
                        (s.object_value::json ->> 'date')::timestamp
                    ) < date_trunc(
                        'second',
                        p.active_to
                    )
                    order by p.active_from desc
                    limit 1
                ) p on true

                left join (
                    select
                        b.event_value::json ->> 'order_id' as order_id,
                        payment ->> 'product_id' as product_id,
                        sum(
                            (payment ->> 'bonus_payment')::numeric
                        ) as bonus_payment,
                        sum(
                            (payment ->> 'bonus_grant')::numeric
                        ) as bonus_grant
                    from stg.bonussystem_events b
                    cross join lateral json_array_elements(
                        b.event_value::json -> 'product_payments'
                    ) as payment
                    where b.event_type = 'bonus_transaction'
                    group by
                        b.event_value::json ->> 'order_id',
                        payment ->> 'product_id'
                ) bp
                    on bp.order_id =
                    s.object_value::json ->> '_id'
                    and bp.product_id =
                        item ->> 'id'

                where s.update_ts > %s::timestamp;
            """, (last_loaded_ts,))

            cur.execute("""
                select max(update_ts)
                from stg.ordersystem_orders
                where update_ts > %s::timestamp
            """, (last_loaded_ts,))

            max_ts = cur.fetchone()[0]

            if max_ts:
                cur.execute("""
                    insert into dds.srv_wf_settings (
                        workflow_key,
                        workflow_settings
                    )
                    values (
                        'dds_fct_product_sales_load',
                        json_build_object(
                            'last_loaded_ts',
                            %s
                        )
                    )
                    on conflict (workflow_key)
                    do update set
                    workflow_settings = excluded.workflow_settings;
                """, (max_ts.isoformat(),))

        conn.commit()

with DAG(
    dag_id="dds_dm_users",
    start_date=datetime(2022, 5, 5),
    schedule_interval="0/15 * * * *",
    catchup=False,
    tags=["sprint5", "dds", "users"],
) as dag:

    load_users = PythonOperator(
    task_id="load_dm_users",
    python_callable=load_dm_users,
)

load_restaurants = PythonOperator(
    task_id="load_dm_restaurants",
    python_callable=load_dm_restaurants,
)

load_timestamps = PythonOperator(
    task_id="load_dm_timestamps",
    python_callable=load_dm_timestamps,
)

load_products = PythonOperator(
    task_id="load_dm_products",
    python_callable=load_dm_products
)

load_orders = PythonOperator(
    task_id="load_dm_orders",
    python_callable=load_dm_orders
)

load_product_sales = PythonOperator(
    task_id="load_fct_product_sales",
    python_callable=load_fct_product_sales
)

load_users >> load_restaurants >> load_timestamps >> load_orders >> load_products >> load_product_sales