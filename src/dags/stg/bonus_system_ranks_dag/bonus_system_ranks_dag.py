import logging

import pendulum
from airflow.decorators import dag, task

from stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from stg.bonus_system_ranks_dag.events_loader import EventsLoader
from lib import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'origin', 'example'],
    is_paused_upon_creation=True
)
def sprint5_example_stg_bonus_system_ranks_dag():

    dwh_pg_connect = ConnectionBuilder.pg_conn(
        "PG_WAREHOUSE_CONNECTION"
    )

    origin_pg_connect = ConnectionBuilder.pg_conn(
        "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
    )

    @task(task_id="ranks_load")
    def load_ranks():
        ranks_loader = RankLoader(
            origin_pg_connect,
            dwh_pg_connect,
            log
        )
        ranks_loader.load_ranks()

    @task(task_id="events_load")
    def load_events():
        events_loader = EventsLoader(
            origin_pg_connect,
            dwh_pg_connect,
            log
        )
        events_loader.load_events()

    ranks_task = load_ranks()
    events_task = load_events()

    ranks_task >> events_task


stg_bonus_system_ranks_dag = sprint5_example_stg_bonus_system_ranks_dag()