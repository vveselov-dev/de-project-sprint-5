from logging import Logger
from typing import List
import json



from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EventObj(BaseModel):
    id: int
    event_ts: object
    event_type: str
    event_value: str


class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, last_loaded_id: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        event_ts,
                        event_type,
                        event_value
                    FROM outbox
                    WHERE id > %(last_loaded_id)s
                    ORDER BY id ASC;
                """,
                {
                    "last_loaded_id": last_loaded_id
                }
            )
            return cur.fetchall()


class EventsDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events (
                        id,
                        event_ts,
                        event_type,
                        event_value
                    )
                    VALUES (
                        %(id)s,
                        %(event_ts)s,
                        %(event_type)s,
                        %(event_value)s
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )


class EventsLoader:
    WF_KEY = "example_events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(
        self,
        pg_origin: PgConnect,
        pg_dest: PgConnect,
        log: Logger
    ) -> None:
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.pg_dest = pg_dest
        self.log = log



    def load_events(self) -> None:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(
                conn,
                self.WF_KEY
            )

            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_ID_KEY: -1
                    }
                )

            last_loaded_id = wf_setting.workflow_settings[
                self.LAST_LOADED_ID_KEY
            ]

            load_queue = self.origin.list_events(last_loaded_id)

            self.log.info(
                f"Found {len(load_queue)} events to load."
            )

            if not load_queue:
                self.log.info("Quitting.")
                return

            for event in load_queue:
                self.stg.insert_event(conn, event)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                event.id for event in load_queue
            )

            self.settings_repository.save_setting(
                conn,
                wf_setting.workflow_key,
                json.dumps(wf_setting.workflow_settings)
            )

            self.log.info(
                f"Load finished on "
                f"{wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}"
            )