
from datetime import datetime
from typing import Any

from psycopg import Connection

from lib.dict_util import json2str


class PgSaver:
    def save_courier(
        self,
        conn: Connection,
        courier: dict,
    ) -> None:
        self._save_object(
            conn=conn,
            table_name="stg.couriers",
            object_id=str(courier["_id"]),
            object_value=courier,
        )

    def save_delivery(
        self,
        conn: Connection,
        delivery: dict,
    ) -> None:
        self._save_object(
            conn=conn,
            table_name="stg.deliveries",
            object_id=str(delivery["delivery_id"]),
            object_value=delivery,
        )

    @staticmethod
    def _save_object(
        conn: Connection,
        table_name: str,
        object_id: str,
        object_value: Any,
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    insert into {table_name} (
                        object_id,
                        object_value,
                        update_ts
                    )
                    values (
                        %(object_id)s,
                        %(object_value)s,
                        %(update_ts)s
                    )
                    on conflict (object_id) do update
                    set
                        object_value = excluded.object_value,
                        update_ts = excluded.update_ts;
                """,
                {
                    "object_id": object_id,
                    "object_value": json2str(object_value),
                    "update_ts": datetime.utcnow(),
                },
            )