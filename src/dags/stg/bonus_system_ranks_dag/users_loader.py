from logging import Logger
from typing import List

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserObj(BaseModel):
    id: int
    order_user_id: str


class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self) -> List[UserObj]:
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_user_id
                    FROM users;
                """
            )
            objs = cur.fetchall()

        return objs


class UserDestRepository:

    def insert_user(self, conn: Connection, user: UserObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_users(id, order_user_id)
                    VALUES (%(id)s, %(order_user_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_user_id = EXCLUDED.order_user_id;
                """,
                {
                    "id": user.id,
                    "order_user_id": user.order_user_id
                },
            )


class UserLoader:

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = UsersOriginRepository(pg_origin)
        self.stg = UserDestRepository()
        self.log = log

    def load_users(self):
        with self.pg_dest.connection() as conn:

            users = self.origin.list_users()

            self.log.info(f"Found {len(users)} users to load.")

            # Полностью очищаем STG перед загрузкой
            with conn.cursor() as cur:
                cur.execute(
                    """
                        TRUNCATE TABLE stg.bonussystem_users;
                    """
                )

            # Загружаем актуальное состояние users
            for user in users:
                self.stg.insert_user(conn, user)

            self.log.info("Users load finished.")