
from logging import Logger

from lib.pg_connect import PgConnect
from stg.courier_api_dag.api_reader import ApiReader
from stg.courier_api_dag.pg_saver import PgSaver


class CourierLoader:
    def __init__(
        self,
        api_reader: ApiReader,
        pg_dest: PgConnect,
        pg_saver: PgSaver,
        logger: Logger,
    ) -> None:
        self.api_reader = api_reader
        self.pg_dest = pg_dest
        self.pg_saver = pg_saver
        self.log = logger

    def run_copy(self) -> dict:
        couriers = self.api_reader.get_all(
            endpoint="couriers",
            sort_field="id",
        )

        deliveries = self.api_reader.get_deliveries_last_7_days()

        self.log.info(
            "Received %s couriers and %s deliveries",
            len(couriers),
            len(deliveries),
        )

        with self.pg_dest.connection() as conn:
            for courier in couriers:
                self.pg_saver.save_courier(conn, courier)

            for delivery in deliveries:
                self.pg_saver.save_delivery(conn, delivery)

        result = {
            "couriers": len(couriers),
            "deliveries": len(deliveries),
        }

        self.log.info("Finished loading API data: %s", result)
        return result