
import logging
import os
import urllib.parse
import urllib.request
import json

from datetime import datetime, timedelta


class ApiReader:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.base_url = os.environ["AIRFLOW_VAR_COURIER_API_URL"]
        self.headers = {
            "X-Nickname": os.environ["AIRFLOW_VAR_COURIER_API_NICKNAME"],
            "X-Cohort": os.environ["AIRFLOW_VAR_COURIER_API_COHORT"],
            "X-API-KEY": os.environ["AIRFLOW_VAR_COURIER_API_KEY"],
        }

    def _get_page(self, endpoint: str, params: dict) -> list:
        query = urllib.parse.urlencode(params)
        url = f"{self.base_url}/{endpoint}?{query}"

        request = urllib.request.Request(url, headers=self.headers)

        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))

    def get_all(self, endpoint: str, sort_field: str) -> list:
        limit = 50
        offset = 0
        result = []

        while True:
            page = self._get_page(
                endpoint,
                {
                    "sort_field": sort_field,
                    "sort_direction": "asc",
                    "limit": limit,
                    "offset": offset,
                },
            )

            if not page:
                break

            result.extend(page)
            self.logger.info(
                "API %s: received %s records, offset=%s",
                endpoint,
                len(page),
                offset,
            )

            if len(page) < limit:
                break

            offset += limit

        return result

    def get_deliveries_last_7_days(self) -> list:
        now = datetime.now()
        date_from = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        date_to = now.strftime("%Y-%m-%d %H:%M:%S")

        limit = 50
        offset = 0
        result = []

        while True:
            page = self._get_page(
                "deliveries",
                {
                    "from": date_from,
                    "to": date_to,
                    "sort_field": "delivery_id",
                    "sort_direction": "asc",
                    "limit": limit,
                    "offset": offset,
                },
            )

            if not page:
                break

            result.extend(page)
            self.logger.info(
                "API deliveries: received %s records, offset=%s",
                len(page),
                offset,
            )

            if len(page) < limit:
                break

            offset += limit

        return result