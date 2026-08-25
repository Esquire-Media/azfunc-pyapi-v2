import os
from typing import Any

import pandas as pd

from libs.data import from_bind


_SQL_STATEMENT_TIMEOUT_MS = int(
    os.getenv(
        "NEIGHBORS_SQL_STATEMENT_TIMEOUT_MS",
        "120000",
    )
)

_SQL_LOCK_TIMEOUT_MS = int(
    os.getenv(
        "NEIGHBORS_SQL_LOCK_TIMEOUT_MS",
        "5000",
    )
)


_PARTITION_SQL = r"""
SELECT
    NULLIF(
        regexp_replace(
            street_number,
            '[^0-9]+',
            '',
            'g'
        ),
        ''
    )::int AS street_number,
    street_name,
    address,
    city,
    state,
    "zipCode",
    "plus4Code"
FROM utils.attom
WHERE "zipCode" = %s
  AND city = %s
  AND state = %s
  AND NULLIF(
        regexp_replace(
            street_number,
            '[^0-9]+',
            '',
            'g'
      ),
      ''
  )::bigint < 999999
"""


class AttomPartitionReader:
    """
    One DB session per Durable activity.

    Each partition uses the original simple CSZ query. There is no local
    retry loop: any database failure escapes the activity and Durable
    Functions retries the small activity batch.
    """

    def __init__(
        self,
        *,
        bind: str = "keystone",
    ) -> None:
        self.bind = bind
        self._provider = from_bind(bind)
        self._session = None

    def __enter__(
        self,
    ) -> "AttomPartitionReader":
        self._session = self._provider.connect()
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> None:
        self.close()

    def close(
        self,
    ) -> None:
        session = self._session
        self._session = None

        if session is None:
            return

        try:
            session.close()
        except Exception:
            pass

    def load_partition(
        self,
        *,
        city: str,
        state: str,
        zip_code: str,
    ) -> pd.DataFrame:
        if self._session is None:
            raise RuntimeError(
                "AttomPartitionReader must be used as a context manager"
            )

        session = self._session
        connection = session.connection()
        raw = connection.connection
        cursor = raw.cursor()

        try:
            cursor.execute(
                "SET LOCAL statement_timeout = "
                f"'{_SQL_STATEMENT_TIMEOUT_MS}ms'"
            )
            cursor.execute(
                "SET LOCAL lock_timeout = "
                f"'{_SQL_LOCK_TIMEOUT_MS}ms'"
            )
            cursor.execute(
                _PARTITION_SQL,
                (
                    zip_code,
                    city,
                    state,
                ),
            )

            rows = cursor.fetchall()
            columns = [
                descriptor[0]
                for descriptor in cursor.description
            ]
        finally:
            cursor.close()

        # End the read transaction and clear SET LOCAL values before the
        # next partition query on this same session.
        session.rollback()

        data = pd.DataFrame(
            rows,
            columns=columns,
        )

        if data.empty:
            return data

        data["street_number"] = pd.to_numeric(
            data["street_number"],
            errors="coerce",
        ).astype("Int64")

        data["street_name"] = (
            data["street_name"]
            .astype(str)
        )

        return data.drop_duplicates()
