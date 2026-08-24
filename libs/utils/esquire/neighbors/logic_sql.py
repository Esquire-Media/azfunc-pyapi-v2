import json
import os
import time
from typing import Any, Optional

import pandas as pd
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError

from libs.data import from_bind


_SQL_STATEMENT_TIMEOUT_MS = int(
    os.getenv("NEIGHBORS_SQL_STATEMENT_TIMEOUT_MS", "20000")
)
_SQL_LOCK_TIMEOUT_MS = int(
    os.getenv("NEIGHBORS_SQL_LOCK_TIMEOUT_MS", "5000")
)
_SQL_RETRY_ATTEMPTS = int(
    os.getenv("NEIGHBORS_SQL_RETRY_ATTEMPTS", "2")
)
_SQL_RETRY_BACKOFF_SECONDS = float(
    os.getenv("NEIGHBORS_SQL_RETRY_BACKOFF_SECONDS", "0.5")
)


_NEIGHBOR_SQL = r"""
WITH base AS (
    SELECT
        x.partition_ord,
        x.source_ord,
        x.city,
        x.state,
        x.zip_code,
        x.street_name,
        x.base_street_num
    FROM jsonb_to_recordset(%s::jsonb) AS x(
        partition_ord integer,
        source_ord integer,
        city text,
        state text,
        zip_code text,
        street_name text,
        base_street_num numeric
    )
    WHERE x.base_street_num IS NOT NULL
),

requested_street_variants AS (
    /*
     * Source street names have already been normalized in Python.
     *
     * For numeric ordinal streets, current Python maps all of:
     *     1, 1ST, 1ND, 1RD, 1TH
     * to the normalized street name "1".
     *
     * Building those variants here lets us discard unrequested ATTOM
     * streets before street-number regexp parsing, DISTINCT, and ranking.
     */
    SELECT DISTINCT
        variants.partition_ord,
        variants.city,
        variants.state,
        variants.zip_code,
        variants.street_name_norm,
        variants.street_name_match
    FROM (
        SELECT
            b.partition_ord,
            b.city,
            b.state,
            b.zip_code,
            b.street_name AS street_name_norm,
            b.street_name AS street_name_match
        FROM base AS b

        UNION ALL

        SELECT
            b.partition_ord,
            b.city,
            b.state,
            b.zip_code,
            b.street_name AS street_name_norm,
            b.street_name || suffix.value AS street_name_match
        FROM base AS b
        CROSS JOIN (
            VALUES
                ('ST'),
                ('ND'),
                ('RD'),
                ('TH')
        ) AS suffix(value)
        WHERE b.street_name ~ '^[0-9]+$'
    ) AS variants
),

matched_attom AS (
    /*
     * Filter by CSZ and requested street first.
     *
     * COALESCE(..., 'None') reproduces pandas astype(str) for a SQL NULL
     * street_name before _normalize_street_name() is called.
     */
    SELECT
        s.partition_ord,
        s.street_name_norm,
        COALESCE(a.street_name::text, 'None') AS raw_street_name,
        a.street_number::text AS raw_street_number,
        a.address,
        a.city,
        a.state,
        a."zipCode",
        a."plus4Code"
    FROM requested_street_variants AS s
    JOIN utils.attom AS a
      ON a."zipCode" = s.zip_code
     AND a.city = s.city
     AND a.state = s.state
     AND upper(
            regexp_replace(
                COALESCE(a.street_name::text, 'None'),
                '^[[:space:]]+|[[:space:]]+$',
                '',
                'g'
            )
         ) = s.street_name_match
),

attom_raw AS (
    /*
     * Equivalent to load_estated_data_db() street-number conversion,
     * but only after CSZ + street filtering.
     */
    SELECT
        m.partition_ord,
        m.street_name_norm,
        parsed.digits::integer AS street_number_int,
        m.raw_street_name,
        m.address,
        m.city,
        m.state,
        m."zipCode",
        m."plus4Code"
    FROM matched_attom AS m
    CROSS JOIN LATERAL (
        SELECT NULLIF(
            regexp_replace(
                m.raw_street_number,
                '[^0-9]+',
                '',
                'g'
            ),
            ''
        ) AS digits
    ) AS parsed
    WHERE parsed.digits IS NOT NULL
      AND parsed.digits::bigint < 999999
),

attom_dedup AS (
    /*
     * Equivalent to load_estated_data_db().drop_duplicates().
     *
     * raw_street_name stays in the key because current Python performs
     * this dedupe BEFORE ordinal street-name normalization.
     */
    SELECT DISTINCT
        partition_ord,
        street_name_norm,
        street_number_int,
        raw_street_name,
        address,
        city,
        state,
        "zipCode",
        "plus4Code"
    FROM attom_raw
),

ranked AS (
    /*
     * Equivalent to:
     *
     *     data.sort_values("street_number").reset_index(drop=True)
     *
     * candidate_pos is the zero-based DataFrame index.
     *
     * Ordering among identical street_number values is intentionally
     * unspecified, matching the current Python/database path.
     */
    SELECT
        d.*,
        row_number() OVER (
            PARTITION BY
                d.partition_ord,
                d.street_name_norm
            ORDER BY
                d.street_number_int
        ) - 1 AS candidate_pos,
        count(*) OVER (
            PARTITION BY
                d.partition_ord,
                d.street_name_norm
        ) AS street_count
    FROM attom_dedup AS d
),

street_sizes AS (
    SELECT DISTINCT
        partition_ord,
        street_name_norm,
        street_count
    FROM ranked
),

base_bounds AS (
    /*
     * Intentionally reproduces current find_neighbors_for_street().
     *
     * Current code constructs:
     *
     *     pd.Series(data.index, index=data["street_number"])
     *
     * and then calls searchsorted(base_num), so searchsorted operates
     * on Series VALUES [0, 1, 2, ...], not on street numbers.
     *
     * The equivalent insertion position is ceil(base_street_num),
     * clamped into [0, street_count].
     */
    SELECT
        b.partition_ord,
        b.source_ord,
        b.street_name,
        b.base_street_num,
        s.street_count,
        least(
            greatest(
                ceil(b.base_street_num)::bigint,
                0::bigint
            ),
            s.street_count
        ) AS base_idx
    FROM base AS b
    JOIN street_sizes AS s
      ON s.partition_ord = b.partition_ord
     AND s.street_name_norm = b.street_name
),

selected AS (
    /*
     * Equivalent to:
     *
     *     start = max(0, base_idx - N)
     *     end = min(len(data), base_idx + N)
     *     range(start, end)
     */
    SELECT
        b.partition_ord,
        b.source_ord,
        b.street_name,
        b.base_street_num,
        r.candidate_pos,
        r.street_number_int,
        r.address,
        r.city,
        r.state,
        r."zipCode",
        r."plus4Code"
    FROM base_bounds AS b
    JOIN ranked AS r
      ON r.partition_ord = b.partition_ord
     AND r.street_name_norm = b.street_name
     AND r.candidate_pos >= greatest(
            0::bigint,
            b.base_idx - %s
         )
     AND r.candidate_pos < least(
            b.street_count,
            b.base_idx + %s
         )
),

parity_filtered AS (
    /*
     * Current Python applies same-side parity AFTER selecting the window.
     *
     * PostgreSQL mod() follows the sign of the dividend, while Python's
     * modulo with positive 2 returns a non-negative remainder. Normalize both
     * sides so negative source house numbers preserve Python semantics.
     */
    SELECT
        s.*
    FROM selected AS s
    WHERE
        %s::boolean = FALSE
        OR mod(
            mod(s.street_number_int::numeric, 2) + 2,
            2
        ) = mod(
            mod(s.base_street_num, 2) + 2,
            2
        )
),

deduped AS (
    /*
     * Equivalent to:
     *
     *     pd.concat(group_results).drop_duplicates()
     *
     * Current Python performs this while street_number and normalized
     * street_name still exist, before projecting the final five columns.
     */
    SELECT
        p.*,
        row_number() OVER (
            PARTITION BY
                p.partition_ord,
                p.street_number_int,
                p.street_name,
                p.address,
                p.city,
                p.state,
                p."zipCode",
                p."plus4Code"
            ORDER BY
                p.source_ord,
                p.candidate_pos
        ) AS duplicate_rank
    FROM parity_filtered AS p
)

SELECT
    address,
    city,
    state,
    "zipCode",
    "plus4Code"
FROM deduped
WHERE duplicate_rank = 1
ORDER BY
    partition_ord,
    street_name COLLATE "C",
    source_ord,
    candidate_pos
"""


def _postgres_error_code(
    exc: BaseException,
) -> Optional[str]:
    candidates = [
        exc,
        getattr(exc, "orig", None),
        getattr(exc, "__cause__", None),
    ]

    for candidate in candidates:
        if candidate is None:
            continue

        code = (
            getattr(candidate, "sqlstate", None)
            or getattr(candidate, "pgcode", None)
        )

        if code:
            return str(code)

    return None


def _is_transient_db_error(
    exc: BaseException,
) -> bool:
    if isinstance(
        exc,
        (
            SQLAlchemyTimeoutError,
            OperationalError,
        ),
    ):
        return True

    if isinstance(exc, DBAPIError) and getattr(
        exc,
        "connection_invalidated",
        False,
    ):
        return True

    code = _postgres_error_code(exc)

    if code:
        if code.startswith("08"):
            return True

        return code in {
            "40001",  # serialization_failure
            "40P01",  # deadlock_detected
            "55P03",  # lock_not_available / lock timeout
            "57014",  # query_canceled / statement_timeout
            "57P01",  # admin_shutdown
            "57P02",  # crash_shutdown
            "57P03",  # cannot_connect_now
        }

    # Raw DBAPI exceptions can surface because this module uses the
    # underlying cursor rather than SQLAlchemy execute().
    return exc.__class__.__name__ in {
        "OperationalError",
        "InterfaceError",
        "ConnectionError",
        "TimeoutError",
    }


class NeighborSqlReader:
    """
    Reuse one DB session for all SQL chunks in one Durable activity.

    Session acquisition is lazy and occurs inside find_neighbors()'s
    retry boundary. A transient failure discards the current session
    and retries only the failed SQL chunk using a fresh connection.
    """

    def __init__(
        self,
        *,
        bind: str = "keystone",
        attempts: int = _SQL_RETRY_ATTEMPTS,
        retry_backoff_seconds: float = _SQL_RETRY_BACKOFF_SECONDS,
    ) -> None:
        self.bind = bind
        self.attempts = max(1, attempts)
        self.retry_backoff_seconds = max(
            0.0,
            retry_backoff_seconds,
        )

        self._provider = from_bind(bind)
        self._session = None

    def __enter__(
        self,
    ) -> "NeighborSqlReader":
        # Deliberately lazy. Connection/pool acquisition must happen
        # inside the retry loop in find_neighbors().
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc: Any,
        traceback: Any,
    ) -> None:
        self.close()

    def _ensure_session(
        self,
    ) -> None:
        if self._session is None:
            self._session = self._provider.connect()

    def _rollback_safely(
        self,
    ) -> None:
        if self._session is None:
            return

        try:
            self._session.rollback()
        except Exception:
            pass

    def _discard_session(
        self,
    ) -> None:
        session = self._session
        self._session = None

        if session is None:
            return

        try:
            session.rollback()
        except Exception:
            pass

        try:
            session.close()
        except Exception:
            pass

    def close(
        self,
    ) -> None:
        self._discard_session()

    def _execute_once(
        self,
        *,
        requests: list[dict[str, Any]],
        n_per_side: int,
        same_side_only: bool,
    ) -> pd.DataFrame:
        self._ensure_session()

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
                _NEIGHBOR_SQL,
                (
                    json.dumps(
                        requests,
                        separators=(",", ":"),
                    ),
                    n_per_side,
                    n_per_side,
                    same_side_only,
                ),
            )

            rows = cursor.fetchall()
            columns = [
                descriptor[0]
                for descriptor in cursor.description
            ]

        finally:
            cursor.close()

        # End the read-only transaction and clear SET LOCAL values.
        session.rollback()

        if not rows:
            return pd.DataFrame(columns=columns)

        return pd.DataFrame(
            rows,
            columns=columns,
        )

    def find_neighbors(
        self,
        *,
        requests: list[dict[str, Any]],
        n_per_side: int,
        same_side_only: bool,
    ) -> pd.DataFrame:
        if not requests:
            return pd.DataFrame(
                columns=[
                    "address",
                    "city",
                    "state",
                    "zipCode",
                    "plus4Code",
                ]
            )

        for attempt in range(
            1,
            self.attempts + 1,
        ):
            try:
                return self._execute_once(
                    requests=requests,
                    n_per_side=n_per_side,
                    same_side_only=same_side_only,
                )

            except Exception as exc:
                if not _is_transient_db_error(exc):
                    self._rollback_safely()
                    raise

                # A timeout or connection error can leave the current
                # transaction/connection unusable. Always reconnect.
                self._discard_session()

                if attempt >= self.attempts:
                    raise

                time.sleep(
                    self.retry_backoff_seconds
                    * (2 ** (attempt - 1))
                )

        raise RuntimeError(
            "Neighbor SQL retry loop exhausted unexpectedly"
        )
