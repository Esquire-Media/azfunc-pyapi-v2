"""Shared address completion and standardization for sales/target matchback.

Autocomplete is the authoritative backend completion operation (including ZIP+4).
One SQL statement batches scalar autocomplete calls using unnest/LATERAL.
Only autocomplete misses are sent to addresses.geocode_address(text[]) in one batch;
its returned `addy` norm_addy is the normalized fallback result.

Targets may alternatively use geocode without autocomplete.
No ZIP+4 retries or Python street repair.
"""
from __future__ import annotations

import atexit
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import os
import re
import threading
from time import perf_counter, sleep
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pandas as pd


NORM_FIELDS = (
    "address", "predir_abbrev", "street_name", "street_type_abbrev",
    "postdir_abbrev", "internal", "location", "state_abbrev", "zip",
    "parsed", "zip4", "address_alphanumeric",
)
SMARTY_COLUMNS = (
    "delivery_line_1", "delivery_line_2", "city_name", "state_abbreviation",
    "zipcode", "plus4_code", "full_zipcode", "latitude", "longitude",
)

# Autocomplete accepts one address string. A single text[] parameter batches
# many calls with LATERAL. Accept its first-ranked result, following
# backend result order. The current function orders lexically by completion_key
# and source origin, not by a match-quality score. Signature is confirmed:
# addresses.autocomplete(text, integer DEFAULT 10, text DEFAULT NULL).
# The normalized_address returned by autocomplete is authoritative;
# geocode only the unmatched inputs.
AUTOCOMPLETE_OUTPUT_COLUMNS = frozenset({
    "input_index", "match_status", "delivery_line_1", "city_name",
    "state_abbreviation", "zipcode", "plus4_code",
})

_NORM_PROJECTION = """
    (r.normalized_address).address AS norm_address,
    (r.normalized_address).predirAbbrev AS norm_predir_abbrev,
    (r.normalized_address).streetName AS norm_street_name,
    (r.normalized_address).streetTypeAbbrev AS norm_street_type_abbrev,
    (r.normalized_address).postdirAbbrev AS norm_postdir_abbrev,
    (r.normalized_address).internal AS norm_internal,
    (r.normalized_address).location AS norm_location,
    (r.normalized_address).stateAbbrev AS norm_state_abbrev,
    (r.normalized_address).zip AS norm_zip,
    (r.normalized_address).parsed AS norm_parsed,
    (r.normalized_address).zip4 AS norm_zip4,
    (r.normalized_address).address_alphanumeric AS norm_address_alphanumeric
"""

# Non-autocomplete callers also resolve a whole text[] batch through geocode.
# The result's `addy` is the corrected norm_addy; `input_addy`, `geomout`,
# and `rating` are not canonical address components.
GEOCODE_SQL = f"""
WITH source AS MATERIALIZED (
    SELECT %s::text[] AS texts
),
inputs AS MATERIALIZED (
    SELECT i.input_index::integer AS input_index, i.input_text
    FROM source AS s
    CROSS JOIN LATERAL unnest(s.texts)
        WITH ORDINALITY AS i(input_text, input_index)
),
geocoded AS MATERIALIZED (
    SELECT g.input_index::integer AS input_index,
           g.normalized_address AS normalized_address,
           public.ST_Y(g.geomout) AS latitude,
           public.ST_X(g.geomout) AS longitude
    FROM source AS s
    CROSS JOIN LATERAL addresses.geocode_address(s.texts) AS g
),
resolved AS MATERIALIZED (
    SELECT i.input_index, i.input_text,
           g.normalized_address,
           CASE WHEN NULLIF(BTRIM((g.normalized_address).streetName::text), '')
                     IS NOT NULL THEN 'normalized_only'::text
                ELSE 'unmatched'::text END AS match_status,
           0::integer AS candidate_count,
           NULL AS address_id,
           NULL::text AS display_address,
           NULL::text AS completion_zip4,
           NULL::integer AS unit_count,
           NULL::text AS source,
           g.latitude, g.longitude
    FROM inputs AS i
    LEFT JOIN geocoded AS g ON g.input_index = i.input_index
)
SELECT r.input_index, r.input_text, r.match_status, r.candidate_count,
       r.address_id, r.display_address, r.completion_zip4,
       r.unit_count, r.source, r.latitude, r.longitude,
       {_NORM_PROJECTION}
FROM resolved AS r
ORDER BY r.input_index
"""

# Preserve the old imported name for callers that referenced the SQL constant.
STANDARDIZE_SQL = GEOCODE_SQL

# The SQL receives one text[] parameter per batch. Autocomplete remains
# scalar; only misses are collected into a second text[] and passed to
# addresses.geocode_address once. Join geocode_address's 1-based input_index back to original
# input positions; never geocode autocomplete hits.
AUTOCOMPLETE_SQL = f"""
WITH inputs AS MATERIALIZED (
    SELECT i.input_text, i.input_index::integer AS input_index
    FROM unnest(%s::text[]) WITH ORDINALITY AS i(input_text, input_index)
),
completions AS MATERIALIZED (
    SELECT i.input_index, i.input_text,
           a.address_id, a.display_address, a.normalized_address,
           a.zip4 AS completion_zip4, a.unit_count, a.source,
           CASE WHEN a.source IS NULL THEN 0 ELSE 1 END AS candidate_count
    FROM inputs AS i
    LEFT JOIN LATERAL (
        SELECT c.*
        FROM addresses.autocomplete(i.input_text, 1) AS c
        LIMIT 1
    ) AS a ON TRUE
),
classified AS MATERIALIZED (
    SELECT c.*, (c.source IS NOT NULL) AS has_completion
    FROM completions AS c
),
fallback_requests AS MATERIALIZED (
    SELECT c.input_index, c.input_text,
           row_number() OVER (ORDER BY c.input_index)::integer AS fallback_index
    FROM classified AS c
    WHERE c.has_completion IS NOT TRUE
),
fallback_texts AS MATERIALIZED (
    SELECT COALESCE(
        array_agg(f.input_text ORDER BY f.fallback_index), ARRAY[]::text[]
    ) AS texts
    FROM fallback_requests AS f
),
fallback_geocoded AS MATERIALIZED (
    SELECT g.input_index::integer AS fallback_index,
           g.normalized_address AS normalized_address,
           public.ST_Y(g.geomout) AS latitude,
           public.ST_X(g.geomout) AS longitude
    FROM (
        SELECT texts FROM fallback_texts WHERE cardinality(texts) > 0
    ) AS f
    CROSS JOIN LATERAL addresses.geocode_address(f.texts) AS g
),
resolved AS MATERIALIZED (
    SELECT c.input_index, c.input_text,
           CASE WHEN c.has_completion THEN c.address_id END AS address_id,
           CASE WHEN c.has_completion THEN c.display_address END AS display_address,
           CASE WHEN c.has_completion THEN c.completion_zip4 END AS completion_zip4,
           CASE WHEN c.has_completion THEN c.unit_count END AS unit_count,
           CASE WHEN c.has_completion THEN c.source END AS source,
           CASE WHEN c.has_completion THEN c.normalized_address
                ELSE g.normalized_address
           END AS normalized_address,
           CASE WHEN c.has_completion THEN 'completed'::text
                WHEN NULLIF(BTRIM((g.normalized_address).streetName::text), '')
                     IS NOT NULL THEN 'normalized_only'::text
                ELSE 'unmatched'::text
           END AS match_status,
           c.candidate_count, c.has_completion,
           g.latitude, g.longitude
    FROM classified AS c
    LEFT JOIN fallback_requests AS f ON f.input_index = c.input_index
    LEFT JOIN fallback_geocoded AS g ON g.fallback_index = f.fallback_index
),
formatted AS MATERIALIZED (
    SELECT r.*,
           NULLIF(CONCAT_WS(' ',
               COALESCE(
                   NULLIF(BTRIM((r.normalized_address).address_alphanumeric::text), ''),
                   NULLIF(BTRIM((r.normalized_address).address::text), '')
               ),
               NULLIF(BTRIM((r.normalized_address).predirAbbrev::text), ''),
               NULLIF(BTRIM((r.normalized_address).streetName::text), ''),
               NULLIF(BTRIM((r.normalized_address).streetTypeAbbrev::text), ''),
               NULLIF(BTRIM((r.normalized_address).postdirAbbrev::text), ''),
               NULLIF(BTRIM((r.normalized_address).internal::text), '')
           ), '') AS canonical_street
    FROM resolved AS r
)
SELECT r.input_index, r.input_text, r.match_status, r.candidate_count,
       r.address_id, r.display_address, r.unit_count, r.source,
       CASE WHEN r.has_completion THEN r.canonical_street END AS delivery_line_1,
       NULL::text AS delivery_line_2,
       CASE WHEN r.has_completion THEN
           NULLIF(BTRIM((r.normalized_address).location::text), '') END AS city_name,
       CASE WHEN r.has_completion THEN
           NULLIF(BTRIM((r.normalized_address).stateAbbrev::text), '') END AS state_abbreviation,
       CASE WHEN r.has_completion THEN
           NULLIF(BTRIM((r.normalized_address).zip::text), '') END AS zipcode,
       CASE WHEN r.has_completion THEN
           COALESCE(NULLIF(BTRIM(r.completion_zip4::text), ''),
                    NULLIF(BTRIM((r.normalized_address).zip4::text), ''))
       END AS plus4_code,
       r.latitude, r.longitude,
       {_NORM_PROJECTION}
FROM formatted AS r
ORDER BY r.input_index
"""

_POOL_LOCK = threading.Lock()
_POOLS: dict[str, tuple[Any, threading.BoundedSemaphore]] = {}


class AddrAllConfigurationError(RuntimeError):
    """Bad Keystone database bind, schema contract, or unavailable connection."""


def _postgres_dsn(bind: str) -> str:
    bind = bind.strip()
    if "://" not in bind:
        if "=" in bind:
            return bind
        raise AddrAllConfigurationError("Invalid DATABIND_SQL_KEYSTONE")
    parsed = urlsplit(bind)
    if parsed.scheme.lower() not in {"postgresql", "postgres", "postgresql+psycopg2", "postgresql+psycopg"}:
        raise AddrAllConfigurationError("DATABIND_SQL_KEYSTONE must use PostgreSQL")
    return urlunsplit(("postgresql", parsed.netloc, parsed.path, parsed.query, parsed.fragment))


def _connect_with_retries(connect: Any, *, operation: str) -> Any:
    """Retry connection failures only, never SQL statements."""
    operational_errors: list[type[Exception]] = []
    try:
        from psycopg2 import OperationalError as Psycopg2OperationalError
        operational_errors.append(Psycopg2OperationalError)
    except ImportError:
        pass
    try:
        from psycopg import OperationalError as PsycopgOperationalError
        operational_errors.append(PsycopgOperationalError)
    except ImportError:
        pass
    try:
        from sqlalchemy.exc import OperationalError as SQLAlchemyOperationalError
        operational_errors.append(SQLAlchemyOperationalError)
    except ImportError:
        pass
    if not operational_errors:
        raise AddrAllConfigurationError("Install psycopg2 or psycopg (with SQLAlchemy)")

    retries = max(0, int(os.getenv("ADDRALL_CONNECT_RETRIES", "2")))
    for attempt in range(retries + 1):
        try:
            return connect()
        except tuple(operational_errors) as exc:
            if attempt == retries:
                raise
            delay = min(8, 2 ** attempt)
            print(
                f"\t\t\t\tAddrAll {operation} failed ({type(exc).__name__}); "
                f"retry {attempt + 1}/{retries} after {delay}s", flush=True,
            )
            sleep(delay)
    raise AssertionError("Unreachable connection retry state")


@contextmanager
def keystone_connection(database_url: str | None = None):
    """Borrow a pooled raw DBAPI connection (psycopg2 or Azure psycopg3).

    Autocomplete/geocode SQL uses the caller connection. Azure primarily uses
    psycopg3; do not require an extra psycopg2 install for callers that do not
    supply their own DBAPI connection.
    """
    bind = database_url or os.getenv("DATABIND_SQL_KEYSTONE")
    if not bind:
        raise AddrAllConfigurationError("Set DATABIND_SQL_KEYSTONE")
    dsn = _postgres_dsn(bind)
    with _POOL_LOCK:
        if dsn not in _POOLS:
            size = int(os.getenv("ADDRALL_POOL_SIZE", "6"))
            timeout = int(os.getenv("ADDRALL_CONNECT_TIMEOUT", "15"))
            if size < 1 or timeout < 1:
                raise AddrAllConfigurationError("ADDRALL_POOL_SIZE and ADDRALL_CONNECT_TIMEOUT must be positive")
            try:
                from psycopg2.pool import ThreadedConnectionPool
            except ImportError:
                # Azure Functions already uses SQLAlchemy+psycopg3. Its
                # QueuePool supplies the same bounded concurrency semantics.
                from sqlalchemy import create_engine
                connect_args = {"connect_timeout": timeout,
                                "application_name": "addrall_autocomplete"}
                if "://" in dsn:
                    pool = create_engine(
                        dsn.replace("postgresql://", "postgresql+psycopg://", 1),
                        pool_size=size, max_overflow=0, pool_timeout=timeout,
                        pool_pre_ping=True, connect_args=connect_args,
                    )
                else:
                    pool = create_engine(
                        "postgresql+psycopg://", pool_size=size, max_overflow=0,
                        pool_timeout=timeout, pool_pre_ping=True,
                        connect_args={**connect_args, "conninfo": dsn},
                    )
            else:
                pool = _connect_with_retries(
                    lambda: ThreadedConnectionPool(
                        1, size, dsn=dsn, connect_timeout=timeout,
                        application_name="matchback_addrall",
                    ),
                    operation="pool initialization",
                )
            _POOLS[dsn] = (pool, threading.BoundedSemaphore(size))
        pool, limiter = _POOLS[dsn]

    with limiter:
        connection = None
        try:
            if hasattr(pool, "raw_connection"):
                connection = _connect_with_retries(
                    pool.raw_connection, operation="connection checkout")
            else:
                connection = _connect_with_retries(
                    pool.getconn, operation="connection checkout")
            connection.autocommit = True
            yield connection
        finally:
            if connection is not None:
                if hasattr(pool, "raw_connection"):
                    connection.close()  # returns to SQLAlchemy QueuePool
                else:
                    pool.putconn(connection, close=bool(connection.closed))


def close_connection_pools() -> None:
    """Call only when no queries are running."""
    with _POOL_LOCK:
        for pool, _ in _POOLS.values():
            if hasattr(pool, "dispose"):
                pool.dispose()
            else:
                pool.closeall()
        _POOLS.clear()


atexit.register(close_connection_pools)


def as_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        pass
    value = str(value).strip()
    return "" if value.lower() in {"nan", "none", "null", "na", "n/a"} else value


_STREET_APOSTROPHE_VARIANTS = str.maketrans({
    "\u2018": "'", "\u2019": "'", "\u02bc": "'",
    "\u201b": "'", "\uff07": "'",
})
_SPACED_DANDREA = re.compile(r"(?<![A-Z0-9])D\s+ANDREA(?![A-Z0-9])", re.I)


def prepare_street_apostrophes(value: Any) -> str:
    street = as_text(value).upper().translate(_STREET_APOSTROPHE_VARIANTS)
    street = re.sub(r"(?<=[A-Z0-9])\s*'+\s*(?=[A-Z0-9])", "'", street)
    street = _SPACED_DANDREA.sub("D'ANDREA", street)
    return " ".join(street.split())


def canonical_street_key(value: Any) -> str:
    """Keep the same apostrophe-insensitive join key on sales and targets."""
    return prepare_street_apostrophes(value).replace("'", "")


def normalize_zipcode(value: Any) -> str | None:
    value = as_text(value).upper().replace("'", "").replace("O", "0").replace("!", "0")
    if re.fullmatch(r"\d{1,9}\.0+", value):
        value = value.split(".")[0]
    if re.fullmatch(r"\d{9}", value):
        return value[:5]
    match = re.fullmatch(r"(\d{1,5})(?:[- ]\d{4})?", value)
    return match.group(1).zfill(5) if match else None


def normalize_zip4(value: Any) -> str | None:
    value = as_text(value).replace("'", "")
    if re.fullmatch(r"\d{1,4}\.0+", value):
        value = value.split(".")[0]
    return value.zfill(4) if re.fullmatch(r"\d{1,4}", value) else None


def zip4_from_full_zip(value: Any) -> str | None:
    text = as_text(value).replace("'", "")
    if re.fullmatch(r"\d{9}\.0+", text):
        text = text.split(".")[0]
    match = re.fullmatch(r"\d{5}[- ]?(\d{4})", text)
    return match.group(1) if match else None


def full_zipcode(zipcode: Any, zip4: Any) -> str | None:
    five, four = normalize_zipcode(zipcode), normalize_zip4(zip4)
    return f"{five}-{four}" if five and four else None


def split_zip_parts(zipcode: Any, zip4: Any = None) -> tuple[str | None, str | None]:
    five = normalize_zipcode(zipcode) or normalize_zipcode(zip4)
    four = normalize_zip4(zip4) or zip4_from_full_zip(zipcode) or zip4_from_full_zip(zip4)
    return five, four


def coerce_match_key_types(
    df: pd.DataFrame, *, zipcode: str, zip4: str | None = None,
    full_zipcode_col: str = "full_zipcode", inplace: bool = False,
) -> pd.DataFrame:
    """Preserve row order and input ZIP+4 when rebuilding postal join keys."""
    if zipcode not in df.columns:
        raise KeyError(f"Missing ZIP column: {zipcode!r}")
    out = df if inplace else df.copy()
    raw_zip5 = out[zipcode].tolist()
    raw_zip4 = out[zip4].tolist() if zip4 and zip4 in out else [None] * len(out)
    raw_full = out[full_zipcode_col].tolist() if full_zipcode_col in out else [None] * len(out)
    zip5_values, zip4_values, full_values = [], [], []
    for five_raw, four_raw, full_raw in zip(raw_zip5, raw_zip4, raw_full):
        five = normalize_zipcode(five_raw) or normalize_zipcode(full_raw)
        four = normalize_zip4(four_raw) or zip4_from_full_zip(five_raw)
        if not four and five and normalize_zipcode(full_raw) == five:
            four = zip4_from_full_zip(full_raw)
        zip5_values.append(five)
        zip4_values.append(four)
        full_values.append(full_zipcode(five, four))
    out[zipcode] = pd.array(zip5_values, dtype="string")
    if zip4 and zip4 in out:
        out[zip4] = pd.array(zip4_values, dtype="string")
    out[full_zipcode_col] = pd.array(full_values, dtype="string")
    return out


_UNIT_END = re.compile(
    r"(?<!\w)(?:APARTMENT|APT|SUITE|STE|UNIT|ROOM|RM|LOT|#)"
    r"\s*#?\s*-?[A-Z0-9]+(?:-[A-Z0-9]+)*\s*$", re.I,
)


def split_secondary(street: Any) -> tuple[str, str]:
    """Separate raw trailing unit on the fallback path."""
    raw = as_text(street).strip()
    match = _UNIT_END.search(raw)
    if not match:
        return raw, ""
    return raw[:match.start()].strip(), raw[match.start():].strip()


_STREET_ALIASES = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW",
    "STREET": "ST", "ROAD": "RD", "AVENUE": "AVE", "BOULEVARD": "BLVD",
    "DRIVE": "DR", "COURT": "CT", "CIRCLE": "CIR", "LANE": "LN",
    "PLACE": "PL", "PARKWAY": "PKWY", "TERRACE": "TER", "TRAIL": "TRL",
    "HIGHWAY": "HWY", "COVE": "CV", "VALLEY": "VLY", "HOLLOW": "HOLW",
}
_STREET_ALIAS_PATTERN = re.compile(
    r"\b(" + "|".join(sorted(_STREET_ALIASES, key=len, reverse=True)) + r")\b", re.I,
)


def standardize_street_text(value: Any) -> str:
    """Compatibility helper; backend standardize is authoritative for keys."""
    street = re.sub(r"\s+", " ", canonical_street_key(value).replace(".", " ")).strip()
    street = re.sub(r"^P\s+O\s+BOX\b", "PO BOX", street)
    return re.sub(
        r"\s+", " ", _STREET_ALIAS_PATTERN.sub(
            lambda m: _STREET_ALIASES[m.group(1).upper()], street,
        ),
    ).strip()


_TRAILING_US_COUNTRY = re.compile(
    r"(?<![A-Z0-9])(?:UNITED\s+STATES(?:\s+OF\s+AMERICA)?"
    r"|U\.?\s*S\.?\s*A\.?|U\.?\s*S\.?)\s*[,;\.]*$", re.I,
)


def _sql_address_text(value: Any) -> str:
    text = as_text(value)
    match = _TRAILING_US_COUNTRY.search(text)
    return text[:match.start()].rstrip(" \t\r\n,;") if match else text


def _join_address_fields(*parts: Any) -> str:
    return ", ".join(text for value in parts if (text := _sql_address_text(value)))


def build_address_text(
    street: Any, city: Any = None, state: Any = None, zipcode: Any = None,
) -> str:
    """Compatibility builder for structured source values."""
    building, _ = split_secondary(street)
    return _join_address_fields(building, city, state, normalize_zipcode(zipcode))


def _select_rows(conn: Any, sql: str, params: tuple) -> list[dict[str, Any]]:
    """Read the selected address results without changing session settings."""
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        names = [
            getattr(d, "name", d[0] if isinstance(d, tuple) else None)
            for d in cursor.description
        ]
        return [dict(zip(names, values)) for values in cursor.fetchall()]


def _record_to_address(record: dict[str, Any]) -> dict[str, Any]:
    """Convert a fallback norm_addy to matchback address fields."""
    out = dict(record)
    for field in NORM_FIELDS:
        if field not in {"parsed", "address"}:
            name = f"norm_{field}"
            out[name] = as_text(record.get(name)).upper() or None
    number = (
        as_text(out.get("norm_address_alphanumeric"))
        or as_text(out.get("norm_address"))
    ).upper()
    parts = (
        number, out.get("norm_predir_abbrev"), out.get("norm_street_name"),
        out.get("norm_street_type_abbrev"), out.get("norm_postdir_abbrev"),
    )
    out["standardized_street"] = (
        " ".join(part for part in parts if part)
        if number and out.get("norm_street_name") else None
    )
    out["standardized_city"] = out.get("norm_location")
    out["standardized_state"] = out.get("norm_state_abbrev")
    out["standardized_zipcode"] = normalize_zipcode(out.get("norm_zip"))
    # Do not treat a geocoded composite as a verified ZIP+4 completion.
    # _delivery_parts separately handles supplied ZIP+4.
    out["zip4"] = None
    return out


def _completion_to_address(record: dict[str, Any]) -> dict[str, Any]:
    """Project autocomplete's FINAL result, without any further parsing."""
    missing = AUTOCOMPLETE_OUTPUT_COLUMNS.difference(record)
    if missing:
        raise AddrAllConfigurationError(
            f"Autocomplete SQL adapter missing output columns: {sorted(missing)}"
        )
    status = as_text(record.get("match_status")).lower()
    if status not in {"completed", "normalized_only", "unmatched"}:
        raise AddrAllConfigurationError(
            f"Unsupported autocomplete match_status: {status!r}"
        )
    out = dict(record)
    out["match_status"] = status
    if status == "completed":
        out["standardized_street"] = as_text(record.get("delivery_line_1")) or None
        out["standardized_city"] = as_text(record.get("city_name")) or None
        out["standardized_state"] = as_text(record.get("state_abbreviation")) or None
        out["standardized_zipcode"] = normalize_zipcode(record.get("zipcode"))
        out["zip4"] = normalize_zip4(record.get("plus4_code"))
    else:
        # The SQL returns geocoded norm_addy fields for autocomplete misses.
        # Use the same converter as the explicit geocode-only path.
        out = _record_to_address(out)
        out["match_status"] = status
    return out


def _query_text(
    request: tuple[str, str, str, str | None],
    address2: str = "",
) -> str:
    """One complete address query, including the unit, per distinct input."""
    street, city, state, zip5 = request
    line = " ".join(
        piece for piece in (_sql_address_text(street), _sql_address_text(address2))
        if piece
    )
    return _join_address_fields(line, city, state, zip5)


def _lookup_batch(
    batch: list[tuple[str, str, str, str | None, str]], *,
    conn: Any, use_autocomplete: bool, candidate_limit: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Batched autocomplete, with one geocode_address(text[]) call for misses."""
    queries = [_query_text(item[:4], item[4]) for item in batch]
    if not queries:
        return [], {"sql_seconds": 0.0, "completed": 0, "fallback": 0, "unmatched": 0, "ambiguous": 0}
    started = perf_counter()
    if use_autocomplete:
        rows = _select_rows(conn, AUTOCOMPLETE_SQL, (queries,))
    else:
        rows = _select_rows(conn, GEOCODE_SQL, (queries,))
    received = [int(row["input_index"]) for row in rows]
    if received != list(range(1, len(queries) + 1)):
        raise AddrAllConfigurationError(
            f"Address SQL returned wrong input indices: expected {len(queries)}, "
            f"received {received[:10]}"
        )
    formatted = [
        _completion_to_address(row) if use_autocomplete else _record_to_address(row)
        for row in rows
    ]
    return formatted, {
        "sql_seconds": perf_counter() - started,
        "completed": sum(row.get("match_status") == "completed" for row in formatted),
        "fallback": sum(row.get("match_status") == "normalized_only" for row in formatted),
        "unmatched": sum(row.get("match_status") == "unmatched" for row in formatted),
        "ambiguous": 0,  # Autocomplete returns the preferred candidate; no ambiguity veto.
    }


def _resolve_addresses(
    requests: list[tuple[str, str, str, str | None, str]], *,
    connection: Any, database_url: str | None, batch_size: int,
    workers: int, use_autocomplete: bool, candidate_limit: int,
) -> list[dict[str, Any]]:
    if not requests:
        return []
    batches = [requests[i:i + batch_size] for i in range(0, len(requests), batch_size)]
    profile = os.getenv("ADDRALL_PROFILE", "").strip().lower() in {"1", "true", "yes", "on"}

    def run_batch(job: tuple[int, list[tuple]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        n, batch = job
        started = perf_counter()
        if connection is not None:
            rows, metrics = _lookup_batch(
                batch, conn=connection, use_autocomplete=use_autocomplete,
                candidate_limit=candidate_limit,
            )
        else:
            with keystone_connection(database_url) as conn:
                rows, metrics = _lookup_batch(
                    batch, conn=conn, use_autocomplete=use_autocomplete,
                    candidate_limit=candidate_limit,
                )
        if profile:
            print(
                f"\t\t\t\tAddrAll worker {threading.current_thread().name} "
                f"batch {n}/{len(batches)}: {len(batch):,} inputs, "
                f"completed={metrics['completed']:,}, "
                f"geocoded={metrics['fallback']:,}, "
                f"unmatched={metrics['unmatched']:,}, "
                f"SQL={metrics['sql_seconds']:.3f}s, "
                f"elapsed={perf_counter() - started:.3f}s", flush=True,
            )
        return rows, metrics

    started = perf_counter()
    if workers == 1:
        completed = map(run_batch, enumerate(batches, start=1))
        outputs = list(completed)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            outputs = list(executor.map(run_batch, enumerate(batches, start=1)))
    print(
        f"\t\t\t\tAddrAll SQL: {len(requests):,} distinct inputs, "
        f"{len(batches)} batches, {workers} workers, "
        f"mode={'autocomplete' if use_autocomplete else 'geocode'}, "
        f"completed={sum(x['completed'] for _, x in outputs):,}, "
        f"geocoded={sum(x['fallback'] for _, x in outputs):,}, "
        f"unmatched={sum(x['unmatched'] for _, x in outputs):,}, "
        f"SQL={sum(x['sql_seconds'] for _, x in outputs):.2f}s, "
        f"elapsed={perf_counter() - started:.2f}s"
    )
    return [row for rows, _ in outputs for row in rows]


def _delivery_parts(
    raw_street: str, raw_city: str, raw_state: str,
    raw_address2: str, zip_parts: tuple[str | None, str | None],
    match: dict[str, Any], *, preserve_supplied_zip4: bool,
    raw_latitude: Any = None, raw_longitude: Any = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Use completed fields directly; preserve independently supplied postal data."""
    completed = match.get("match_status") == "completed"
    building, unit = split_secondary(raw_street)
    if completed:
        # Autocomplete owns canonicalization, unit placement, and locality.
        # No Python spelling repairs or fallback geocode after it.
        line1 = as_text(match.get("delivery_line_1")) or raw_street
        line2 = as_text(match.get("delivery_line_2")) or raw_address2
        city = as_text(match.get("city_name")) or raw_city
        state = as_text(match.get("state_abbreviation")) or raw_state
    else:
        # Geocode-only or autocomplete miss: SQL returned the corrected
        # `addy` composite for this record.
        standardized = as_text(match.get("standardized_street")) or building
        line1 = " ".join(piece for piece in (standardized, unit) if piece).upper() or None
        line2 = raw_address2 or (match.get("norm_internal") if not unit else None)
        city = as_text(match.get("standardized_city")) or raw_city
        state = as_text(match.get("standardized_state")) or raw_state

    zip5_input, zip4_input = zip_parts
    zip5_db = normalize_zipcode(match.get("standardized_zipcode"))
    zip4_db = normalize_zip4(match.get("zip4")) if completed else None
    zip5 = zip5_db or zip5_input
    parsed_zip4 = normalize_zip4(match.get("norm_zip4"))
    if not zip5_input and not zip4_input and parsed_zip4:
        zip4_input = parsed_zip4
        zip5_input = zip5_db

    if preserve_supplied_zip4 and zip4_input:
        # Target ZIP+4 lists are authoritative; never graft a supplied
        # ZIP+4 onto a different completed ZIP5.
        if zip5_input:
            zip5 = zip5_input
        zip4, zip4_source = zip4_input, "input"
    elif zip4_db:
        zip4, zip4_source = zip4_db, "database"
    elif zip4_input and (not zip5_db or zip5_input == zip5_db):
        zip4, zip4_source = zip4_input, "input"
    else:
        zip4 = None
        zip4_source = "input_zip_mismatch" if zip4_input else "missing"

    # A geocode result supplies coordinates as a pair. Autocomplete and
    # missing geometries leave existing source coordinates untouched.
    has_geocoded_point = (
        match.get("latitude") is not None and match.get("longitude") is not None
    )
    latitude = (match["latitude"] if has_geocoded_point
                else raw_latitude if as_text(raw_latitude) else None)
    longitude = (match["longitude"] if has_geocoded_point
                 else raw_longitude if as_text(raw_longitude) else None)
    parts = {
        "delivery_line_1": line1,
        "delivery_line_2": line2 or None,
        "city_name": city or None,
        "state_abbreviation": state or None,
        "zipcode": zip5,
        "plus4_code": zip4,
        "full_zipcode": full_zipcode(zip5, zip4),
        "latitude": latitude,
        "longitude": longitude,
    }
    extra = {
        "addrall_building_street": line1 or None,
        "addrall_secondary_status": "unverified" if unit or raw_address2 else "absent",
        "addrall_zip4_source": zip4_source,
        "addrall_input_full_zipcode": full_zipcode(zip_parts[0], zip_parts[1]),
        "addrall_database_full_zipcode": full_zipcode(zip5_db, zip4_db),
    }
    return parts, extra


def clean_addresses(
    df: pd.DataFrame, ADDRESS: str, CITY: str | None = None,
    STATE: str | None = None, ZIPCODE: str | None = None, *,
    ZIP4: str | None = None, ADDRESS2: str | None = None,
    connection: Any = None, database_url: str | None = None,
    batch_size: int | None = None, candidate_limit: int = 50,
    keep_invalid: bool = True, use_geocode: bool = True,
    retry_missing_zip4: bool = False,
    skip_invalid_geocode_regex: bool = True,
    compact: bool = False,
    use_autocomplete: bool | None = None,
    preserve_supplied_zip4: bool = False,
) -> pd.DataFrame:
    """Return Smarty-compatible columns and optional full AddrAll debug columns.

    `use_geocode` remains the historical alias for `use_autocomplete`.
    Regardless of that setting, the non-autocomplete path now calls
    addresses.geocode_address(text[]). Retired flags still do not add SQL passes.
    """
    if ADDRESS not in df:
        raise KeyError(f"Missing address column {ADDRESS!r}")
    size = int(batch_size or os.getenv("ADDRALL_BATCH_SIZE", "1000"))
    if size < 1:
        raise ValueError("batch_size must be positive")
    autocomplete = use_geocode if use_autocomplete is None else use_autocomplete
    # Legacy ADDRALL_ZIP_LOCALITY remains recognized for configuration
    # visibility, but its extra SQL pass is retired: autocomplete supplies
    # city/state/ZIP; geocode now repairs fallback locality as well.
    if (os.getenv("ADDRALL_ZIP_LOCALITY", "0").strip().lower()
            in {"1", "true", "yes", "on"}
            and os.getenv("ADDRALL_PROFILE", "0").strip().lower()
            in {"1", "true", "yes", "on"}):
        print("\t\t\t\tAddrAll: ADDRALL_ZIP_LOCALITY ignored (no extra ZIP lookup)")
    started = perf_counter()

    def values(name: str | None) -> list[str]:
        return [as_text(x) for x in df[name].tolist()] if name and name in df else [""] * len(df)

    raw_street, raw_city, raw_state = values(ADDRESS), values(CITY), values(STATE)
    raw_zip, raw_zip4, raw_address2 = values(ZIPCODE), values(ZIP4), values(ADDRESS2)
    raw_latitude = df["latitude"].tolist() if "latitude" in df else [None] * len(df)
    raw_longitude = df["longitude"].tolist() if "longitude" in df else [None] * len(df)
    raw_full_zip = values("full_zipcode")
    original_streets = values("addrall_original_street") if "addrall_original_street" in df else raw_street
    postal = [
        split_zip_parts(five or full, four or full)
        for five, four, full in zip(raw_zip, raw_zip4, raw_full_zip)
    ]
    keys = [
        (street, city.upper(), state.upper(), parts[0], address2)
        for street, city, state, parts, address2 in zip(
            raw_street, raw_city, raw_state, postal, raw_address2
        )
    ]
    unique = list(dict.fromkeys(key for key in keys if key[0].strip()))
    workers = max(1, int(os.getenv("ADDRALL_WORKERS", "6")))
    if connection is not None:
        workers = 1
    else:
        workers = min(workers, max(1, int(os.getenv("ADDRALL_POOL_SIZE", "6"))))
    # Fail before touching the database when the new backend adapter is missing.
    if unique:
        found = _resolve_addresses(
            unique, connection=connection, database_url=database_url,
            batch_size=size, workers=workers, use_autocomplete=autocomplete,
            candidate_limit=candidate_limit,
        )
        by_key = dict(zip(unique, found))
    else:
        by_key = {}

    if compact:
        # Keep memory use low for large target lists; debug-enabled target
        # callers disable compact mode to retain full diagnostics.
        columns: dict[str, list[Any]] = {name: [] for name in SMARTY_COLUMNS}
        for key, street, city, state, address2, parts, lat, lon in zip(
            keys, raw_street, raw_city, raw_state, raw_address2, postal,
            raw_latitude, raw_longitude,
        ):
            delivery, _ = _delivery_parts(
                street, city, state, address2, parts,
                by_key.get(key) or {},
                preserve_supplied_zip4=preserve_supplied_zip4,
                raw_latitude=lat, raw_longitude=lon,
            )
            for name in SMARTY_COLUMNS:
                columns[name].append(delivery[name])
        output = df.copy(deep=False)
        for name, vals in columns.items():
            output[name] = pd.array(vals, dtype="string")
        if os.getenv("ADDRALL_PROFILE", "").lower() in {"1", "true", "yes", "on"}:
            print(f"\t\t\t\tAddrAll compact target: {len(keys):,} rows, {len(unique):,} distinct queries")
        return output

    records: list[dict[str, Any]] = []
    for i, key in enumerate(keys):
        match = by_key.get(key) or {}
        delivery, extra = _delivery_parts(
            raw_street[i], raw_city[i], raw_state[i], raw_address2[i],
            postal[i], match, preserve_supplied_zip4=preserve_supplied_zip4,
            raw_latitude=raw_latitude[i], raw_longitude=raw_longitude[i],
        )
        status = match.get("match_status") or "input_formatted"
        if status == "normalized_only" and not match.get("standardized_street"):
            status = "input_formatted"
        original = {
            "addrall_original_street": original_streets[i] or raw_street[i],
            "addrall_original_address2": raw_address2[i],
            "addrall_original_city": raw_city[i],
            "addrall_original_state": raw_state[i],
            "addrall_original_zipcode": raw_zip[i],
            "addrall_original_zip4": raw_zip4[i],
        }
        diagnostic = {
            "addrall_validated": None,
            "addrall_match_status": status,
            "addrall_legacy_key_compatible": status == "completed",
            "addrall_source": match.get("source"),
            "addrall_address_id": match.get("address_id"),
            "addrall_autocomplete_input_text": match.get("input_text") or _query_text(key[:4], key[4]),
            "addrall_autocomplete_candidates": match.get("candidate_count") if autocomplete else None,
            "addrall_autocomplete_display_address": match.get("display_address"),
            "addrall_autocomplete_source": match.get("source") if autocomplete else None,
            "addrall_normalize_status": "applied" if match.get("standardized_street") else "unstructured",
            "addrall_normalize_input_text": match.get("input_text") or _query_text(key[:4], key[4]),
            "addrall_normalize_attempted_texts": None,
            "addrall_normalize_raw_street": match.get("standardized_street"),
            "addrall_normalized_after_geocode": False,
            # Legacy debug fields remain available. Coordinates from the
            # geocoder are emitted as latitude/longitude above.
            "addrall_geocode_source": None,
            "addrall_geocode_match": None,
            "addrall_geocode_input_text": None,
            "addrall_geocode_attempted_texts": None,
            "addrall_geocode_success_input_text": None,
            "addrall_geocode_success_variant": None,
            "addrall_geocode_sources_tried": None,
            "addrall_geocode_street_pre_normalize": None,
            "addrall_zip4_retry_input_text": None,
            "addrall_zip4_retry_sources_tried": None,
            "addrall_zip4_retry_source": None,
            "addrall_zip4_retry_variant": None,
            "addrall_zip4_retry_attempted_texts": None,
            "addrall_unit_count": match.get("unit_count"),
        }
        for field in NORM_FIELDS:
            diagnostic[f"addrall_norm_{field}"] = match.get(f"norm_{field}")
        records.append({**diagnostic, **original, **extra, **delivery})

    output = df.copy()
    names = list(records[0]) if records else [
        "addrall_validated", "addrall_match_status",
        "addrall_legacy_key_compatible", "addrall_source", "addrall_address_id",
        "addrall_autocomplete_input_text", "addrall_autocomplete_candidates",
        "addrall_autocomplete_display_address", "addrall_autocomplete_source",
        "addrall_normalize_status", "addrall_normalize_input_text",
        "addrall_normalize_attempted_texts", "addrall_normalize_raw_street",
        "addrall_normalized_after_geocode", "addrall_geocode_source",
        "addrall_geocode_match", "addrall_geocode_input_text",
        "addrall_geocode_attempted_texts", "addrall_geocode_success_input_text",
        "addrall_geocode_success_variant", "addrall_geocode_sources_tried",
        "addrall_geocode_street_pre_normalize", "addrall_zip4_retry_input_text",
        "addrall_zip4_retry_sources_tried", "addrall_zip4_retry_source",
        "addrall_zip4_retry_variant", "addrall_zip4_retry_attempted_texts",
        "addrall_unit_count", "addrall_original_street",
        "addrall_original_address2", "addrall_original_city",
        "addrall_original_state", "addrall_original_zipcode",
        "addrall_original_zip4", "addrall_building_street",
        "addrall_secondary_status", "addrall_zip4_source",
        "addrall_input_full_zipcode", "addrall_database_full_zipcode",
        *SMARTY_COLUMNS, *(f"addrall_norm_{f}" for f in NORM_FIELDS),
    ]
    for field in names:
        # Caller may have captured raw values before their own preprocessing.
        if field.startswith("addrall_original_") and field in df:
            continue
        output[field] = [record.get(field) for record in records]

    client_columns = [col for col in df if not col.startswith("addrall_")]
    address_columns = [col for col in SMARTY_COLUMNS if col in output]
    other_columns = [col for col in output if col not in client_columns and col not in address_columns]
    output = output.loc[:, list(dict.fromkeys(client_columns + address_columns + other_columns))]
    if not keep_invalid:
        output = output.loc[
            output["addrall_match_status"].isin(["completed", "normalized_only"])
        ].copy()
    print(
        f"\t\t\t\tAddrAll DataFrame: {len(df):,} rows -> {len(output):,} rows, "
        f"{len(unique):,} unique queries, total {perf_counter() - started:.2f}s"
    )
    return output


def normalize_target_addresses(
    df: pd.DataFrame, *,
    STREET: str = "street", CITY: str = "city", STATE: str = "state",
    ZIP: str = "ZIP", ZIP4: str = "zip4", ADDRESS2: str | None = None,
    connection: Any = None, database_url: str | None = None,
    batch_size: int | None = None, geocode: bool = False,
    compact: bool = False, autocomplete: bool | None = None,
) -> pd.DataFrame:
    """Target join keys with either autocomplete or geocode-only mode.

    Provided target ZIP+4 always wins; autocomplete supplies completed ZIP+4
    when available. ZIP+4-only rows bypass this wrapper in import_targets.
    """
    if STREET not in df:
        raise KeyError(f"Missing street column {STREET!r}")
    if df.empty:
        return df.copy()
    use_autocomplete = geocode if autocomplete is None else autocomplete
    enriched = clean_addresses(
        df, STREET, CITY, STATE, ZIP, ZIP4=ZIP4 if ZIP4 in df else None,
        ADDRESS2=ADDRESS2, connection=connection, database_url=database_url,
        batch_size=batch_size, use_autocomplete=use_autocomplete,
        retry_missing_zip4=False, compact=compact, preserve_supplied_zip4=True,
    )
    out = enriched.copy(deep=not compact)
    out[STREET] = pd.array(enriched["delivery_line_1"].tolist(), dtype="string")
    out[CITY] = pd.array(enriched["city_name"].tolist(), dtype="string")
    out[STATE] = pd.array(enriched["state_abbreviation"].tolist(), dtype="string")
    out[ZIP] = pd.array(enriched["zipcode"].tolist(), dtype="string")
    out[ZIP4] = pd.array(enriched["plus4_code"].tolist(), dtype="string")
    out["full_zipcode"] = pd.array(enriched["full_zipcode"].tolist(), dtype="string")
    out["delivery_line_1"] = out[STREET].array
    out["city_name"] = out[CITY].array
    out["state_abbreviation"] = out[STATE].array
    out["zipcode"] = out[ZIP].array
    out["plus4_code"] = out[ZIP4].array
    return out


def strip_enrichment_columns(
    df: pd.DataFrame, *, preserve: Sequence[str] = (),
) -> pd.DataFrame:
    """Drop diagnostic/Smarty aliases without deleting full_zipcode."""
    keep = set(preserve) | {"full_zipcode", "latitude", "longitude"}
    temporary = set(SMARTY_COLUMNS) - keep
    return df.loc[:, [
        col for col in df.columns
        if col in keep or not (col.startswith("addrall_") or col in temporary)
    ]].copy()


def save_addrall_debug(
    df: pd.DataFrame, kind: str, *, label: str | None = None,
) -> None:
    """Unchanged ADDRALL_DEBUG_DIR CSV export for complete diagnostics."""
    directory_name = os.getenv("ADDRALL_DEBUG_DIR")
    if not directory_name:
        return
    if kind != 'sales':
        return

    from pathlib import Path
    from uuid import uuid4

    directory = Path(directory_name)
    directory.mkdir(parents=True, exist_ok=True)
    filename_label = re.sub(r"[^a-zA-Z0-9_.-]+", "_", label or "").strip("._")[:64]
    prefix = f"{kind}_{filename_label}" if filename_label else kind
    path = directory / f"{prefix}_{uuid4().hex}.csv"
    df.to_csv(path, index=False)
    print(f"AddrAll debug: {path}")


def smarty_streets_cleaning(
    df: pd.DataFrame, ADDRESS: str, CITY: str | None = None,
    STATE: str | None = None, ZIPCODE: str | None = None, **kwargs,
) -> pd.DataFrame:
    """Compatibility alias; no Smarty or geocoding service is called."""
    return clean_addresses(df, ADDRESS, CITY, STATE, ZIPCODE, **kwargs)
