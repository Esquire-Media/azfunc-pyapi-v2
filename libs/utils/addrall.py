"""SQL-backed address cleaning for rooftop polygons.

Complete with addresses.autocomplete(text, 1). For autocomplete misses,
run addresses.normalize(text[]) -> addresses.standardize(norm_addy[]) in the
same batched SQL query. No Smarty, geocoding, or debug artifacts.
"""

from __future__ import annotations

from contextlib import closing
import os
import re
from urllib.parse import urlsplit, urlunsplit

import pandas as pd


ADDRESS_COLUMNS = (
    "delivery_line_1", "delivery_line_2", "city_name", "state_abbreviation",
    "zipcode", "plus4_code", "full_zipcode",
)

# One SQL statement per batch. Autocomplete returns at most one result per
# address; only misses are passed to the batch normalize/standardize functions.
ADDRESS_SQL = """
WITH inputs AS MATERIALIZED (
    SELECT i.input_index::integer, i.input_text
    FROM unnest(%s::text[]) WITH ORDINALITY AS i(input_text, input_index)
),
completions AS MATERIALIZED (
    SELECT i.input_index, a.normalized_address,
           a.zip4 AS completion_zip4, (a.source IS NOT NULL) AS completed
    FROM inputs AS i
    LEFT JOIN LATERAL (
        SELECT c.*
        FROM addresses.autocomplete(i.input_text, 1) AS c
        LIMIT 1
    ) AS a ON TRUE
),
fallback_requests AS MATERIALIZED (
    SELECT i.input_index, i.input_text,
           row_number() OVER (ORDER BY i.input_index)::integer AS fallback_index
    FROM inputs AS i
    JOIN completions AS c USING (input_index)
    WHERE c.completed IS NOT TRUE
),
fallback_texts AS MATERIALIZED (
    SELECT COALESCE(
        array_agg(f.input_text ORDER BY f.fallback_index), ARRAY[]::text[]
    ) AS texts
    FROM fallback_requests AS f
),
fallback_parsed AS MATERIALIZED (
    SELECT CASE WHEN cardinality(f.texts) = 0
                THEN ARRAY[]::tiger.norm_addy[]
                ELSE addresses.normalize(f.texts)
           END AS addys
    FROM fallback_texts AS f
),
fallback_standardized AS MATERIALIZED (
    SELECT CASE WHEN cardinality(p.addys) = 0
                THEN ARRAY[]::tiger.norm_addy[]
                ELSE addresses.standardize(p.addys)
           END AS addys
    FROM fallback_parsed AS p
),
resolved AS MATERIALIZED (
    SELECT c.input_index, c.completed, c.completion_zip4,
           CASE WHEN c.completed THEN c.normalized_address
                ELSE s.addys[f.fallback_index]
           END AS normalized_address
    FROM completions AS c
    CROSS JOIN fallback_standardized AS s
    LEFT JOIN fallback_requests AS f USING (input_index)
),
formatted AS MATERIALIZED (
    SELECT r.*,
           COALESCE(
               NULLIF(BTRIM((r.normalized_address).address_alphanumeric::text), ''),
               NULLIF(BTRIM((r.normalized_address).address::text), '')
           ) AS number,
           NULLIF(BTRIM((r.normalized_address).streetName::text), '') AS street_name
    FROM resolved AS r
),
streets AS MATERIALIZED (
    SELECT r.*,
           NULLIF(CONCAT_WS(' ', r.number,
               NULLIF(BTRIM((r.normalized_address).predirAbbrev::text), ''),
               r.street_name,
               NULLIF(BTRIM((r.normalized_address).streetTypeAbbrev::text), ''),
               NULLIF(BTRIM((r.normalized_address).postdirAbbrev::text), '')
           ), '') AS building_street
    FROM formatted AS r
)
SELECT r.input_index,
       CASE WHEN r.completed THEN 'completed'
            WHEN r.street_name IS NOT NULL THEN 'normalized_only'
            ELSE 'unmatched' END AS match_status,
       CASE WHEN r.completed THEN
           NULLIF(CONCAT_WS(' ', r.building_street,
               NULLIF(BTRIM((r.normalized_address).internal::text), '')
           ), '')
            WHEN r.number IS NOT NULL AND r.street_name IS NOT NULL
                THEN r.building_street
       END AS delivery_line_1,
       NULLIF(BTRIM((r.normalized_address).internal::text), '') AS norm_internal,
       NULLIF(BTRIM((r.normalized_address).location::text), '') AS city_name,
       NULLIF(BTRIM((r.normalized_address).stateAbbrev::text), '') AS state_abbreviation,
       NULLIF(BTRIM((r.normalized_address).zip::text), '') AS zipcode,
       NULLIF(BTRIM((r.normalized_address).zip4::text), '') AS norm_zip4,
       CASE WHEN r.completed THEN COALESCE(
           NULLIF(BTRIM(r.completion_zip4::text), ''),
           NULLIF(BTRIM((r.normalized_address).zip4::text), '')
       ) END AS plus4_code
FROM streets AS r
ORDER BY r.input_index
"""

_UNIT_END = re.compile(
    r"(?<!\w)(?:APARTMENT|APT|SUITE|STE|UNIT|ROOM|RM|LOT|#)"
    r"\s*#?\s*-?[A-Z0-9]+(?:-[A-Z0-9]+)*\s*$", re.I,
)
_TRAILING_US = re.compile(
    r"(?<![A-Z0-9])(?:UNITED\s+STATES(?:\s+OF\s+AMERICA)?"
    r"|U\.?\s*S\.?\s*A\.?|U\.?\s*S\.?)\s*[,;\.]*$", re.I,
)


def _text(value: object) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (TypeError, ValueError):
        pass
    value = str(value).strip()
    return "" if value.lower() in {"nan", "none", "null", "na", "n/a"} else value


def _zip_parts(value: object) -> tuple[str | None, str | None]:
    value = _text(value).upper().replace("'", "").replace("O", "0").replace("!", "0")
    if re.fullmatch(r"\d{1,9}\.0+", value):
        value = value.split(".")[0]
    if match := re.fullmatch(r"(\d{1,5})(?:[- ](\d{4}))?", value):
        return match[1].zfill(5), match[2]
    if re.fullmatch(r"\d{9}", value):
        return value[:5], value[5:]
    return None, None


def _zip4(value: object) -> str | None:
    value = _text(value).replace("'", "")
    if re.fullmatch(r"\d{1,4}\.0+", value):
        value = value.split(".")[0]
    return value.zfill(4) if re.fullmatch(r"\d{1,4}", value) else None


def _query_text(street: str, address2: str, city: str, state: str, zip5: str | None) -> str:
    parts = []
    for component in (" ".join(p for p in (street, address2) if p), city, state, zip5):
        component = component or ""
        country = _TRAILING_US.search(component)
        if country:
            component = component[:country.start()].rstrip(" \t\r\n,;")
        if component:
            parts.append(component)
    return ", ".join(parts)


def _postgres_dsn() -> str:
    bind = os.environ["DATABIND_SQL_KEYSTONE"].strip()
    if "://" not in bind:
        if "=" in bind:
            return bind
        raise ValueError("DATABIND_SQL_KEYSTONE must be a PostgreSQL DSN")
    parsed = urlsplit(bind)
    if parsed.scheme.lower() not in {"postgresql", "postgres", "postgresql+psycopg2"}:
        raise ValueError("DATABIND_SQL_KEYSTONE must use PostgreSQL")
    return urlunsplit(("postgresql", parsed.netloc, parsed.path, parsed.query, parsed.fragment))


def _read_addresses(conn, queries: list[str]) -> list[dict]:
    with conn.cursor() as cursor:
        cursor.execute(ADDRESS_SQL, (queries,))
        columns = [col.name if hasattr(col, "name") else col[0] for col in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    indices = [int(row["input_index"]) for row in rows]
    if indices != list(range(1, len(queries) + 1)):
        raise ValueError(f"Address SQL returned incorrect indices: {indices[:10]}")
    return rows


def _format_result(street: str, address2: str, city: str, state: str,
                   zip_input: tuple[str | None, str | None], result: dict) -> dict:
    completed = result["match_status"] == "completed"
    unit_match = _UNIT_END.search(street)
    building = street[:unit_match.start()].strip() if unit_match else street
    unit = unit_match.group().strip() if unit_match else ""

    if completed:
        line1 = _text(result["delivery_line_1"]) or street
        line2 = address2
    else:
        line1 = " ".join(p for p in (_text(result["delivery_line_1"]) or building, unit) if p).upper()
        line2 = address2 or (_text(result["norm_internal"]) if not unit else "")

    raw_zip5, raw_zip4 = zip_input
    db_zip5, _ = _zip_parts(result["zipcode"])
    if not raw_zip5 and not raw_zip4 and (parsed_zip4 := _zip4(result.get("norm_zip4"))):
        raw_zip5, raw_zip4 = db_zip5, parsed_zip4
    zip5 = db_zip5 or raw_zip5
    db_zip4 = _zip4(result["plus4_code"]) if completed else None
    zip4 = db_zip4 or (raw_zip4 if raw_zip4 and (not db_zip5 or db_zip5 == raw_zip5) else None)

    return {
        "delivery_line_1": line1 or None,
        "delivery_line_2": line2 or None,
        "city_name": _text(result["city_name"]) or city or None,
        "state_abbreviation": _text(result["state_abbreviation"]) or state or None,
        "zipcode": zip5,
        "plus4_code": zip4,
        "full_zipcode": f"{zip5}-{zip4}" if zip5 and zip4 else None,
    }


def clean_addresses(
    df: pd.DataFrame,
    ADDRESS: str,
    CITY: str | None = None,
    STATE: str | None = None,
    ZIPCODE: str | None = None,
    *,
    ADDRESS2: str | None = None,
    batch_size: int = 1000,
) -> pd.DataFrame:
    """Clean addresses with autocomplete and standardization fallback.

    Preserve original columns and order; append cleaned address fields.
    Discard inputs that neither autocomplete nor standardize can resolve.
    """
    if ADDRESS not in df.columns:
        raise KeyError(f"Missing address column: {ADDRESS}")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    for name in (CITY, STATE, ZIPCODE, ADDRESS2):
        if name is not None and name not in df.columns:
            raise KeyError(f"Missing address column: {name}")
    if df.empty:
        return df.assign(**{name: pd.array([], dtype="string") for name in ADDRESS_COLUMNS})

    def column(name: str | None) -> list[str]:
        return [_text(value) for value in df[name].tolist()] if name else [""] * len(df)

    streets, cities, states = column(ADDRESS), column(CITY), column(STATE)
    addresses2, zip_inputs = column(ADDRESS2), [_zip_parts(value) for value in column(ZIPCODE)]
    keys = [
        (street, city.upper(), state.upper(), postal[0], address2)
        for street, city, state, postal, address2 in zip(streets, cities, states, zip_inputs, addresses2)
    ]
    unique = list(dict.fromkeys(key for key in keys if key[0]))
    matches = {}
    if unique:
        import psycopg2

        with closing(psycopg2.connect(_postgres_dsn(), connect_timeout=15)) as conn:
            conn.autocommit = True
            for start in range(0, len(unique), batch_size):
                batch = unique[start:start + batch_size]
                queries = [_query_text(street, address2, city, state, zip5)
                           for street, city, state, zip5, address2 in batch]
                matches.update(zip(batch, _read_addresses(conn, queries)))

    kept = []
    cleaned = []
    for i, key in enumerate(keys):
        result = matches.get(key)
        if result is None or result["match_status"] not in {"completed", "normalized_only"}:
            continue
        kept.append(i)
        cleaned.append(_format_result(streets[i], addresses2[i], cities[i], states[i], zip_inputs[i], result))

    output = df.iloc[kept].copy()
    for name in ADDRESS_COLUMNS:
        output[name] = pd.array([item[name] for item in cleaned], dtype="string")
    return output
