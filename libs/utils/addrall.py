"""Batched, general-purpose address enrichment using Keystone AddrAll.

Geocode distinct building addresses, recover stored ZIP+4, normalize with
addresses.normalize(text[]), and preserve every input row on misses. Returns
canonical address fields and source/quality metadata without claiming USPS DPV.
"""
from __future__ import annotations

import atexit
from concurrent.futures import ThreadPoolExecutor
from time import perf_counter
import math
import os
import re
import struct
import threading
from collections.abc import Sequence
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pandas as pd



# Guard against a documented upstream address_standardizer backend crash
# for text near 272 characters. The input remains in the output.
_MAX_SQL_ADDRESS_LENGTH = 240

NORM_FIELDS = (
    "address", "predir_abbrev", "street_name", "street_type_abbrev",
    "postdir_abbrev", "internal", "location", "state_abbrev", "zip",
    "parsed", "zip4", "address_alphanumeric",
)
# Exact norm_addy JSON keys with their internal SQL result aliases.
# Keep the geocoder-returned object distinct from post-normalization results.
GEOCODER_NORM_ADDY_FIELDS = {
    "address": "norm_address",
    "predirAbbrev": "norm_predir_abbrev",
    "streetName": "norm_street_name",
    "streetTypeAbbrev": "norm_street_type_abbrev",
    "postdirAbbrev": "norm_postdir_abbrev",
    "internal": "norm_internal",
    "location": "norm_location",
    "stateAbbrev": "norm_state_abbrev",
    "zip": "norm_zip",
    "zip4": "norm_zip4",
    "address_alphanumeric": "norm_address_alphanumeric",
}
SMARTY_COLUMNS = (
    "delivery_line_1", "delivery_line_2", "city_name", "state_abbreviation",
    "zipcode", "plus4_code", "full_zipcode", "latitude", "longitude",
)

# Use AddrAll's batched array normalizer for all input datasets.
# Index each norm_addy[] element before projecting composite fields; using
# unnest(norm_addy[]) in FROM expands fields and breaks the record alias.
NORMALIZE_SQL = """
WITH batch AS MATERIALIZED (
    SELECT addresses.normalize(%s::text[]) AS normalized
),
indexed AS MATERIALIZED (
    SELECT
        s.input_index::int AS input_index,
        b.normalized[s.input_index] AS normalized_address
    FROM batch AS b
    CROSS JOIN LATERAL generate_subscripts(b.normalized, 1)
        AS s(input_index)
)
SELECT
    p.input_index,
    (p.normalized_address).address AS norm_address,
    (p.normalized_address).predirAbbrev AS norm_predir_abbrev,
    (p.normalized_address).streetName AS norm_street_name,
    (p.normalized_address).streetTypeAbbrev AS norm_street_type_abbrev,
    (p.normalized_address).postdirAbbrev AS norm_postdir_abbrev,
    (p.normalized_address).internal AS norm_internal,
    (p.normalized_address).location AS norm_location,
    (p.normalized_address).stateAbbrev AS norm_state_abbrev,
    (p.normalized_address).zip AS norm_zip,
    (p.normalized_address).parsed AS norm_parsed,
    (p.normalized_address).zip4 AS norm_zip4,
    (p.normalized_address).address_alphanumeric AS norm_address_alphanumeric
FROM indexed AS p
ORDER BY p.input_index
"""

# Backward-compatible SQL alias; no second target-only implementation.
TARGET_NORMALIZE_SQL = NORMALIZE_SQL

# Optional ZIP -> city/state enrichment, resolved once per DataFrame.
# Never choose an arbitrary city when a ZIP spans multiple postal city names.
ZIP_LOCALITY_SQL = """
SELECT z.zip::text AS zip5,
       CASE WHEN COUNT(DISTINCT z.city::text) = 1
            THEN MIN(z.city::text) END AS city,
       MIN(z.state::text) AS state
FROM tiger.zip_lookup_base AS z
WHERE z.zip = ANY(%s::text[])
GROUP BY z.zip
HAVING COUNT(DISTINCT z.state) = 1
"""

# Geocoder's normalized_address may describe its parsed input, rather
# than the stored match. Trust it only with a strong, house-level match.
GEOCODE_SQL = """
SELECT
    g.input_index, g.source, g.address_id, g.zip4,
    g.rating AS geocode_rating, g."match" AS geocode_match,
    g.geomout::text AS geomout_hex,
    (g.normalized_address).address AS norm_address,
    (g.normalized_address).predirAbbrev AS norm_predir_abbrev,
    (g.normalized_address).streetName AS norm_street_name,
    (g.normalized_address).streetTypeAbbrev AS norm_street_type_abbrev,
    (g.normalized_address).postdirAbbrev AS norm_postdir_abbrev,
    (g.normalized_address).internal AS norm_internal,
    (g.normalized_address).location AS norm_location,
    (g.normalized_address).stateAbbrev AS norm_state_abbrev,
    (g.normalized_address).zip AS norm_zip,
    (g.normalized_address).parsed AS norm_parsed,
    (g.normalized_address).zip4 AS norm_zip4,
    (g.normalized_address).address_alphanumeric AS norm_address_alphanumeric
FROM addresses.geocode_address(%s::text[], %s::text[]) AS g
"""

_POOL_LOCK = threading.Lock()
_POOLS: dict[str, tuple[Any, threading.BoundedSemaphore]] = {}
class AddrAllConfigurationError(RuntimeError):
    """Bad Keystone database bind or unavailable PostgreSQL connection."""


def _postgres_dsn(bind: str) -> str:
    bind = bind.strip()
    if "://" not in bind:
        if "=" in bind:
            return bind  # libpq keyword DSN
        raise AddrAllConfigurationError("Invalid DATABIND_SQL_KEYSTONE")
    parsed = urlsplit(bind)
    if parsed.scheme.lower() not in {"postgresql", "postgres", "postgresql+psycopg2"}:
        raise AddrAllConfigurationError("DATABIND_SQL_KEYSTONE must use PostgreSQL")
    return urlunsplit(("postgresql", parsed.netloc, parsed.path, parsed.query, parsed.fragment))


@contextmanager
def keystone_connection(database_url: str | None = None):
    """Borrow a thread-safe pooled psycopg2 connection; no ORM/reflection."""
    bind = database_url or os.getenv("DATABIND_SQL_KEYSTONE")
    if not bind:
        raise AddrAllConfigurationError("Set DATABIND_SQL_KEYSTONE")
    dsn = _postgres_dsn(bind)
    with _POOL_LOCK:
        if dsn not in _POOLS:
            try:
                from psycopg2.pool import ThreadedConnectionPool
            except ImportError as exc:
                raise AddrAllConfigurationError("Install psycopg2") from exc
            size = int(os.getenv("ADDRALL_POOL_SIZE", "4"))
            if size < 1:
                raise AddrAllConfigurationError("ADDRALL_POOL_SIZE must be positive")
            _POOLS[dsn] = (
                ThreadedConnectionPool(
                    1, size, dsn=dsn, connect_timeout=15,
                    application_name="matchback_addrall",
                ),
                threading.BoundedSemaphore(size),
            )
        pool, limiter = _POOLS[dsn]
    with limiter:
        connection = pool.getconn()
        try:
            connection.autocommit = True
            yield connection
        finally:
            pool.putconn(connection, close=bool(connection.closed))


def close_connection_pools() -> None:
    """Only call when no queries are in flight."""
    with _POOL_LOCK:
        for pool, _ in _POOLS.values():
            pool.closeall()
        _POOLS.clear()


atexit.register(close_connection_pools)


def as_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if bool(pd.isna(value)):
            return ""
    except (ValueError, TypeError):
        pass
    value = str(value).strip()
    return "" if value.lower() in {"nan", "none", "null", "na", "n/a"} else value


_STREET_APOSTROPHE_VARIANTS = str.maketrans({
    "\u2018": "'", "\u2019": "'", "\u02bc": "'",
    "\u201b": "'", "\uff07": "'",
})
# A known broken-apostrophe spelling in source data. Do not generically
# merge "D <word>" / "O <word>": those can be legitimate street names.
_SPACED_DANDREA = re.compile(r"(?<![A-Z0-9])D\s+ANDREA(?![A-Z0-9])", re.I)


def prepare_street_apostrophes(value: Any) -> str:
    """Repair apostrophe spellings before geocoding AND DB normalization.

    Preserve a single ASCII apostrophe for the geocoder, which already
    recognizes D'ANDREA. Leave names without apostrophes unchanged.
    """
    street = as_text(value).upper().translate(_STREET_APOSTROPHE_VARIANTS)
    street = re.sub(r"(?<=[A-Z0-9])\s*'+\s*(?=[A-Z0-9])", "'", street)
    street = _SPACED_DANDREA.sub("D'ANDREA", street)
    return " ".join(street.split())


def canonical_street_key(value: Any) -> str:
    """Fold apostrophe/no-apostrophe spellings in final building match keys.

    Both sales and targets use this after DB normalization; originals and
    secondary units remain untouched. Do not turn apostrophes into spaces,
    since D'ANDREA and DANDREA must have the same join key.
    """
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



_UNIT_END = re.compile(
    r"(?<!\w)(?:APARTMENT|APT|SUITE|STE|UNIT|ROOM|RM|LOT|#)"
    r"\s*#?\s*-?[A-Z0-9]+(?:-[A-Z0-9]+)*\s*$",
    flags=re.IGNORECASE,
)
_HOUSE = re.compile(r"^[A-Z0-9]+(?:-[A-Z0-9]+)*", flags=re.IGNORECASE)
_NUMBER_STREET_FALLBACK = re.compile(
    r"^\s*((?:\d+[A-Z0-9-]*|[A-Z]+\d+[A-Z0-9-]*))\s+(.+?)\s*$",
    flags=re.IGNORECASE,
)

# Conservative, symmetric fallback normalization for records absent from the
# address store. Only whole-word directional / road-type expansions are folded;
# never invent a house number, street, ZIP, unit or ZIP+4.
_STREET_ALIASES = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW",
    "SOUTHEAST": "SE", "SOUTHWEST": "SW",
    "STREET": "ST", "ROAD": "RD", "AVENUE": "AVE",
    "BOULEVARD": "BLVD", "DRIVE": "DR", "COURT": "CT",
    "CIRCLE": "CIR", "LANE": "LN", "PLACE": "PL",
    "PARKWAY": "PKWY", "TERRACE": "TER",
    "TRAIL": "TRL", "HIGHWAY": "HWY", "COVE": "CV",
    "VALLEY": "VLY", "HOLLOW": "HOLW",
}
_STREET_ALIAS_PATTERN = re.compile(
    r"\b(" + "|".join(sorted(_STREET_ALIASES, key=len, reverse=True)) + r")\b",
    flags=re.IGNORECASE,
)


def standardize_street_text(value: Any) -> str:
    """Best-effort, database-independent street formatting on both datasets."""
    street = re.sub(r"\s+", " ", canonical_street_key(value).replace(".", " ")).strip()
    street = re.sub(r"^P\s+O\s+BOX\b", "PO BOX", street)
    street = _STREET_ALIAS_PATTERN.sub(
        lambda m: _STREET_ALIASES[m.group(1).upper()], street
    )
    return re.sub(r"\s+", " ", street).strip()


def split_secondary(street: Any) -> tuple[str, str]:
    """Remove a trailing apartment/suite for building lookup; preserve it verbatim."""
    raw = as_text(street).strip()
    match = _UNIT_END.search(raw)
    if not match:
        return prepare_street_apostrophes(raw), ""
    building = raw[:match.start()].strip()
    return prepare_street_apostrophes(building), raw[match.start():].strip()


def build_address_text(
    street: Any, city: Any = None, state: Any = None, zipcode: Any = None,
) -> str:
    """Database validation and geocoding input (without a secondary unit)."""
    building, _ = split_secondary(street)
    return _join_address_fields(
        building, city, state, normalize_zipcode(zipcode)
    )


def split_zip_parts(zipcode: Any, zip4: Any = None) -> tuple[str | None, str | None]:
    """Handle ZIP, ZIP+4 in one column, or an explicit four-digit extension."""
    # A standalone four-digit ZIP+4 is not a ZIP5.
    five = normalize_zipcode(zipcode)
    if not five and re.fullmatch(r"\d{5}(?:[- ]?\d{4})?", as_text(zip4)):
        five = normalize_zipcode(zip4)
    four = (normalize_zip4(zip4) or zip4_from_full_zip(zipcode)
            or zip4_from_full_zip(zip4))
    return five, four


def coerce_match_key_types(
    df: pd.DataFrame,
    *,
    zipcode: str,
    zip4: str | None = None,
    full_zipcode_col: str = "full_zipcode",
    inplace: bool = False,
) -> pd.DataFrame:
    """Normalize the exact postal join columns after all DataFrame merges.

    Handles numbers read as floats, ZIP+4 embedded in a ZIP/full-ZIP column,
    leading zeros, and nullable keys. Never borrow a ZIP+4 from a different
    ZIP5. Keep duplicate indexes and row order unchanged.
    """
    if zipcode not in df.columns:
        raise KeyError(f"Missing ZIP column: {zipcode!r}")
    out = df if inplace else df.copy()
    raw_zip5 = out[zipcode].tolist()
    raw_zip4 = (
        out[zip4].tolist()
        if zip4 and zip4 in out.columns else [None] * len(out)
    )
    raw_full = (
        out[full_zipcode_col].tolist()
        if full_zipcode_col in out.columns else [None] * len(out)
    )

    zip5_values = []
    zip4_values = []
    full_values = []
    for five_value, four_value, full_value in zip(raw_zip5, raw_zip4, raw_full):
        five = normalize_zipcode(five_value) or normalize_zipcode(full_value)
        four = normalize_zip4(four_value) or zip4_from_full_zip(five_value)
        if not four and five and normalize_zipcode(full_value) == five:
            four = zip4_from_full_zip(full_value)
        zip5_values.append(five)
        zip4_values.append(four)
        full_values.append(full_zipcode(five, four))

    # pd.array assigns positionally even when CSV concatenation left
    # duplicate index labels; no reindexing of the input DataFrame.
    out[zipcode] = pd.array(zip5_values, dtype="string")
    if zip4 and zip4 in out.columns:
        out[zip4] = pd.array(zip4_values, dtype="string")
    out[full_zipcode_col] = pd.array(full_values, dtype="string")
    return out


def _select_rows(conn: Any, sql: str, params: tuple) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute(sql, params)
        names = [
            getattr(d, "name", d[0] if isinstance(d, tuple) else None)
            for d in cursor.description
        ]
        return [dict(zip(names, values)) for values in cursor.fetchall()]


def _select_normalized_rows(
    conn: Any, addresses: list[str],
) -> list[dict[str, Any]]:
    """Normalize in batches, isolating only malformed *input* records.

    Data exceptions in the PostgreSQL parser must not discard the entire
    source chunk. A failed individual address receives a null parser record
    so its original fields can survive. Other errors, including timeouts and
    connection failures, propagate instead of masquerading as bad addresses.
    """
    if not addresses:
        return []

    input_error_codes = {"22003", "22P02", "22001", "2201B"}

    def fetch(start: int, end: int) -> list[dict[str, Any]]:
        try:
            rows = _select_rows_with_savepoint(
                conn, NORMALIZE_SQL, (addresses[start:end],)
            )
        except Exception as exc:
            if getattr(exc, "pgcode", None) not in input_error_codes:
                raise
            if end - start == 1:
                return [{"input_index": start + 1, "normalize_error": True}]
            middle = (start + end) // 2
            return fetch(start, middle) + fetch(middle, end)

        for row in rows:
            row["input_index"] = start + int(row["input_index"])
        return rows

    rows = fetch(0, len(addresses))
    expected = list(range(1, len(addresses) + 1))
    indices = [int(row["input_index"]) for row in rows]
    if indices != expected:
        raise AddrAllConfigurationError(
            "addresses.normalize(text[]) returned incorrectly indexed rows; "
            f"expected {len(addresses)}, got {indices[:10]}. "
            "Refusing to return inconsistent match keys."
        )
    return rows


def _select_rows_with_savepoint(conn: Any, sql: str, params: tuple) -> list[dict[str, Any]]:
    """Allow optional lookups or recoverable per-record errors in a transaction.

    The shared SQLAlchemy bind uses a DBAPI transaction, while the module's
    own psycopg pool uses autocommit. In either case an optional failed lookup
    must not poison the next normalize/geocode statement.
    """
    if getattr(conn, "autocommit", False):
        return _select_rows(conn, sql, params)
    with conn.cursor() as cursor:
        cursor.execute("SAVEPOINT addrall_optional_lookup")
    try:
        rows = _select_rows(conn, sql, params)
    except Exception:
        with conn.cursor() as cursor:
            cursor.execute("ROLLBACK TO SAVEPOINT addrall_optional_lookup")
        raise
    finally:
        with conn.cursor() as cursor:
            cursor.execute("RELEASE SAVEPOINT addrall_optional_lookup")
    return rows


def _select_geocode_rows(
    conn: Any,
    texts: list[str],
    sources: list[str],
    *,
    skip_invalid_regex: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    """Optionally isolate a malformed input on PostgreSQL SQLSTATE 2201B.

    Recursively retry smaller batches; preserve the original input indexes.
    All other database errors propagate.
    """
    if not skip_invalid_regex:
        return _select_rows(conn, GEOCODE_SQL, (texts, sources)), 0

    skipped = 0

    def fetch(start: int, end: int) -> list[dict[str, Any]]:
        nonlocal skipped
        try:
            rows = _select_rows_with_savepoint(
                conn, GEOCODE_SQL, (texts[start:end], sources)
            )
        except Exception as exc:
            if getattr(exc, "pgcode", None) != "2201B":
                raise
            if end - start == 1:
                skipped += 1
                print(
                    "\t\t\t\tAddrAll geocode skipped regex error: "
                    f"{texts[start]!r}"
                )
                return []
            middle = (start + end) // 2
            return fetch(start, middle) + fetch(middle, end)

        # A sub-batch starts its input_index at 1. Restore the index within
        # the original geocode call before _lookup_batch maps to its requests.
        for row in rows:
            row["input_index"] = start + int(row["input_index"])
        return rows

    return fetch(0, len(texts)), skipped


def _point_from_ewkb_hex(value: Any) -> tuple[float, float] | None:
    """Decode an EPSG:4326 PostGIS POINT without calling ST_X/ST_Y.

    A geometry cast to text produces EWKB hex. Only 2D points are used;
    missing/unsupported geometry leaves existing input coordinates intact.
    """
    if not isinstance(value, str):
        return None
    text = value.strip()
    if text.startswith("\\x"):
        text = text[2:]
    try:
        data = bytes.fromhex(text)
    except ValueError:
        return None
    if len(data) < 21:
        return None
    if data[0] == 1:
        order = "<"
    elif data[0] == 0:
        order = ">"
    else:
        return None
    try:
        geom_type = struct.unpack_from(order + "I", data, 1)[0]
        # EWKB Z/M/SRID flags occupy the top bits; require 2D POINT.
        if (geom_type & 0xC0000000) or (geom_type & 0x0FFFFFFF) != 1:
            return None
        offset = 5
        if geom_type & 0x20000000:
            srid = struct.unpack_from(order + "I", data, offset)[0]
            if srid != 4326:
                return None
            offset += 4
        longitude, latitude = struct.unpack_from(order + "dd", data, offset)
    except struct.error:
        return None
    if not (math.isfinite(latitude) and math.isfinite(longitude)):
        return None
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return None
    return latitude, longitude


def _record_to_address(record: dict[str, Any]) -> dict[str, Any]:
    """Format stored PostGIS composite fields into Smarty-compatible parts."""
    result = dict(record)
    if "geomout_hex" in result:
        point = _point_from_ewkb_hex(result.pop("geomout_hex"))
        if point is not None:
            result["latitude"], result["longitude"] = point
    for field in NORM_FIELDS:
        name = f"norm_{field}"
        if field not in ("parsed", "address"):
            result[name] = as_text(result.get(name)).upper() or None
    number = (
        as_text(result.get("norm_address_alphanumeric"))
        or as_text(result.get("norm_address"))
    ).upper()
    street_name = result.get("norm_street_name")
    parts = (
        number, result.get("norm_predir_abbrev"), street_name,
        result.get("norm_street_type_abbrev"), result.get("norm_postdir_abbrev"),
    )
    result["standardized_street"] = (
        " ".join(part for part in parts if part)
        if number and street_name else None
    )
    result["standardized_city"] = result.get("norm_location")
    result["standardized_state"] = result.get("norm_state_abbrev")
    result["standardized_zipcode"] = normalize_zipcode(result.get("norm_zip"))
    result["zip4"] = (
        normalize_zip4(result.get("zip4"))
        or normalize_zip4(result.get("norm_zip4"))
    )
    return result


def _house_number(street: Any) -> str | None:
    match = _HOUSE.match(as_text(street))
    return match.group(0).upper() if match else None


def _usable_candidate(
    candidate: dict[str, Any], request: tuple[str, str, str, str | None],
    *, allow_state_correction: bool = False,
    allow_zip_correction: bool = False,
) -> bool:
    """Protect the house number while allowing evidence-backed locality fixes.

    Same ZIP is the strongest locality anchor; the database can then correct a
    city alias or mistyped state. A changed ZIP is only safe when *both* the
    supplied city and state agree with the stored address. The caller also
    requires an exact-street match code before permitting that correction.
    """
    street, city, state, zip5 = request
    stored_street = candidate.get("standardized_street")
    stored_city = candidate.get("standardized_city")
    stored_state = candidate.get("standardized_state")
    stored_zip = candidate.get("standardized_zipcode")
    if not stored_street or not stored_state or not stored_zip:
        return False
    if not _house_number(street) or _house_number(street) != _house_number(stored_street):
        return False

    if zip5 and stored_zip != zip5:
        return bool(
            allow_zip_correction
            and city and state
            and stored_city == city and stored_state == state
        )
    if state and stored_state != state:
        # A ZIP-consistent stored address can correct a wrong state.
        if not (allow_state_correction and zip5 and stored_zip == zip5):
            return False
    if not zip5 and city and stored_city != city:
        return False
    # A ZIP-consistent stored address may use a different postal city name.
    return True


def _choose_geocode(
    raw: dict[str, Any], request: tuple | None = None,
) -> dict[str, Any] | None:
    """Project an actual geocoder hit, not a source='none' placeholder.

    Do not reject database matches for input locality disagreements. They are
    often the precise corrections the enrichment pipeline is intended to make.
    TIGER is not postal confirmation and cannot supply a verified ZIP+4.
    """
    source = as_text(raw.get("source")).lower()
    if source in {"", "none"}:
        return None
    result = _record_to_address(raw)
    # Preserve the exact typed composite fields from the geocoder. Do not
    # derive identity from _record_to_address's display normalization.
    result["geocoder_norm_addy"] = {
        name: raw.get(field)
        for name, field in GEOCODER_NORM_ADDY_FIELDS.items()
    }
    parts = {part.strip().lower() for part in as_text(raw.get("geocode_match")).split(",")}
    if source == "tiger":
        result["match_status"] = (
            "tiger_interpolated" if "interpolated" in parts else "tiger_approximate"
        )
    else:
        result["match_status"] = "geocoded"
    result["legacy_compatible"] = source in {"address", "openaddresses"}
    if source == "tiger" or parts.intersection({"any_unit", "missing_unit", "building"}):
        # A building-level or uncertain-secondary hit does not establish a
        # secondary unit's ZIP+4. TIGER also cannot prove a postal extension.
        result["zip4"] = None
    return result



def _choose_tiger_street(
    raw: dict[str, Any], request: tuple[str, str, str, str | None],
) -> dict[str, Any] | None:
    """Use a TIGER street correction as an unverified comparison key.

    TIGER interpolation can repair street spelling, but supplies no USPS
    ZIP+4. Require the same house number and ZIP/state, and an interpolated
    street-level result; never accept street-only or locality-only fallback.
    """
    if raw.get("source") != "tiger":
        return None
    parts = {
        part.strip().lower()
        for part in as_text(raw.get("geocode_match")).split(",")
    }
    if "interpolated" not in parts:
        return None
    candidate = _record_to_address(raw)
    if not _usable_candidate(candidate, request):
        return None
    candidate["match_status"] = "tiger_interpolated"
    candidate["legacy_compatible"] = False
    candidate["zip4"] = None
    return candidate


def _load_zip_localities(conn: Any, requests: list[tuple]) -> dict[str, tuple[str, str]]:
    """Look up missing city/state from ZIP once, not once per address.

    ZIP data is an optional optimization. A missing TIGER lookup table should
    never prevent an address from being cleaned. Database errors in actual
    geocoding/normalization are *not* swallowed.
    """
    zips = sorted({
        zip5 for _, city, state, zip5 in requests
        if zip5 and (not city or not state)
    })
    if not zips or os.getenv("ADDRALL_ZIP_LOCALITY", "0").lower() not in {"1", "true", "yes", "on"}:
        return {}
    started = perf_counter()
    try:
        rows = _select_rows_with_savepoint(conn, ZIP_LOCALITY_SQL, (zips,))
    except Exception as exc:
        print(f"\t\tAddrAll ZIP locality unavailable ({type(exc).__name__}: {exc}). "
              "ZIP-only geocoding may be slow.")
        return {}
    result = {
        row["zip5"]: (as_text(row.get("city")).upper(),
                       as_text(row.get("state")).upper())
        for row in rows
        if row.get("zip5") and len(as_text(row.get("state"))) == 2
    }
    if os.getenv("ADDRALL_PROFILE", "").lower() in {"1", "true", "yes"}:
        print(f"\t\tAddrAll ZIP locality: {len(result)}/{len(zips)} ZIPs mapped "
              f"in {perf_counter() - started:.2f}s")
    return result


_TRAILING_US_COUNTRY = re.compile(
    r"(?<![A-Z0-9])"
    r"(?:UNITED\s+STATES(?:\s+OF\s+AMERICA)?"
    r"|U\.?\s*S\.?\s*A\.?"
    r"|U\.?\s*S\.?)"
    r"[\s,;.]*$",
    flags=re.IGNORECASE,
)


def _sql_address_text(value: Any) -> str:
    """Remove country suffix for database lookup/normalization text only."""
    text = as_text(value)
    match = _TRAILING_US_COUNTRY.search(text)
    return text[:match.start()].rstrip(" \t\r\n,;") if match else text


def _join_address_fields(*parts: Any) -> str:
    """Comma-join supplied parts, excluding country-only fields and tails."""
    return ", ".join(
        text for part in parts if (text := _sql_address_text(part))
    )


def _geocode_text_variants(
    request: tuple[str, str, str, str | None],
    originals: Sequence[str],
    locality: dict[str, tuple[str, str]] | None,
) -> list[tuple[str, str]]:
    """Keep useful complete-address alternatives, not redundant street-only calls.

    If locality is supplied in separate columns, the first query already
    contains the building street plus that locality. Retrying only the street
    (even with a trailing secondary unit or country suffix) drops information
    and needlessly loads the geocoder. Preserve genuinely complete, different
    address representations and all single-field address variants.
    """
    street, city, state, zip5 = request
    variants = [("combined_fields", _query_text(request, locality))]
    has_separate_locality = any(as_text(part) for part in (city, state, zip5))
    building_street = prepare_street_apostrophes(
        _sql_address_text(street)
    ).strip(" ,;.")
    for original in originals:
        raw = _sql_address_text(original)
        if not raw or raw == variants[0][1]:
            continue
        if has_separate_locality:
            original_building, _ = split_secondary(raw)
            original_street_only = prepare_street_apostrophes(
                _sql_address_text(original_building)
            ).strip(" ,;.")
            if original_street_only == building_street:
                # No new city, state or ZIP: this is just the street again.
                continue
        variants.append(("preserved_input", raw))
    return list(dict((text, (label, text)) for label, text in variants if text).values())


def _geocode_richness(candidate: dict[str, Any]) -> tuple[int, int, int]:
    """Rank returned DB data by enrichment, never by similarity to the input."""
    return (
        int(bool(normalize_zip4(candidate.get("zip4")))),
        sum(bool(candidate.get(k)) for k in (
            "standardized_street", "standardized_city",
            "standardized_state", "standardized_zipcode"
        )),
        int(bool(candidate.get("standardized_zipcode"))),
    )


def _normalization_richness(candidate: dict[str, Any]) -> tuple[int, int]:
    """Compare parser outputs by useful fields, never against source input."""
    return (
        5 * int(bool(candidate.get("standardized_street")))
        + 5 * int(bool(candidate.get("standardized_zipcode")))
        + 2 * int(bool(candidate.get("standardized_city")))
        + 2 * int(bool(candidate.get("standardized_state"))),
        sum(bool(candidate.get(k)) for k in (
            "standardized_street", "standardized_city",
            "standardized_state", "standardized_zipcode"
        )),
    )


def _query_text(
    request: tuple[str, str, str, str | None],
    locality: dict[str, tuple[str, str]] | None = None,
) -> str:
    """Join populated columns for the geocoder; never parse single-field text."""
    street, city, state, zip5 = request
    inferred_city, inferred_state = (locality or {}).get(zip5, ("", ""))
    return _join_address_fields(
        street, city or inferred_city, state or inferred_state, zip5
    )


def _choose_normalized(
    raw: dict[str, Any], request: tuple[str, str, str, str | None],
    *, trust_parser: bool = False,
) -> dict[str, Any] | None:
    """Use the DB parser's canonical street, preserving house and locality.

    A direct addresses.normalize() result can change a street token such as
    "87" -> "87TH"; rejecting that change would discard the target's
    normalized key. A normalized_address attached to a *rejected geocode*
    still needs the stricter token-overlap check.
    """
    # The direct text[] normalizer's output is authoritative. Only the
    # rejected-GEOCODE interpretation of norm_addy needs candidate checks.
    if trust_parser:
        return _choose_final_normalize(raw)
    candidate = _record_to_address(raw)
    street, city, state, zip5 = request
    if not candidate.get("standardized_street"):
        return None
    if _house_number(street) != _house_number(candidate["standardized_street"]):
        return None
    if zip5 and candidate.get("standardized_zipcode") not in (None, zip5):
        return None
    if state and candidate.get("standardized_state") not in (None, state):
        return None
    # A rejected geocoder hit may be unrelated despite matching ZIP and house.
    # Direct addresses.normalize() results do not need this extra veto: the
    # database parser is the authority for both sales and target street keys.
    if not trust_parser:
        ignore = {"N", "S", "E", "W", "NE", "NW", "SE", "SW", "ST",
                  "RD", "AVE", "DR", "CT", "CIR", "LN", "BLVD", "PL",
                  "TER", "TRL", "PKWY", "HWY"}
        def street_tokens(value: str) -> set[str]:
            text = standardize_street_text(value)
            return {
                part for part in re.findall(r"[A-Z0-9]+", text)[1:]
                if part not in ignore
            }
        original_tokens = street_tokens(street)
        normalized_tokens = street_tokens(candidate["standardized_street"])
        if original_tokens and normalized_tokens and not (
            original_tokens & normalized_tokens
        ):
            return None
    candidate["match_status"] = "normalized_only"
    candidate["legacy_compatible"] = False
    # Parser output is not a verified postal match. Never claim ZIP+4.
    candidate["zip4"] = None
    return candidate



def _choose_final_normalize(raw: dict[str, Any]) -> dict[str, Any]:
    """One canonical PostgreSQL formatter for sales AND street targets.

    addresses.normalize(text[]) is a formatter, not an address validator.
    No house/street/city/state/ZIP equality veto is applied here. The same
    norm_addy fields construct both sides' final address comparison key.
    Verified ZIP+4 stays separate and is never taken from this parser.
    """
    candidate = _record_to_address(raw)
    candidate["zip4"] = None
    return candidate


def _choose_final_sales_normalize(
    raw: dict[str, Any],
    prepared: tuple[str, str, str, str | None],
) -> dict[str, Any]:
    """Compatibility wrapper for existing applications."""
    return _choose_final_normalize(raw)


def _lookup_batch(
    batch: list[tuple[str, str, str, str | None]],
    *,
    locality: dict[str, tuple[str, str]],
    sources: list[str],
    use_geocode: bool,
    conn: Any,
    original_texts: dict[tuple, list[str]] | None = None,
    skip_invalid_geocode_regex: bool = True,
    compact: bool = False,
    postal_retry: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Geocode informative input variants, then normalize DB-selected fields.

    Keep DB evidence ahead of parser interpretations, original text only as
    fallback, and avoid redundant street-only geocoder calls. This operates
    on the bounded batch passed by the Azure Function, not an entire blob.
    """
    options = [
        [(label, text) for label, text in _geocode_text_variants(
            key, (original_texts or {}).get(key, ()), locality
        ) if len(text) <= _MAX_SQL_ADDRESS_LENGTH]
        for key in batch
    ]
    results = [
        {"match_status": "input_formatted", "legacy_compatible": False,
         "geocode_input_text": variants[0][1] if variants and use_geocode else None,
         "normalize_status": None}
        for variants in options
    ]
    chosen: dict[int, dict[str, Any]] = {}
    observed: dict[int, dict[str, Any]] = {}
    attempted: dict[int, dict[str, None]] = {}
    timings = {"geocode_seconds": 0.0, "normalize_seconds": 0.0}
    counts = {"geocoded": 0, "matched": 0, "normalized": 0}

    if use_geocode:
        for attempt_no in range(max(map(len, options), default=0)):
            query_to_input: dict[str, list[int]] = {}
            labels: dict[tuple[int, str], str] = {}
            for idx, variants in enumerate(options):
                if attempt_no >= len(variants):
                    continue
                previous = chosen.get(idx)
                if previous:
                    complete = all(previous.get(field) for field in (
                        "standardized_street", "standardized_city",
                        "standardized_state", "standardized_zipcode",
                    ))
                    # Stored addresses are already good; a postal-only retry
                    # (when requested) happens later with final delivery text.
                    if complete and (postal_retry or
                        previous.get("source") in {"address", "openaddresses"}):
                        continue
                label, query = variants[attempt_no]
                seen = attempted.setdefault(idx, {})
                if query in seen:
                    continue
                seen[query] = None
                query_to_input.setdefault(query, []).append(idx)
                labels[(idx, query)] = label
            if not query_to_input:
                continue
            texts = list(query_to_input)
            start = perf_counter()
            geos, skipped = _select_geocode_rows(
                conn, texts, sources,
                skip_invalid_regex=skip_invalid_geocode_regex,
            )
            timings["geocode_seconds"] += perf_counter() - start
            counts["geocoded"] += len(texts)
            if skipped and os.getenv("ADDRALL_PROFILE", "0") == "1":
                print(f"AddrAll: {skipped} malformed geocode inputs skipped")
            # One SQL input can return multiple candidates; ignore explicit
            # misses and use the most informative actual database response.
            selected: dict[int, dict[str, Any]] = {}
            for raw in geos:
                pos = int(raw["input_index"]) - 1
                if not 0 <= pos < len(texts):
                    raise ValueError("Unexpected geocode input_index")
                result = _choose_geocode(raw)
                if result is None:
                    for idx in query_to_input[texts[pos]]:
                        observed.setdefault(idx, raw)
                    continue
                if (pos not in selected or
                    _geocode_richness(result) > _geocode_richness(selected[pos])):
                    selected[pos] = result
            for pos, candidate in selected.items():
                query = texts[pos]
                for idx in query_to_input[query]:
                    prior = chosen.get(idx)
                    if prior is None or _geocode_richness(candidate) > _geocode_richness(prior):
                        chosen[idx] = candidate
                        if not compact:
                            results[idx]["geocode_success_input_text"] = query
                            results[idx]["geocode_success_variant"] = labels[idx, query]
        for idx, result in enumerate(results):
            candidate = chosen.get(idx)
            if candidate:
                result.update(candidate)
                # Keep the geocoder's returned norm_addy distinct from the
                # later addresses.normalize() pass and from raw-input fallbacks.
                # Sales identity uses this original returned structure.
                result["geocoder_norm_addy"] = candidate.get("geocoder_norm_addy")
                result["geocode_source"] = candidate.get("source")
                result["geocode_match"] = candidate.get("geocode_match")
                result["geocode_rating"] = candidate.get("geocode_rating")
                result["geocode_street_pre_normalize"] = candidate.get("standardized_street")
            elif idx in observed:
                result["geocode_source"] = observed[idx].get("source")
                result["geocode_match"] = observed[idx].get("geocode_match")
            if not compact:
                result["geocode_attempted_texts"] = " || ".join(attempted.get(idx, ()))
    counts["matched"] = len(chosen)

    # The formatter receives the geocoded address when available. A single
    # free-form input retains its own SQL text rather than being split in Python.
    parser_options: dict[int, list[tuple[str, str]]] = {}
    for idx, (key, result) in enumerate(zip(batch, results)):
        if idx in chosen:
            text = _join_address_fields(
                result.get("standardized_street") or key[0],
                result.get("standardized_city") or key[1],
                result.get("standardized_state") or key[2],
                result.get("standardized_zipcode") or key[3],
            )
        else:
            text = options[idx][0][1] if options[idx] else ""
        if not compact:
            result["normalize_input_text"] = text or None
        if not text:
            if batch[idx][0] and not options[idx]:
                result["normalize_status"] = "skipped_overlong"
            continue
        variants = [("geocoded_or_combined", text)]
        current = chosen.get(idx, {})
        if not current or not all(current.get(k) for k in (
            "standardized_street", "standardized_city",
            "standardized_state", "standardized_zipcode",
        )):
            for label, query in options[idx]:
                if query != text:
                    variants.append((label, query))
        # Length guard protects the address_standardizer backend; skipped text
        # is retained as original input in the final delivery fields.
        variants = [(label, query) for label, query in variants
                    if len(query) <= _MAX_SQL_ADDRESS_LENGTH]
        if variants:
            parser_options[idx] = variants
        elif text:
            result["normalize_status"] = "skipped_overlong"

    address_fields = (
        "standardized_street", "standardized_city",
        "standardized_state", "standardized_zipcode",
    )
    normalized_cache: dict[str, dict[str, Any]] = {}
    best: dict[int, dict[str, Any]] = {}
    primary: dict[int, dict[str, Any]] = {}
    alternate_values: dict[int, list[dict[str, Any]]] = {}
    normalized_attempts: dict[int, list[str]] = {}
    for attempt_no in range(max(map(len, parser_options.values()), default=0)):
        query_to_input: dict[str, list[int]] = {}
        for idx, variants in parser_options.items():
            if attempt_no >= len(variants):
                continue
            if attempt_no:
                parsed = best.get(idx, {})
                stored = chosen.get(idx, {})
                if all(parsed.get(field) or stored.get(field) for field in address_fields):
                    continue
            query = variants[attempt_no][1]
            query_to_input.setdefault(query, []).append(idx)
            if not compact:
                normalized_attempts.setdefault(idx, []).append(query)
        missing = [text for text in query_to_input if text not in normalized_cache]
        if missing:
            start = perf_counter()
            parsed_rows = _select_normalized_rows(conn, missing)
            timings["normalize_seconds"] += perf_counter() - start
            counts["normalized"] += len(missing)
            normalized_cache.update({
                text: _choose_final_normalize(raw)
                for text, raw in zip(missing, parsed_rows)
            })
            # Preserve an isolated per-address parser error separately.
            for text, raw in zip(missing, parsed_rows):
                if raw.get("normalize_error"):
                    normalized_cache[text]["normalize_error"] = True
        for text, indices in query_to_input.items():
            candidate = normalized_cache[text]
            for idx in indices:
                alternate_values.setdefault(idx, []).append(candidate)
                primary.setdefault(idx, candidate)
                if idx not in best or _normalization_richness(candidate) > _normalization_richness(best[idx]):
                    best[idx] = candidate
                    if not compact:
                        results[idx]["normalize_input_text"] = text

    for idx, candidate in best.items():
        result = results[idx]
        candidate = dict(candidate)
        for alternate in alternate_values[idx]:
            for field in address_fields:
                if not candidate.get(field) and alternate.get(field):
                    candidate[field] = alternate[field]
        if idx in chosen:
            building = (primary.get(idx, {}).get("standardized_street")
                        or result.get("standardized_street"))
        else:
            building = candidate.get("standardized_street")
        result["normalize_status"] = (
            "applied" if building else
            "parser_input_error" if candidate.get("normalize_error") else "unstructured"
        )
        if building:
            result["standardized_street"] = building
        for field in address_fields[1:]:
            if candidate.get(field) and not result.get(field):
                result[field] = candidate[field]
        # Preserve the parser's original structured fields for Smarty consumers.
        # Components and canonical street must describe the same parse.
        # Other representations may fill missing locality, not replace the
        # structured street of a successful geocode.
        structured = (
            primary.get(idx, {})
            if idx in chosen and primary.get(idx, {}).get("standardized_street")
            else candidate
        )
        for name in NORM_FIELDS:
            field = f"norm_{name}"
            if field in structured:
                result[field] = structured[field]
        if not compact:
            result["normalize_raw_street"] = building
            result["normalize_attempted_texts"] = " || ".join(normalized_attempts.get(idx, ()))
        result["normalized_after_geocode"] = idx in chosen
        if result["match_status"] == "input_formatted" and building:
            result["match_status"] = "normalized_only"
    if compact:
        fields = (
            "standardized_street", "standardized_city", "standardized_state",
            "standardized_zipcode", "zip4", "norm_internal", "norm_address",
            "norm_address_alphanumeric", "norm_street_name",
            "norm_street_type_abbrev", "norm_predir_abbrev",
            "norm_postdir_abbrev", "latitude", "longitude",
            "geocode_source", "geocode_match", "geocode_rating",
            "match_status", "legacy_compatible", "normalize_status",
        )
        results = [{field: row.get(field) for field in fields} for row in results]
    metrics = {
        **timings, **counts,
        "normalize_applied": sum(r.get("normalize_status") == "applied" for r in results),
        "normalize_unstructured": sum(r.get("normalize_status") == "unstructured" for r in results),
    }
    return results, metrics


def _resolve_buildings(
    requests: list[tuple[str, str, str, str | None]],
    *, conn: Any, batch_size: int, use_geocode: bool,
    sources: list[str], workers: int, database_url: str | None = None,
    locality: dict[str, tuple[str, str]] | None = None,
    original_texts: dict[tuple, list[str]] | None = None,
    skip_invalid_geocode_regex: bool = True,
    compact: bool = False, postal_retry: bool = False,
) -> list[dict[str, Any]]:
    """Batch geocode/normalize, gathering batches without retaining duplicates."""
    if not requests:
        return []
    locality = locality or {}
    batches = [requests[i:i + batch_size] for i in range(0, len(requests), batch_size)]

    def lookup(batch):
        if conn is not None:
            return _lookup_batch(
                batch, locality=locality, sources=sources,
                use_geocode=use_geocode, conn=conn,
                original_texts=original_texts,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                compact=compact, postal_retry=postal_retry,
            )
        with keystone_connection(database_url) as worker_conn:
            return _lookup_batch(
                batch, locality=locality, sources=sources,
                use_geocode=use_geocode, conn=worker_conn,
                original_texts=original_texts,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                compact=compact, postal_retry=postal_retry,
            )

    started = perf_counter()
    found: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    if workers == 1:
        completed = map(lookup, batches)
        for batch_found, batch_metrics in completed:
            found.extend(batch_found)
            metrics.append(batch_metrics)
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for batch_found, batch_metrics in executor.map(lookup, batches):
                found.extend(batch_found)
                metrics.append(batch_metrics)
    if os.getenv("ADDRALL_PROFILE", "0").lower() in {"1", "true", "yes"}:
        print(
            f"AddrAll: {len(requests)} distinct, {len(batches)} batches, "
            f"{workers} DB workers, {perf_counter() - started:.2f}s; "
            f"geocode {sum(m['geocode_seconds'] for m in metrics):.2f}s; "
            f"normalize {sum(m['normalize_seconds'] for m in metrics):.2f}s"
        )
    return found


def _retry_zip4_by_key(
    keys: list[tuple[str, str, str, str | None]],
    by_key: dict[tuple, dict[str, Any]],
    *,
    original_texts: dict[tuple, list[str]],
    conn: Any,
    batch_size: int,
    skip_invalid_geocode_regex: bool,
    eligible_keys: set[tuple] | None = None,
) -> None:
    """Recover stored ZIP+4 after normalization, without a sales-only record pass.

    Try corrected delivery first, then complete preserved geocoder input, then
    a genuinely different full original address. Re-normalize successful postal
    results. Missing ZIP+4 never causes an existing address to be replaced.
    """
    unresolved = {
        key for key in keys
        if key[0] and (eligible_keys is None or key in eligible_keys)
        and not normalize_zip4(by_key[key].get("zip4"))
    }
    seen: dict[tuple, set[str]] = {key: set() for key in unresolved}
    variants = ("normalized_delivery", "initial_geocode", "original_raw")
    for variant in variants:
        if not unresolved:
            break
        query_to_keys: dict[str, list[tuple]] = {}
        for key in unresolved:
            row = by_key[key]
            if variant == "normalized_delivery":
                query = _join_address_fields(
                    row.get("standardized_street") or key[0],
                    row.get("standardized_city") or key[1],
                    row.get("standardized_state") or key[2],
                    row.get("standardized_zipcode") or key[3],
                )
            elif variant == "initial_geocode":
                query = row.get("geocode_input_text") or _query_text(key, {})
            else:
                choices = _geocode_text_variants(key, original_texts.get(key, ()), {})
                preserved = [text for label, text in choices if label == "preserved_input"]
                query = next((text for text in preserved if text not in seen[key]), "")
            if not query or len(query) > _MAX_SQL_ADDRESS_LENGTH or query in seen[key]:
                continue
            seen[key].add(query)
            query_to_keys.setdefault(query, []).append(key)
        if not query_to_keys:
            continue
        queries = list(query_to_keys)
        for start in range(0, len(queries), batch_size):
            chunk = queries[start:start + batch_size]
            geos, _ = _select_geocode_rows(
                conn, chunk, ["usps", "openaddresses"],
                skip_invalid_regex=skip_invalid_geocode_regex,
            )
            # Choose a real postal candidate carrying ZIP+4. No fabricated
            # postal extension from the parser or TIGER interpolation.
            candidates: dict[int, dict[str, Any]] = {}
            for geo in geos:
                idx = int(geo["input_index"]) - 1
                if not 0 <= idx < len(chunk):
                    raise ValueError("Unexpected postal retry input_index")
                candidate = _choose_geocode(geo)
                if (candidate is not None and
                    candidate.get("source") in {"address", "openaddresses"} and
                    normalize_zip4(candidate.get("zip4"))):
                    candidates.setdefault(idx, candidate)
            if not candidates:
                continue
            hits = sorted(candidates.items())
            norm_queries = [
                _join_address_fields(
                    candidate.get("standardized_street") or chunk[idx],
                    candidate.get("standardized_city"),
                    candidate.get("standardized_state"),
                    candidate.get("standardized_zipcode"),
                )
                for idx, candidate in hits
            ]
            valid_idx = [i for i, text in enumerate(norm_queries) if len(text) <= _MAX_SQL_ADDRESS_LENGTH]
            parsed = {}
            if valid_idx:
                parsed_rows = _select_normalized_rows(conn, [norm_queries[i] for i in valid_idx])
                parsed = {valid_idx[i]: _choose_final_normalize(row)
                          for i, row in enumerate(parsed_rows)}
            for hit_idx, (idx, candidate) in enumerate(hits):
                parsed_fields = parsed.get(hit_idx, {})
                for key in query_to_keys[chunk[idx]]:
                    if key not in unresolved:
                        continue
                    previous = by_key[key]
                    # Recovered postal match overrides the first-pass ZIP5
                    # only together with its own ZIP+4, not an old extension.
                    for field in ("standardized_street", "standardized_city",
                                  "standardized_state", "standardized_zipcode"):
                        value = (parsed_fields.get(field) if field == "standardized_street"
                                 else None) or candidate.get(field) or previous.get(field)
                        if value:
                            previous[field] = value
                    for name in NORM_FIELDS:
                        field = f"norm_{name}"
                        if field in parsed_fields:
                            previous[field] = parsed_fields[field]
                    for field in ("latitude", "longitude"):
                        if candidate.get(field) is not None:
                            previous[field] = candidate[field]
                    previous["zip4"] = normalize_zip4(candidate["zip4"])
                    previous["match_status"] = "geocoded_retry"
                    previous["legacy_compatible"] = True
                    previous["source"] = candidate.get("source")
                    previous["address_id"] = candidate.get("address_id")
                    previous["geocode_source"] = candidate.get("source")
                    previous["geocode_match"] = candidate.get("geocode_match")
                    previous["geocode_rating"] = candidate.get("geocode_rating")
                    previous["normalize_status"] = (
                        "applied" if parsed_fields.get("standardized_street")
                        else previous.get("normalize_status")
                    )
                    previous["zip4_retry_variant"] = variant
                    unresolved.remove(key)



def _secondary_lookup_text(inline_unit: Any, address2: Any) -> str:
    """Keep original secondary identifiers; suppress duplicated line1/line2."""
    a, b = as_text(inline_unit), as_text(address2)
    if a and b and a.upper() == b.upper():
        return a
    return " ".join(part for part in (a, b) if part)


def _recover_secondary_zip4(
    *,
    keys: list[tuple[str, str, str, str | None]],
    bases: list[tuple[str, str]],
    address2_values: list[str],
    zip_parts: list[tuple[str | None, str | None]],
    by_key: dict[tuple, dict[str, Any]],
    conn: Any,
    batch_size: int,
    skip_invalid_geocode_regex: bool,
    require_zip4: bool = True,
    include_supplied_zip4: bool = False,
) -> dict[tuple, dict[str, Any]]:
    """Recover explicit unit-level geocoder matches (and, usually, ZIP+4).

    First-pass geocoding deduplicates buildings and cannot establish a specific
    unit's ZIP+4. Query distinct *complete unit addresses* via postal sources,
    and accept only geocoder match codes containing `unit`. Unrecognized or
    unknown units keep the original ZIP+4 (or remain missing).
    """
    text_to_units: dict[str, list[tuple]] = {}
    for key, (_, unit), address2, (_, supplied_four) in zip(
        keys, bases, address2_values, zip_parts
    ):
        supplied_unit = _secondary_lookup_text(unit, address2)
        if not key[0] or not supplied_unit or (supplied_four and not include_supplied_zip4):
            continue
        candidate = by_key.get(key, {})
        query = _join_address_fields(
            " ".join(x for x in (
                candidate.get("standardized_street") or key[0], supplied_unit
            ) if x),
            candidate.get("standardized_city") or key[1],
            candidate.get("standardized_state") or key[2],
            candidate.get("standardized_zipcode") or key[3],
        )
        if query and len(query) <= _MAX_SQL_ADDRESS_LENGTH:
            text_to_units.setdefault(query, []).append((key, supplied_unit))
    if not text_to_units:
        return {}
    resolved: dict[tuple, dict[str, Any]] = {}
    queries = list(text_to_units)
    for start in range(0, len(queries), batch_size):
        chunk = queries[start:start + batch_size]
        raw_rows, _ = _select_geocode_rows(
            conn, chunk, ["usps", "openaddresses"],
            skip_invalid_regex=skip_invalid_geocode_regex,
        )
        for raw in raw_rows:
            idx = int(raw["input_index"]) - 1
            if not 0 <= idx < len(chunk):
                raise ValueError("Unexpected secondary ZIP+4 input_index")
            match = {part.strip().lower() for part in
                     as_text(raw.get("geocode_match")).split(",")}
            if "unit" not in match:
                continue
            selected = _choose_geocode(raw)
            if (selected is None or
                selected.get("source") not in {"address", "openaddresses"} or
                (require_zip4 and not normalize_zip4(selected.get("zip4")))):
                continue
            for unit_key in text_to_units[chunk[idx]]:
                resolved.setdefault(unit_key, selected)
    return resolved



def clean_addresses(
    df: pd.DataFrame, ADDRESS: str, CITY: str | None = None,
    STATE: str | None = None, ZIPCODE: str | None = None, *,
    ZIP4: str | None = None, ADDRESS2: str | None = None,
    connection: Any = None, database_url: str | None = None,
    batch_size: int | None = None, candidate_limit: int = 50,
    keep_invalid: bool = True, use_geocode: bool = True,
    retry_missing_zip4: bool = True,
    skip_invalid_geocode_regex: bool = True,
    geocode_sources: Sequence[str] | None = None,
    output_mode: str = "full",
    canonical_geocoder_output: bool = False,
) -> pd.DataFrame:
    """Return standardized address, coordinates and provenance with all input rows.

    Distinct buildings are geocoded in batches, can recover ZIP+4 through a
    second stored-address lookup, and are normalized by the same database
    formatter. Unresolved addresses keep their best original components.
    Input secondary units are never invented or silently discarded.

    output_mode='audience' returns only enrichment fields (without copying the
    original DataFrame or building the full Smarty/diagnostic field set).
    The Azure audience activity merges these fields into its original chunk.
    """
    if ADDRESS not in df:
        raise KeyError(f"Missing address column {ADDRESS!r}")
    if output_mode not in {"full", "audience"}:
        raise ValueError("output_mode must be 'full' or 'audience'")
    size = int(batch_size or os.getenv("ADDRALL_BATCH_SIZE", "500"))
    if size < 1 or not 1 <= candidate_limit <= 100:
        raise ValueError("batch_size must be positive; candidate_limit must be 1..100")

    def values(col: str | None) -> list[str]:
        return (
            [as_text(value) for value in df[col].tolist()]
            if col and col in df else [""] * len(df)
        )

    raw_street, raw_city = values(ADDRESS), values(CITY)
    raw_state, raw_zip = values(STATE), values(ZIPCODE)
    raw_zip4 = values(ZIP4)
    raw_full_zipcode = values("full_zipcode")
    raw_address2 = values(ADDRESS2)
    raw_latitude = values("latitude")
    raw_longitude = values("longitude")
    bases = [split_secondary(street) for street in raw_street]
    zip_parts = []
    for z, ext, full in zip(raw_zip, raw_zip4, raw_full_zipcode):
        five, four = split_zip_parts(z, ext)
        full_five = normalize_zipcode(full)
        if not five:
            five = full_five
        # An existing full_zipcode is another input field, but never borrow
        # its extension when it belongs to a different ZIP5.
        if not four and five and five == full_five:
            four = zip4_from_full_zip(full)
        zip_parts.append((five, four))
    keys = [
        (base.upper(), city.upper(), state.upper(), five)
        for (base, _), city, state, (five, _) in zip(
            bases, raw_city, raw_state, zip_parts
        )
    ]
    unique = list(dict.fromkeys(key for key in keys if key[0]))
    preserved_streets = values("addrall_original_street") if "addrall_original_street" in df else raw_street
    original_texts: dict[tuple, list[str]] = {}
    for key, supplied, original in zip(keys, raw_street, preserved_streets):
        if not key[0]:
            continue
        choices = original_texts.setdefault(key, [])
        for text in (supplied, original):
            if text and text not in choices:
                choices.append(text)
    # Azure hosts may run several activities at once; do not allocate a thread
    # pool and four DB sessions per invocation unless explicitly requested.
    workers = max(1, int(os.getenv("ADDRALL_WORKERS", "1")))
    if connection is not None:
        workers = 1
    else:
        # All slots are available after the single ZIP-locality query.
        pool_size = max(1, int(os.getenv("ADDRALL_POOL_SIZE", "4")))
        workers = min(workers, pool_size)
    configured_sources = (
        geocode_sources if geocode_sources is not None
        else os.getenv("ADDRALL_GEOCODE_SOURCES", "usps,openaddresses")
    )
    if isinstance(configured_sources, str):
        configured_sources = configured_sources.split(",")
    sources = [str(source).strip().lower() for source in configured_sources if str(source).strip()]
    if not sources or any(s not in {"usps", "openaddresses", "tiger"} for s in sources):
        raise ValueError("Invalid ADDRALL_GEOCODE_SOURCES")
    started = perf_counter()
    if unique:
        if connection is None:
            # Release this connection before the parallel geocode batches:
            # all pool connections can then be used by the workers.
            with keystone_connection(database_url) as conn:
                locality = _load_zip_localities(conn, unique)
            found = _resolve_buildings(
                unique, conn=None, batch_size=size, locality=locality,
                use_geocode=use_geocode, sources=sources,
                workers=workers, database_url=database_url,
                original_texts=original_texts,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                compact=(output_mode == "audience"),
                postal_retry=retry_missing_zip4,
            )
        else:
            found = _resolve_buildings(
                unique, conn=connection, batch_size=size,
                use_geocode=use_geocode, sources=sources, workers=1,
                original_texts=original_texts,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                compact=(output_mode == "audience"),
                postal_retry=retry_missing_zip4,
            )
        by_key = dict(zip(unique, found))
        if use_geocode and retry_missing_zip4:
            if connection is None:
                with keystone_connection(database_url) as conn:
                    _retry_zip4_by_key(
                        unique, by_key, original_texts=original_texts,
                        conn=conn, batch_size=size,
                        skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                        eligible_keys={
                            key for key, (_, unit), address2, (_, ext) in zip(
                                keys, bases, raw_address2, zip_parts
                            ) if not ext and not (unit or address2)
                        },
                    )
            else:
                _retry_zip4_by_key(
                    unique, by_key, original_texts=original_texts,
                    conn=connection, batch_size=size,
                    skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                    eligible_keys={
                        key for key, (_, unit), address2, (_, ext) in zip(
                            keys, bases, raw_address2, zip_parts
                        ) if not ext and not (unit or address2)
                    },
                )
    else:
        by_key = {}

    # Only explicit secondary-unit matches can supplement an apartment's ZIP+4.
    # Bounded by the caller's current CSV chunk and distinct unit keys.
    unit_matches: dict[tuple, dict[str, Any]] = {}
    if use_geocode and (retry_missing_zip4 or canonical_geocoder_output) and unique and any(
        unit or address2 for (_, unit), address2 in zip(bases, raw_address2)
    ):
        if connection is None:
            with keystone_connection(database_url) as conn:
                unit_matches = _recover_secondary_zip4(
                    keys=keys, bases=bases, address2_values=raw_address2,
                    zip_parts=zip_parts, by_key=by_key, conn=conn,
                    batch_size=size,
                    skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                    require_zip4=not canonical_geocoder_output,
                    include_supplied_zip4=canonical_geocoder_output,
                )
        else:
            unit_matches = _recover_secondary_zip4(
                keys=keys, bases=bases, address2_values=raw_address2,
                zip_parts=zip_parts, by_key=by_key, conn=connection,
                batch_size=size,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
                require_zip4=not canonical_geocoder_output,
                include_supplied_zip4=canonical_geocoder_output,
            )

    records: list[dict[str, Any]] = []
    for i, key in enumerate(keys):
        match = by_key.get(
            key, {"validated": None, "match_status": "input_formatted"}
        )
        # Retain compatibility with previously established match keys.
        legacy_key = bool(match.get("legacy_compatible"))
        std_street = canonical_street_key(
            match.get("standardized_street") or bases[i][0]
        )
        # Preserve the original *unconfirmed* unit exactly, as Smarty did
        # with unknown-secondary (DPV S).
        unit = bases[i][1]
        delivery_line_1 = " ".join(x for x in (std_street, unit) if x).upper()
        city = match.get("standardized_city") or raw_city[i].upper()
        state = match.get("standardized_state") or raw_state[i].upper()
        if not legacy_key:
            city = re.sub(r"\s+", " ", city).strip()
            state = re.sub(r"\s+", " ", state).strip()
        # A nonstandard input ZIP remains visible if no normalized ZIP exists.
        # It is deliberately not used to construct a false ZIP+4 key.
        zip5 = match.get("standardized_zipcode") or zip_parts[i][0] or raw_zip[i] or None
        # Preserve both ZIP+4 keys. The canonical full_zipcode is used by
        # existing matchback joins; the input key is retained for diagnostics
        # and possible secondary matching when the database changes it.
        input_zip5, input_zip4 = zip_parts[i]
        # Building hits cannot supply apartment ZIP+4; an explicit unit hit can.
        has_secondary = bool(unit or raw_address2[i])
        matched_unit = unit_matches.get((key, _secondary_lookup_text(unit, raw_address2[i])))
        postal_match = matched_unit if has_secondary else match
        # A building-level geocode cannot identify a specific unit. When
        # requested, expose ONLY the explicitly matched unit's canonical
        # structure; an unrecognized unit has no canonical identity.
        geocoder_norm_addy = (
            matched_unit.get("geocoder_norm_addy")
            if matched_unit and (
                not canonical_geocoder_output or as_text(matched_unit.get("norm_internal"))
            ) else
            None if has_secondary and canonical_geocoder_output else
            match.get("geocoder_norm_addy")
        )
        verified_zip4 = normalize_zip4(postal_match.get("zip4")) if postal_match else None
        stored_zip5 = normalize_zipcode(postal_match.get("standardized_zipcode")) if postal_match else None
        if matched_unit and stored_zip5:
            zip5 = stored_zip5
        row_status = "geocoded_retry" if matched_unit else match.get("match_status")
        row_geo_source = matched_unit.get("source") if matched_unit else match.get("geocode_source")
        row_geo_match = matched_unit.get("geocode_match") if matched_unit else match.get("geocode_match")
        row_geo_rating = matched_unit.get("geocode_rating") if matched_unit else match.get("geocode_rating")
        row_secondary_status = (
            "postal_unit_match" if matched_unit else
            "unverified" if has_secondary else "absent"
        )
        input_full_zipcode = full_zipcode(input_zip5, input_zip4)
        database_full_zipcode = full_zipcode(stored_zip5, verified_zip4)
        input_zip4_compatible = not stored_zip5 or stored_zip5 == input_zip5
        if verified_zip4:
            zip4 = verified_zip4
            zip4_source = "database"
        elif input_zip4 and input_zip4_compatible:
            zip4 = input_zip4
            zip4_source = "input"
        else:
            # Never combine a newly normalized ZIP5 with an extension
            # belonging to a different input ZIP5.
            zip4 = None
            zip4_source = (
                "input_zip_mismatch" if input_zip4 else "missing"
            )
            if not input_zip4 and raw_zip4[i] and (
                not stored_zip5 or stored_zip5 == input_zip5
            ):
                # Keep an uninterpretable input extension as supplied, without
                # treating it as a valid postal join key.
                zip4 = raw_zip4[i]
                zip4_source = "input_unparsed"
        coord_lat = (matched_unit.get("latitude") if matched_unit else None)
        coord_lon = (matched_unit.get("longitude") if matched_unit else None)
        if coord_lat is None or coord_lon is None:
            coord_lat = match.get("latitude")
            coord_lon = match.get("longitude")
        if coord_lat is None or coord_lon is None:
            coord_lat = raw_latitude[i] or None
            coord_lon = raw_longitude[i] or None
            coord_source = "input" if coord_lat is not None and coord_lon is not None else None
        else:
            coord_source = row_geo_source or match.get("source")
        if output_mode == "audience":
            # Neighbor processing needs these components, not the full Smarty
            # metadata/AddrAll debug records. The input DataFrame stays with
            # the caller, so misses never erase original fields.
            fallback_parts = _NUMBER_STREET_FALLBACK.match(bases[i][0])
            number = (
                as_text(match.get("norm_address_alphanumeric"))
                or as_text(match.get("norm_address"))
                or (fallback_parts.group(1) if fallback_parts else "")
            )
            # If a caller supplied a separately parsed street_name, preserve
            # it when AddrAll cannot parse the address. The entire raw street
            # suffix is not a safe replacement for that existing component.
            has_source_street_name = any(
                name in df.columns for name in
                ("street_name", "streetName", "streetname")
            )
            street_name = (
                as_text(match.get("norm_street_name"))
                or (fallback_parts.group(2) if fallback_parts and not has_source_street_name else "")
            )
            last_line = " ".join(
                part for part in (
                    as_text(city), as_text(state),
                    full_zipcode(zip5, zip4) or as_text(zip5),
                ) if part
            )
            records.append({
                "primary_number": number or None,
                "street_name": street_name or None,
                "street_suffix": match.get("norm_street_type_abbrev"),
                "street_predirection": match.get("norm_predir_abbrev"),
                "street_postdirection": match.get("norm_postdir_abbrev"),
                "delivery_line_1": delivery_line_1 or None,
                "delivery_line_2": raw_address2[i] or (match.get("norm_internal") if not unit else None),
                "last_line": last_line or None,
                "city_name": city or None,
                "state_abbreviation": state or None,
                "zipcode": zip5,
                "plus4_code": zip4,
                "full_zipcode": full_zipcode(zip5, zip4),
                "latitude": coord_lat,
                "longitude": coord_lon,
                "addrall_match_status": row_status,
                "addrall_geocode_source": row_geo_source,
                "addrall_geocode_match": row_geo_match,
                "addrall_geocode_rating": row_geo_rating,
                "addrall_coordinate_source": coord_source,
                "addrall_zip4_source": zip4_source,
                "addrall_normalize_status": match.get("normalize_status"),
                "addrall_secondary_status": row_secondary_status,
            })
            continue
        result = {
            "addrall_validated": match.get("validated"),
            "addrall_match_status": row_status,
            "addrall_legacy_key_compatible": legacy_key,
            "addrall_source": match.get("source"),
            "addrall_address_id": match.get("address_id"),
            "addrall_geocode_source": row_geo_source,
            "addrall_geocode_match": row_geo_match,
            "addrall_geocode_rating": row_geo_rating,
            "addrall_geocode_input_text": match.get("geocode_input_text"),
            "addrall_geocode_attempted_texts": match.get("geocode_attempted_texts"),
            "addrall_geocode_success_variant": match.get("geocode_success_variant"),
            "addrall_zip4_retry_variant": match.get("zip4_retry_variant"),
            "addrall_normalize_attempted_texts": match.get("normalize_attempted_texts"),
            "addrall_coordinate_source": coord_source,
            "addrall_unit_count": match.get("unit_count"),
            "addrall_secondary_status": row_secondary_status,
            "addrall_zip4_source": zip4_source,
            "addrall_input_full_zipcode": input_full_zipcode,
            "addrall_database_full_zipcode": database_full_zipcode,
            "addrall_original_street": preserved_streets[i] or raw_street[i],
            "addrall_original_address2": raw_address2[i],
            "addrall_original_city": raw_city[i],
            "addrall_original_state": raw_state[i],
            "addrall_original_zipcode": raw_zip[i],
            "addrall_original_zip4": raw_zip4[i],
            "addrall_original_full_zipcode": raw_full_zipcode[i],
            "addrall_original_latitude": raw_latitude[i],
            "addrall_original_longitude": raw_longitude[i],
            "addrall_building_street": std_street or None,
            "addrall_geocode_street_pre_normalize": match.get("geocode_street_pre_normalize"),
            "addrall_normalize_input_text": match.get("normalize_input_text"),
            "addrall_normalized_after_geocode": match.get("normalized_after_geocode"),
            "addrall_normalize_status": match.get("normalize_status"),
            "addrall_normalize_raw_street": match.get("normalize_raw_street"),
            "delivery_line_1": delivery_line_1 or None,
            "delivery_line_2": raw_address2[i] or (match.get("norm_internal") if not unit else None),
            "city_name": city or None,
            "state_abbreviation": state or None,
            "zipcode": zip5,
            "plus4_code": zip4,
            "full_zipcode": full_zipcode(zip5, zip4),
            "latitude": coord_lat,
            "longitude": coord_lon,
        }
        if canonical_geocoder_output:
            result["addrall_geocoder_norm_addy"] = geocoder_norm_addy
        result.update({f"addrall_{name}": match.get(name) for name in (
            "norm_address", "norm_predir_abbrev", "norm_street_name",
            "norm_street_type_abbrev", "norm_postdir_abbrev", "norm_internal",
            "norm_location", "norm_state_abbrev", "norm_zip", "norm_parsed",
            "norm_zip4", "norm_address_alphanumeric",
        )})
        records.append(result)

    if output_mode == "audience":
        # No client-column copy and no double set of diagnostic columns.
        projected = pd.DataFrame.from_records(records, columns=(
            "primary_number", "street_name", "street_suffix",
            "street_predirection", "street_postdirection",
            "delivery_line_1", "delivery_line_2", "last_line",
            "city_name", "state_abbreviation", "zipcode", "plus4_code",
            "full_zipcode", "latitude", "longitude", "addrall_match_status",
            "addrall_geocode_source", "addrall_geocode_match",
            "addrall_geocode_rating", "addrall_coordinate_source",
            "addrall_zip4_source", "addrall_normalize_status",
            "addrall_secondary_status",
        ))
        projected.index = df.index
        return projected

    output = df.copy()
    fields = {
        key: [record[key] for record in records]
        for key in (records[0].keys() if records else (
            "addrall_validated", "addrall_match_status",
            "addrall_legacy_key_compatible", "addrall_source",
            "addrall_address_id", "addrall_geocoder_norm_addy",
            "addrall_geocode_source",
            "addrall_geocode_match", "addrall_geocode_rating",
            "addrall_geocode_input_text", "addrall_geocode_attempted_texts",
            "addrall_geocode_success_variant", "addrall_zip4_retry_variant",
            "addrall_normalize_attempted_texts", "addrall_coordinate_source",
            "addrall_unit_count",
            "addrall_secondary_status", "addrall_zip4_source",
            "addrall_input_full_zipcode", "addrall_database_full_zipcode",
            "addrall_original_street", "addrall_original_address2",
            "addrall_original_city", "addrall_original_state",
            "addrall_original_zipcode", "addrall_original_zip4",
            "addrall_original_full_zipcode", "addrall_original_latitude",
            "addrall_original_longitude", "addrall_building_street",
            "addrall_geocode_street_pre_normalize",
            "addrall_normalize_input_text",
            "addrall_normalized_after_geocode",
            "addrall_normalize_status", "addrall_normalize_raw_street",
            *SMARTY_COLUMNS, *(f"addrall_norm_{f}" for f in NORM_FIELDS),
        ))
    }
    for field, values_list in fields.items():
        # The importer can pass original_* fields captured before its own
        # uppercase/ZIP normalization. Do not replace them with intermediate
        # preprocessed values.
        output[field] = (
            df[field].tolist()
            if field.startswith("addrall_original_") and field in df
            else values_list
        )

    # Preserve incoming client-column order and present standardized address
    # fields before optional diagnostics.
    client_columns = [
        column for column in df.columns if not column.startswith("addrall_")
    ]
    address_columns = [
        column for column in SMARTY_COLUMNS if column in output.columns
    ]
    diagnostic_columns = [
        column for column in output.columns
        if column not in client_columns and column not in address_columns
    ]
    ordered_columns = list(dict.fromkeys(
        client_columns + address_columns + diagnostic_columns
    ))
    output = output.loc[:, ordered_columns]

    if not keep_invalid:
        output = output.loc[
            output["addrall_match_status"].isin(["geocoded", "geocoded_retry", "tiger_interpolated", "normalized_only"])
        ].copy()
    if os.getenv("ADDRALL_PROFILE", "").lower() in {"1", "true", "yes"}:
        print(
            f"\t\t\t\tAddrAll DataFrame: {len(df):,} rows -> {len(output):,} rows, "
            f"{len(unique):,} unique buildings, "
            f"total {perf_counter() - started:.2f}s"
        )
    return output
