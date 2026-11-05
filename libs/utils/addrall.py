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
    return " ".join(
        part for part in (
            building, as_text(city), as_text(state),
            normalize_zipcode(zipcode) or "",
        ) if part
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
    raw: dict[str, Any], request: tuple[str, str, str, str | None],
) -> dict[str, Any] | None:
    """Use unambiguous stored-address hits to correct street and locality.

    The geocoder has already selected the best candidate. Preserve its
    street-level spelling corrections, ZIP-consistent city aliases, and
    ZIP-consistent wrong-state corrections. Changing the input ZIP requires
    stronger proof: an exact street match plus agreement on city and state.

    No TIGER interpolation or street-only/locality-only fallback is treated
    as a stored, ZIP+4-eligible postal address.
    """
    if raw.get("source") not in {"address", "openaddresses"}:
        return None
    parts = {
        part.strip().lower()
        for part in as_text(raw.get("geocode_match")).split(",")
    }
    if not parts.intersection({"street", "street_name", "street_corrected"}):
        return None
    if "interpolated" in parts or "locality" in parts:
        return None
    if "zip_city_differs" in parts and not request[3]:
        return None

    candidate = _record_to_address(raw)
    if not _usable_candidate(
        candidate, request,
        allow_state_correction="state_differs" in parts,
        allow_zip_correction=(
            "city_zip_differs" in parts and "street" in parts
        ),
    ):
        return None
    # A matched building without the requested secondary unit cannot prove
    # which ZIP+4 belongs to that secondary unit.
    if parts.intersection({"any_unit", "missing_unit", "building"}):
        candidate["zip4"] = None
    candidate["match_status"] = "geocoded"
    return candidate



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
    if not zips or os.getenv("ADDRALL_ZIP_LOCALITY", "1") == "0":
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


def _query_text(
    request: tuple[str, str, str, str | None],
    locality: dict[str, tuple[str, str]],
) -> str:
    street, city, state, zip5 = request
    guessed_city, guessed_state = locality.get(zip5, ("", ""))
    return " ".join(
        part for part in (street, city or guessed_city,
                          state or guessed_state, zip5 or "")
        if part
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
    retry_keys: set[tuple] | None = None,
    skip_invalid_geocode_regex: bool = False,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Geocode -> optional ZIP+4 recovery -> normalize final address.

    No independent Python alias substitution is applied to parser results.
    Stored-address evidence and ZIP+4 remain available if parsing fails.
    """
    results: list[dict[str, Any]] = [
        {"validated": None, "match_status": "input_formatted",
         "legacy_compatible": False}
        for _ in batch
    ]
    texts = [_query_text(req, locality) for req in batch]
    geocode_seconds = normalize_seconds = retry_seconds = 0.0
    attempted_geo = matched_geo = parsed_from_geo = retry_matches = 0


    # Step 1: Geocode eligible building addresses.
    if use_geocode:
        eligible = [
            i for i, (street, city, state, zip5) in enumerate(batch)
            if any(ch.isdigit() for ch in (_house_number(street) or ""))
            and (city or state or zip5 or re.search(r"\b\d{5}(?:-\d{4})?\b", street))
            and len(texts[i]) <= _MAX_SQL_ADDRESS_LENGTH
        ]
        if eligible:
            started = perf_counter()
            geos, regex_skipped = _select_geocode_rows(
                conn, [texts[i] for i in eligible], sources,
                skip_invalid_regex=skip_invalid_geocode_regex,
            )
            geocode_seconds = perf_counter() - started
            attempted_geo = len(eligible)
            if regex_skipped:
                print(
                    f"\t\t\t\tAddrAll: {regex_skipped} geocode "
                    "regex error(s); those streets will still be normalized."
                )
            for geo in geos:
                idx = int(geo["input_index"]) - 1
                if not 0 <= idx < len(eligible):
                    raise ValueError("Unexpected geocode input_index")
                i = eligible[idx]
                selected = _choose_geocode(geo, batch[i])
                if selected is not None:
                    selected["legacy_compatible"] = True
                    matched_geo += 1
                else:
                    selected = _choose_tiger_street(geo, batch[i])
                    if selected is None:
                        selected = _choose_normalized(geo, batch[i])
                    if selected is not None:
                        parsed_from_geo += 1
                if selected is not None:
                    results[i].update(selected)
                    results[i]["geocode_street_pre_normalize"] = (
                        selected.get("standardized_street")
                    )
                results[i]["geocode_source"] = geo.get("source")
                results[i]["geocode_match"] = geo.get("geocode_match")
                results[i]["geocode_rating"] = geo.get("geocode_rating")

    # Step 2: Retry *corrected* first-pass addresses still missing ZIP+4.
    # The second pass deliberately excludes TIGER.
    retry: list[tuple[int, str]] = []
    if use_geocode and retry_keys:
        for i, (request, result) in enumerate(zip(batch, results)):
            if request not in retry_keys or normalize_zip4(result.get("zip4")):
                continue
            street = result.get("standardized_street")
            city = result.get("standardized_city")
            state = result.get("standardized_state")
            zip5 = result.get("standardized_zipcode")
            if not all((street, city, state, zip5)):
                continue
            if _house_number(street) != _house_number(request[0]):
                continue
            if request[3] and zip5 != request[3]:
                continue
            if request[2] and state != request[2]:
                continue
            corrected = " ".join((street, city, state, zip5))
            if len(corrected) > _MAX_SQL_ADDRESS_LENGTH:
                continue
            if " ".join(corrected.upper().split()) == " ".join(texts[i].upper().split()):
                continue
            retry.append((i, corrected))

    if retry:
        started = perf_counter()
        retry_rows, _ = _select_geocode_rows(
            conn, [text for _, text in retry], ["usps", "openaddresses"],
            skip_invalid_regex=skip_invalid_geocode_regex,
        )
        retry_seconds = perf_counter() - started
        for row in retry_rows:
            idx = int(row["input_index"]) - 1
            if not 0 <= idx < len(retry):
                raise ValueError("Unexpected ZIP+4 retry input_index")
            i = retry[idx][0]
            selected = _choose_geocode(row, batch[i])
            if selected is None or not normalize_zip4(selected.get("zip4")):
                continue
            selected["legacy_compatible"] = True
            selected["match_status"] = "geocoded_retry"
            results[i].update(selected)
            results[i]["geocode_street_pre_normalize"] = (
                selected.get("standardized_street")
            )
            results[i]["geocode_source"] = row.get("source")
            results[i]["geocode_match"] = row.get("geocode_match")
            results[i]["geocode_rating"] = row.get("geocode_rating")
            retry_matches += 1

    # Step 3: Let the database parser create the final street representation.
    # Feed the corrected street/locality instead of the original misspelling.
    #
    # Parsing may never introduce a ZIP+4. Preserve it from the geocoder
    # (or supplied input) independently of normalized_address.
    to_parse: list[tuple[int, str]] = []
    for i, (request, result) in enumerate(zip(batch, results)):
        street = result.get("standardized_street") or request[0]
        if not as_text(street):
            continue
        # Normalize all street-bearing inputs, including PO boxes.
        city = result.get("standardized_city") or request[1]
        state = result.get("standardized_state") or request[2]
        zip5 = result.get("standardized_zipcode") or request[3]
        # Do not give the street parser ZIP+4; keep extensions independently
        # from the standardized street/locality representation.
        parse_text = " ".join(
            part for part in (street, city, state, zip5 or "") if part
        )
        result["normalize_input_text"] = parse_text
        if len(parse_text) > _MAX_SQL_ADDRESS_LENGTH:
            # Upstream address_standardizer may crash its backend with very
            # long input; keep the original address instead.
            result["normalize_status"] = "skipped_overlong"
            continue
        to_parse.append((i, parse_text))

    if to_parse:
        started = perf_counter()
        normalized = _select_normalized_rows(
            conn, [text for _, text in to_parse],
        )
        normalize_seconds = perf_counter() - started
        for row in normalized:
            idx = int(row["input_index"]) - 1
            if not 0 <= idx < len(to_parse):
                raise ValueError("Unexpected normalize input_index")
            i, _ = to_parse[idx]
            result = results[i]
            selected = _choose_final_normalize(row)

            result["normalize_raw_street"] = selected.get("standardized_street")
            # This is the SAME writeback for geocoded sales and target-only
            # addresses. Do not reject changed street names or ordinals.
            if selected.get("standardized_street"):
                result["standardized_street"] = selected["standardized_street"]
                result["normalize_status"] = "applied"
            else:
                # norm_addy may be returned without parseable street fields
                # (e.g. non-street PO boxes). Keep the original string, not
                # a Python abbreviation, and make this visible in debug.
                result["normalize_status"] = (
                    "parser_input_error" if row.get("normalize_error")
                    else "unstructured"
                )

            # Database geocoding (sales) supplies corrected locality/ZIP,
            # while direct parser results fill target locality. Never allow
            # the formatter to discard a verified ZIP or its ZIP+4.
            for field in (
                "standardized_city", "standardized_state",
                "standardized_zipcode",
            ):
                if selected.get(field) and not result.get(field):
                    result[field] = selected[field]
            for field in NORM_FIELDS:
                name = f"norm_{field}"
                if name in selected:
                    result[name] = selected[name]
            result["normalized_after_geocode"] = bool(result.get("geocode_street_pre_normalize"))
            if result["match_status"] == "input_formatted" and result.get("normalize_status") == "applied":
                result["match_status"] = "normalized_only"
                result["legacy_compatible"] = False

    metrics = {
        "zip4_retried": len(retry),
        "zip4_recovered": retry_matches,
        "zip4_retry_seconds": retry_seconds,
        "geocoded": attempted_geo,
        "matched": matched_geo,
        "parsed_from_geocoder": parsed_from_geo,
        "normalized": len(to_parse),
        "normalize_applied": sum(
            result.get("normalize_status") == "applied" for result in results
        ),
        "normalize_unstructured": sum(
            result.get("normalize_status") == "unstructured"
            for result in results
        ),
        "geocode_seconds": geocode_seconds,
        "normalize_seconds": normalize_seconds,
    }
    if os.getenv("ADDRALL_PROFILE", "").lower() in {"1", "true", "yes"}:
        print(
            f"\t\t\t\tAddrAll batch: {len(batch):,}, "
            f"geocode {geocode_seconds:.3f}s ({attempted_geo:,}), "
            f"ZIP+4 retry {retry_seconds:.3f}s ({len(retry):,} attempted, "
            f"{retry_matches:,} recovered), "
            f"normalize {normalize_seconds:.3f}s ({len(to_parse):,}), "
            # f"stored {matched_geo:,}, parsed {parsed_from_geo:,}"
        )
    return results, metrics


def _resolve_buildings(
    requests: list[tuple[str, str, str, str | None]],
    *, conn: Any, batch_size: int, use_geocode: bool,
    sources: list[str], workers: int, database_url: str | None = None,
    locality: dict[str, tuple[str, str]] | None = None,
    retry_keys: set[tuple] | None = None,
    skip_invalid_geocode_regex: bool = False,
) -> list[dict[str, Any]]:
    """Batched SQL with one warm connection per worker and ordered results."""
    if not requests:
        return []

    locality = locality if locality is not None else _load_zip_localities(conn, requests)
    batches = [
        requests[start:start + batch_size]
        for start in range(0, len(requests), batch_size)
    ]

    def lookup(batch: list[tuple]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if conn is not None:
            return _lookup_batch(
                batch, locality=locality, sources=sources,
                use_geocode=use_geocode, conn=conn,
                retry_keys=retry_keys,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
            )
        with keystone_connection(database_url) as worker_conn:
            return _lookup_batch(
                batch, locality=locality, sources=sources,
                use_geocode=use_geocode, conn=worker_conn,
                retry_keys=retry_keys,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
            )

    started = perf_counter()
    if workers == 1:
        completed = [lookup(batch) for batch in batches]
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            completed = list(executor.map(lookup, batches))

    metrics = [summary for _, summary in completed]
    elapsed = perf_counter() - started
    if os.getenv("ADDRALL_PROFILE", "").lower() in {"1", "true", "yes"}:
        print(
            f"\t\t\t\tAddrAll SQL: {len(requests):,} distinct addresses, "
            f"{len(batches)} batches, {workers} workers, elapsed {elapsed:.2f}s; "
            f"\n\t\t\t\t\tgeocode={sum(m['geocode_seconds'] for m in metrics):.2f}s "
            f"({sum(m['geocoded'] for m in metrics):,} inputs, "
            f"{sum(m['matched'] for m in metrics):,} stored, "
            f"{sum(m['parsed_from_geocoder'] for m in metrics):,} parsed); "
            f"\n\t\t\t\t\tZIP+4 retry={sum(m['zip4_retry_seconds'] for m in metrics):.2f}s "
            f"({sum(m['zip4_retried'] for m in metrics):,} attempted, "
            f"{sum(m['zip4_recovered'] for m in metrics):,} recovered); "
            f"\n\t\t\t\t\tnormalize={sum(m['normalize_seconds'] for m in metrics):.2f}s "
            f"({sum(m['normalized'] for m in metrics):,} inputs, "
            f"{sum(m['normalize_applied'] for m in metrics):,} applied, "
            f"{sum(m['normalize_unstructured'] for m in metrics):,} unstructured)"
        )
    return [record for batch, _ in completed for record in batch]


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
    raw_address2 = values(ADDRESS2)
    raw_latitude = values("latitude")
    raw_longitude = values("longitude")
    bases = [split_secondary(street) for street in raw_street]
    zip_parts = [split_zip_parts(z, ext) for z, ext in zip(raw_zip, raw_zip4)]
    keys = [
        (base.upper(), city.upper(), state.upper(), five)
        for (base, _), city, state, (five, _) in zip(
            bases, raw_city, raw_state, zip_parts
        )
    ]
    unique = list(dict.fromkeys(key for key in keys if key[0]))
    retry_keys = (
        {key for key, (_, four) in zip(keys, zip_parts) if key[0] and not four}
        if retry_missing_zip4 and use_geocode else set()
    )
    # Azure hosts may run several activities at once; do not allocate a thread
    # pool and four DB sessions per invocation unless explicitly requested.
    workers = max(1, int(os.getenv("ADDRALL_WORKERS", "1")))
    if connection is not None:
        workers = 1
    else:
        # All slots are available after the single ZIP-locality query.
        pool_size = max(1, int(os.getenv("ADDRALL_POOL_SIZE", "4")))
        workers = min(workers, pool_size)
    sources = [
        source.strip() for source in (
            geocode_sources if geocode_sources is not None
            else os.getenv("ADDRALL_GEOCODE_SOURCES", "usps,openaddresses").split(",")
        ) if source.strip()
    ]
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
                retry_keys=retry_keys,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
            )
        else:
            found = _resolve_buildings(
                unique, conn=connection, batch_size=size,
                use_geocode=use_geocode, sources=sources, workers=1,
                retry_keys=retry_keys,
                skip_invalid_geocode_regex=skip_invalid_geocode_regex,
            )
        by_key = dict(zip(unique, found))
    else:
        by_key = {}

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
        # We geocode the *building*, not the supplied apartment/suite. A
        # building-level extension must not be applied to all of its units.
        has_secondary = bool(unit or raw_address2[i])
        verified_zip4 = (
            normalize_zip4(match.get("zip4")) if not has_secondary else None
        )
        stored_zip5 = normalize_zipcode(match.get("standardized_zipcode"))
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
        coord_lat = match.get("latitude")
        coord_lon = match.get("longitude")
        if coord_lat is None or coord_lon is None:
            coord_lat = raw_latitude[i] or None
            coord_lon = raw_longitude[i] or None
            coord_source = "input" if coord_lat is not None and coord_lon is not None else None
        else:
            coord_source = match.get("geocode_source") or match.get("source")
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
                "delivery_line_2": raw_address2[i] or None,
                "last_line": last_line or None,
                "city_name": city or None,
                "state_abbreviation": state or None,
                "zipcode": zip5,
                "plus4_code": zip4,
                "full_zipcode": full_zipcode(zip5, zip4),
                "latitude": coord_lat,
                "longitude": coord_lon,
                "addrall_match_status": match.get("match_status"),
                "addrall_geocode_source": match.get("geocode_source"),
                "addrall_geocode_match": match.get("geocode_match"),
                "addrall_geocode_rating": match.get("geocode_rating"),
                "addrall_coordinate_source": coord_source,
                "addrall_zip4_source": zip4_source,
                "addrall_normalize_status": match.get("normalize_status"),
                "addrall_secondary_status": "unverified" if unit or raw_address2[i] else "absent",
            })
            continue
        result = {
            "addrall_validated": match.get("validated"),
            "addrall_match_status": match.get("match_status"),
            "addrall_legacy_key_compatible": legacy_key,
            "addrall_source": match.get("source"),
            "addrall_address_id": match.get("address_id"),
            "addrall_geocode_source": match.get("geocode_source"),
            "addrall_geocode_match": match.get("geocode_match"),
            "addrall_geocode_rating": match.get("geocode_rating"),
            "addrall_coordinate_source": coord_source,
            "addrall_unit_count": match.get("unit_count"),
            "addrall_secondary_status": "unverified" if unit or raw_address2[i] else "absent",
            "addrall_zip4_source": zip4_source,
            "addrall_input_full_zipcode": input_full_zipcode,
            "addrall_database_full_zipcode": database_full_zipcode,
            "addrall_original_street": raw_street[i],
            "addrall_original_address2": raw_address2[i],
            "addrall_original_city": raw_city[i],
            "addrall_original_state": raw_state[i],
            "addrall_original_zipcode": raw_zip[i],
            "addrall_original_zip4": raw_zip4[i],
            "addrall_building_street": std_street or None,
            "addrall_geocode_street_pre_normalize": match.get("geocode_street_pre_normalize"),
            "addrall_normalize_input_text": match.get("normalize_input_text"),
            "addrall_normalized_after_geocode": match.get("normalized_after_geocode"),
            "addrall_normalize_status": match.get("normalize_status"),
            "addrall_normalize_raw_street": match.get("normalize_raw_street"),
            "delivery_line_1": delivery_line_1 or None,
            "delivery_line_2": raw_address2[i] or None,
            "city_name": city or None,
            "state_abbreviation": state or None,
            "zipcode": zip5,
            "plus4_code": zip4,
            "full_zipcode": full_zipcode(zip5, zip4),
            "latitude": coord_lat,
            "longitude": coord_lon,
        }
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
            "addrall_address_id", "addrall_geocode_source",
            "addrall_geocode_match", "addrall_geocode_rating",
            "addrall_coordinate_source", "addrall_unit_count",
            "addrall_secondary_status", "addrall_zip4_source",
            "addrall_input_full_zipcode", "addrall_database_full_zipcode",
            "addrall_original_street", "addrall_original_address2",
            "addrall_original_city", "addrall_original_state",
            "addrall_original_zipcode", "addrall_original_zip4",
            "addrall_building_street",
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
