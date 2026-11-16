"""On-demand coordinates for audience spatial operations.

This does not call AddrAll autocomplete/normalization and never modifies
canonical address IDs. It geocodes the already-resolved address text only.
"""
from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any, Callable, Mapping, Sequence

# Current addresses.geocode_address result columns are:
# input_index, input_text, input_addy, addy, geomout, rating.
# There is NO source or match metadata. geomout is a point in EPSG:4326.
_GEOCODE_SQL = """
SELECT g.input_index,
       g.rating,
       ST_Y(g.geomout) AS latitude,
       ST_X(g.geomout) AS longitude
FROM addresses.geocode_address(%s::text[]) AS g
"""

COORD_COLUMNS = ("latitude", "longitude", "latlon_status", "latlon_rating")
_UNIT_SUFFIX = re.compile(
    r"(?:^|\s)(?:APT|APARTMENT|STE|SUITE|UNIT|RM|ROOM|LOT|FL|FLOOR|#)"
    r"\s*#?\s*[A-Z0-9-]+\s*$", re.I,
)


def valid_coordinates(lat: Any, lon: Any) -> bool:
    """Validate finite WGS84 coordinate pairs, excluding the (0,0) sentinel."""
    try:
        y, x = float(lat), float(lon)
    except (TypeError, ValueError):
        return False
    return (math.isfinite(y) and math.isfinite(x)
            and -90 <= y <= 90 and -180 <= x <= 180
            and (y, x) != (0.0, 0.0))


def _first(record: Mapping[str, Any], *fields: str) -> str:
    for field in fields:
        value = record.get(field)
        if value is not None and str(value).strip().lower() not in {"", "nan", "none", "null"}:
            return str(value).strip()
    return ""


def build_geocode_text(record: Mapping[str, Any]) -> str:
    """Construct a query from existing resolved fields; do not re-normalize."""
    line = _first(record, "delivery_line_1", "street", "street_address",
                  "address_line_1", "address1", "add1")
    if not line:
        number = _first(record, "address_alphanumeric", "primary_number", "street_number", "address")
        street = _first(record, "streetName", "street_name")
        if street:
            line = " ".join(part for part in (
                number, _first(record, "predirAbbrev", "street_predirection"),
                street, _first(record, "streetTypeAbbrev", "street_suffix"),
                _first(record, "postdirAbbrev", "street_postdirection"),
            ) if part)
        else:
            # Older sales CSVs use 'address' for the whole street line.
            line = _first(record, "address")
    if not line or not any(ch.isalpha() for ch in line):
        return ""

    unit = _first(record, "delivery_line_2", "address_line_2", "address2", "add2", "internal")
    # Completed AddrAll delivery_line_1 can already include a unit, including
    # one written with different punctuation/abbreviations than address2.
    if unit and not _UNIT_SUFFIX.search(line) and not line.upper().endswith(unit.upper()):
        line = f"{line} {unit}"

    city = _first(record, "city_name", "city", "location")
    state = _first(record, "state_abbreviation", "state", "stateAbbrev", "st")
    zipcode = _first(record, "zipcode", "zipCode", "zip", "full_zipcode")
    return ", ".join(part for part in (line, city, state, zipcode) if part)


def query_geocodes(texts: Sequence[str]) -> list[dict[str, Any]]:
    """Run the separate batch geocoder; never affect address canonicalization."""
    from libs.utils.addrall import keystone_connection

    if not texts:
        return []
    with keystone_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(_GEOCODE_SQL, (list(texts),))
            fields = [getattr(c, "name", c[0] if isinstance(c, tuple) else None)
                      for c in cursor.description]
            return [dict(zip(fields, values)) for values in cursor.fetchall()]


def enrich_coordinate_rows(
    rows: list[dict[str, Any]], *,
    batch_size: int = 50,
    lookup: Callable[[Sequence[str]], list[dict[str, Any]]] = query_geocodes,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Fill missing coordinates only; preserve row order, IDs, and all source fields.

    'geocoded' means a valid coordinate was returned. It does NOT imply rooftop
    precision, because the geocoder no longer returns source/match metadata.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    out: list[dict[str, Any]] = []
    pending: dict[str, list[dict[str, Any]]] = {}

    for original in rows:
        row = dict(original)
        for col in COORD_COLUMNS:
            row.setdefault(col, "")
        if valid_coordinates(row.get("latitude"), row.get("longitude")):
            row["latlon_status"] = "existing"
        else:
            row["latitude"] = ""
            row["longitude"] = ""
            row["latlon_rating"] = ""
            query = build_geocode_text(row)
            if query:
                pending.setdefault(query, []).append(row)
            else:
                row["latlon_status"] = "missing_address"
        out.append(row)

    unique = list(pending)
    for start in range(0, len(unique), batch_size):
        batch = unique[start:start + batch_size]
        results = lookup(batch)
        by_index: dict[int, Mapping[str, Any]] = {}
        for result in results:
            index = int(result["input_index"])
            if not (1 <= index <= len(batch)) or index in by_index:
                raise ValueError(f"Geocoder returned invalid or duplicate input_index {index}")
            by_index[index] = result

        for index, query in enumerate(batch, 1):
            found = by_index.get(index)
            lat = found.get("latitude") if found else None
            lon = found.get("longitude") if found else None
            if found is None:
                status = "no_result"
            elif not valid_coordinates(lat, lon):
                status = "invalid_result"
            else:
                status = "geocoded"
            for row in pending[query]:
                row["latlon_status"] = status
                row["latlon_rating"] = (
                    "" if found is None or found.get("rating") is None
                    else str(found["rating"])
                )
                if status == "geocoded":
                    row["latitude"] = str(lat)
                    row["longitude"] = str(lon)

    return out, dict(Counter(row["latlon_status"] for row in out))
