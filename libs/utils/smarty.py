"""Compatibility boundary for legacy Smarty consumers; powered by AddrAll.

This module enriches addresses. `dpv_match_code` and other USPS/Smarty-only
fields are intentionally null; AddrAll store membership is not deliverability.
Use `libs.utils.addrall.clean_addresses` for new callers.
"""
from __future__ import annotations

import re
from typing import Any, Sequence

import pandas as pd

from libs.utils.addrall import clean_addresses, as_text


# Flattened columns consumed by legacy Smarty callers.
SMARTY_FIELDS = (
    "input_index", "candidate_index", "delivery_line_1", "delivery_line_2",
    "last_line", "delivery_point_barcode", "smarty_key", "smarty_key_ext",
    "primary_number", "street_predirection", "street_name", "street_suffix",
    "street_postdirection", "secondary_number", "secondary_designator",
    "city_name", "default_city_name", "state_abbreviation", "zipcode",
    "plus4_code", "full_zipcode", "delivery_point", "delivery_point_check_digit",
    "record_type", "zip_type", "county_fips", "county_name", "carrier_route",
    "congressional_district", "rdi", "elot_sequence", "elot_sort",
    "latitude", "longitude", "precision", "time_zone", "utc_offset", "dst",
    "dpv_match_code", "dpv_footnotes", "dpv_cmra", "dpv_vacant",
    "dpv_no_stat", "active", "footnotes", "enhanced_match",
)

# Avoid calling an unstructured street a confidently parsed street number.
_NUMBER_STREET = re.compile(
    r"^\s*((?:\d+[A-Z0-9-]*|[A-Z]+\d+[A-Z0-9-]*))\s+(.+?)\s*$",
    flags=re.I,
)
_UNIT_END = re.compile(
    r"(?<!\w)(?P<designator>APARTMENT|APT|SUITE|STE|UNIT|ROOM|RM|LOT|#)\s*#?\s*"
    r"(?P<number>[A-Z0-9]+(?:-[A-Z0-9]+)*)\s*$", re.I,
)


def _smarty_components(row: pd.Series) -> dict[str, Any]:
    """Project AddrAll's canonical parts, with conservative input fallback."""
    norm_number = as_text(row.get("addrall_norm_address_alphanumeric")) or as_text(
        row.get("addrall_norm_address")
    )
    norm_name = as_text(row.get("addrall_norm_street_name"))
    input_street = as_text(row.get("addrall_original_street"))
    unit = _UNIT_END.search(input_street) or _UNIT_END.search(
        as_text(row.get("addrall_original_address2"))
    )
    inline_unit = _UNIT_END.search(input_street)
    line_without_unit = input_street[:inline_unit.start()].strip() if inline_unit else input_street
    raw_parts = _NUMBER_STREET.match(line_without_unit)
    number = norm_number or as_text(row.get("primary_number")) or (
        raw_parts.group(1) if raw_parts else None
    )
    street_name = norm_name or as_text(row.get("street_name")) or (
        raw_parts.group(2) if raw_parts else None
    )
    secondary = unit.group("number") if unit else None
    secondary_type = unit.group("designator").upper() if unit else None
    secondary_type = {"APARTMENT": "APT", "SUITE": "STE", "ROOM": "RM"}.get(
        secondary_type, secondary_type
    )
    return {
        "primary_number": number or None,
        "street_name": street_name or None,
        "street_predirection": (as_text(row.get("addrall_norm_predir_abbrev"))
                                or as_text(row.get("street_predirection")) or None),
        "street_suffix": (as_text(row.get("addrall_norm_street_type_abbrev"))
                          or as_text(row.get("street_suffix")) or None),
        "street_postdirection": (as_text(row.get("addrall_norm_postdir_abbrev"))
                                 or as_text(row.get("street_postdirection")) or None),
        "secondary_number": secondary or as_text(row.get("secondary_number")) or None,
        "secondary_designator": (secondary_type
                                 or as_text(row.get("secondary_designator")) or None),
    }


def bulk_validate(
    df: pd.DataFrame,
    address_col: str,
    addr2_col: str | None = None,
    city_col: str | None = None,
    state_col: str | None = None,
    zip_col: str | None = None,
    *,
    zip4_col: str | None = None,
    connection: Any = None,
    database_url: str | None = None,
    batch_size: int | None = None,
    candidate_limit: int = 100,
    geocode_sources: Sequence[str] | None = None,
    use_geocode: bool = True,
    retry_missing_zip4: bool = True,
    drop_address: bool = False,
) -> pd.DataFrame:
    """Fill and normalize all rows while retaining unresolved/original addresses.

    `candidate_limit` is kept for old call signatures but has no effect:
    the AddrAll engine uses batched geocoding, not autocomplete.
    """
    if not 1 <= candidate_limit <= 100:
        raise ValueError("candidate_limit must be 1..100")

    result = clean_addresses(
        df, ADDRESS=address_col, CITY=city_col, STATE=state_col,
        ZIPCODE=zip_col, ADDRESS2=addr2_col, ZIP4=zip4_col,
        connection=connection, database_url=database_url,
        batch_size=batch_size, use_geocode=use_geocode,
        retry_missing_zip4=retry_missing_zip4,
        geocode_sources=geocode_sources,
    )

    parts = [_smarty_components(row) for _, row in result.iterrows()]
    for name in ("primary_number", "street_name", "street_predirection",
                 "street_suffix", "street_postdirection", "secondary_number",
                 "secondary_designator"):
        result[name] = [row[name] for row in parts]

    last_line = []
    for city, state, code, full_code in zip(
        result["city_name"], result["state_abbreviation"],
        result["zipcode"], result["full_zipcode"],
    ):
        value = " ".join(x for x in (
            as_text(city), as_text(state), as_text(full_code) or as_text(code)
        ) if x)
        # The caller may already carry a useful last_line that cannot be
        # reconstructed from the supplied component columns.
        existing = result["last_line"].iloc[len(last_line)] if "last_line" in result else None
        if all(as_text(part) for part in (city, state, code)):
            last_line.append(value or None)
        else:
            last_line.append(as_text(existing) or value or None)
    result["last_line"] = last_line
    result["input_index"] = list(range(len(result)))
    result["candidate_index"] = [
        0 if as_text(line) else None for line in result["delivery_line_1"]
    ]

    # USPS-specific signals cannot be reconstructed from parser/store hits.
    for name in SMARTY_FIELDS:
        if name not in result:
            result[name] = None

    if drop_address:
        result = result.drop(columns=[address_col])
    return result


def detect_column_names(
    cols: Sequence[str], override_mappings: dict[str, str] | None = None,
) -> dict[str, str | None]:
    """Map present address columns without guessing unavailable locality roles."""
    aliases = {
        "street": ("address", "street", "streetaddress", "deliveryline1",
                   "add1", "address1", "street1", "addressline1"),
        "addr2": ("address2", "addressline2", "street2", "add2", "unit", "suite"),
        "city": ("city", "cityname", "municipality", "location"),
        "state": ("state", "stateabbreviation", "stateabbrev", "st"),
        "zip": ("zip", "zipcode", "postalcode", "postal", "zip5", "fullzipcode"),
        "zip4": ("zip4", "plus4", "plus4code", "zipcode4"),
    }
    normalized = {str(col): re.sub(r"[^a-z0-9]", "", str(col).lower()) for col in cols}
    mappings: dict[str, str | None] = {}
    overrides = override_mappings or {}
    used: set[str] = set()
    for role, names in aliases.items():
        if role in overrides:
            column = overrides[role]
            if column not in normalized:
                raise KeyError(f"Unknown column_mapping.{role}: {column!r}")
            mappings[role] = column
            used.add(column)
            continue
        match = next((col for key in names for col, normalized_name in normalized.items()
                      if col not in used and normalized_name == key), None)
        if match is None:
            match = next((col for key in names for col, normalized_name in normalized.items()
                          if col not in used and len(key) >= 4 and
                          normalized_name.endswith(key) and not normalized_name.endswith("id")), None)
        mappings[role] = match
        if match:
            used.add(match)
    if mappings["street"] is None:
        raise ValueError("No street address field; supply column_mapping.street")
    return mappings


def get_items_recursive(obj: Any, dict: dict[str, Any] | None = None) -> dict[str, Any]:
    """Legacy object flattener maintained for callers outside the provided files."""
    result = {} if dict is None else dict
    for key, value in vars(obj).items():
        if key.startswith("__"):
            continue
        if hasattr(value, "__dict__"):
            get_items_recursive(value, result)
        else:
            result[key] = value
    return result