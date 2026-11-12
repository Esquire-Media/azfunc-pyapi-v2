"""Geocoder-authoritative sales address IDs; Smarty EAV plus norm_addy JSONB.

Ingestion and historical backfill hash the exact same canonical structure
returned by addresses.geocode_address, not local parser/source corrections.
"""
from __future__ import annotations

import json
import re
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

import pandas as pd

from libs.utils.addrall import (
    as_text,
    clean_addresses,
    normalize_zip4,
    normalize_zipcode,
    full_zipcode,
    split_secondary,
)

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.generate_ids import (
    NAMESPACE_ADDRESS,
)

ADDRESS_TYPE_ID = uuid.UUID('fe694dd2-2dc4-452f-910c-7023438bb0ac')
ADDRESS_ID_VERSION = 'geocoder_norm_addy_v2'

# Exact PostGIS norm_addy JSON keys; these are NOT separate EAV attributes.
NORM_ADDY_TYPES = {
    'address': 'numeric',
    'predirAbbrev': 'string',
    'streetName': 'string',
    'streetTypeAbbrev': 'string',
    'postdirAbbrev': 'string',
    'internal': 'string',
    'location': 'string',
    'stateAbbrev': 'string',
    'zip': 'string',
    'zip4': 'string',
    'address_alphanumeric': 'string',
}

# Preserve the existing EAV query contract for sales.fn_find_matching_addresses
# and other Smarty-based consumers. These are individual string EAV values.
# The structured address is one JSONB EAV attribute named 'norm_addy'.
SMARTY_FIELDS = (
    'delivery_line_1', 'delivery_line_2', 'city_name',
    'state_abbreviation', 'zipcode', 'plus4_code', 'full_zipcode',
    'latitude', 'longitude', 'last_line', 'primary_number', 'street_name',
    'street_predirection', 'street_suffix', 'street_postdirection',
    'hashed_address_id',
)
STORED_ADDRESS_TYPES = {**{name: 'string' for name in SMARTY_FIELDS},
                        'norm_addy': 'jsonb'}


# Hash every address component returned by the geocoder, in a fixed JSON
# structure. No source fallbacks, street alias folding or ZIP+4 exclusion.
ID_FIELDS = tuple(NORM_ADDY_TYPES)

_SECONDARY_WHITESPACE = re.compile(r'\s+')
_UNIT_ALIASES = re.compile(r'^(?:APARTMENT|APT\.?|UNIT|#)\s*#?\s*', re.I)
_SUITE_ALIASES = re.compile(r'^(?:SUITE|STE\.?)\s*#?\s*', re.I)


def _value(value: Any) -> str | None:
    text = as_text(value)
    return text.upper() if text else None


def _unit_key(value: Any) -> str:
    text = _SECONDARY_WHITESPACE.sub(' ', as_text(value).upper()).strip()
    text = _UNIT_ALIASES.sub('APT ', text)
    text = _SUITE_ALIASES.sub('STE ', text)
    return text.strip()


_UNUSABLE_STREETS = frozenset({
    '-', '--', '---', '.', '...', '?', '???', '/', '\\',
    '\u2014', '\u2013', '\ufffd', '\u00e2\u20ac\u201d',
    '\u00e2\u20ac\u201c', 'N/A', 'NA', 'NULL', 'NONE',
})


def _usable_component(value: Any) -> str | None:
    """Suppress known empty/garbled placeholders in derived address fields."""
    text = _value(value)
    return (text if text and text not in _UNUSABLE_STREETS
            and re.search(r'[A-Z0-9]', text, flags=re.ASCII) else None)


def _geocoder_house_number(value: Any) -> int | float | None:
    """Serialize the numeric PostGIS field as a JSON number, not a string."""
    raw = as_text(value)
    if not raw:
        return None
    try:
        number = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f'Unexpected geocoder numeric house number: {raw!r}') from exc
    if not number.is_finite():
        raise ValueError(f'Non-finite geocoder house number: {raw!r}')
    if number == number.to_integral_value():
        return int(number)
    rendered = float(number)
    if Decimal(str(rendered)) != number:
        raise ValueError(f'Geocoder house number loses JSON precision: {raw!r}')
    return rendered


def norm_addy_from_enriched(enriched: Mapping[str, Any], _raw: Mapping[str, Any]) -> dict[str, Any]:
    """Use ONLY the actual geocoder-returned norm_addy structure.

    The full AddrAll output also contains a post-geocode formatter and source
    fallbacks. Those may help display an unrecognized input but must never
    change canonical IDs or masquerade as a geocoder result.
    """
    returned = enriched.get('addrall_geocoder_norm_addy')
    if not isinstance(returned, Mapping):
        return {field: None for field in NORM_ADDY_TYPES}
    # The provider owns spelling/casing/spacing canonicalization. Preserve
    # its returned text exactly rather than introducing local equivalences.
    result = {
        field: (_geocoder_house_number(returned.get(field)) if field == 'address'
                else (str(returned[field]) if returned.get(field) is not None else None))
        for field in NORM_ADDY_TYPES
    }
    return result


def canonical_address_id(address: Mapping[str, Any]) -> str | None:
    """Hash the returned address components without equivalence heuristics.

    Different geocoder-returned structures intentionally get different UUIDs.
    Only an empty/unidentifiable result is refused. All keys, including null
    positions and ZIP+4, participate; `parsed` is not an address attribute.
    """
    if not _usable_component(address.get('streetName')):
        # The existing migration audits and address readers require an
        # identifiable street component. Do not invent one from raw input.
        return None
    payload = {
        field: address.get(field)
        for field in ID_FIELDS
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                         separators=(',', ':'), allow_nan=False)
    return str(uuid.uuid5(NAMESPACE_ADDRESS, ADDRESS_ID_VERSION + '|' + encoded))


def enrich_norm_addy(
    source: pd.DataFrame,
    *,
    street_col: str | None = 'street',
    addr2_col: str | None = 'addr2',
    city_col: str | None = 'city',
    state_col: str | None = 'state',
    zip_col: str | None = 'zipcode',
    zip4_col: str | None = None,
    batch_size: int = 200,
) -> list[dict[str, Any]]:
    """Enrich one bounded batch and return canonical fields, ID, and Smarty values.

    Does not copy the client's full DataFrame.
    SQL errors propagate, allowing Durable retries. Empty/invalid rows remain
    represented with address_id=None instead of a shared empty-address UUID.
    """
    def source_values(col: str | None) -> list[Any]:
        return source[col].tolist() if col and col in source else [None] * len(source)

    inputs = pd.DataFrame({
        'street': source_values(street_col),
        'addr2': source_values(addr2_col),
        'city': source_values(city_col),
        'state': source_values(state_col),
        'zipcode': source_values(zip_col),
        'zip4': source_values(zip4_col),
        'latitude': source_values('latitude'),
        'longitude': source_values('longitude'),
    })
    if source.empty:
        return []
    for name in ('primary_number', 'street_name', 'street_suffix',
                 'street_predirection', 'street_postdirection'):
        if name in source:
            inputs[name] = source[name].tolist()
    cleaned = clean_addresses(
        inputs, 'street', 'city', 'state', 'zipcode',
        ADDRESS2='addr2', ZIP4='zip4',
        batch_size=batch_size, keep_invalid=True, use_geocode=True,
        retry_missing_zip4=True, output_mode='full',
        canonical_geocoder_output=True,
    )
    if len(cleaned) != len(source):
        raise ValueError('AddrAll changed the number of address rows')
    records = []
    for raw, enriched in zip(inputs.to_dict('records'), cleaned.to_dict('records')):
        norm = norm_addy_from_enriched(enriched, raw)
        norm['address_id'] = canonical_address_id(norm)
        norm['_smarty'] = smarty_from_enriched(enriched, norm)
        records.append(norm)
    return records


def smarty_from_enriched(enriched: Mapping[str, Any], norm: Mapping[str, Any]) -> dict[str, str | None]:
    """Project AddrAll's real Smarty-compatible columns, not parser telemetry.

    Address filters must use the same canonical locality/ZIP as the stored
    norm_addy. A source-only ZIP+4 cannot be attached to a changed ZIP5.
    """
    postal_zip = normalize_zipcode(norm.get('zip'))
    postal_zip4 = normalize_zip4(enriched.get('plus4_code')) or normalize_zip4(norm.get('zip4'))
    primary = _value(norm.get('address_alphanumeric')) or (
        str(norm['address']) if norm.get('address') is not None else None
    )
    street_parts = (
        primary, _value(norm.get('predirAbbrev')), _value(norm.get('streetName')),
        _value(norm.get('streetTypeAbbrev')), _value(norm.get('postdirAbbrev')),
    )
    fallback_line = ' '.join(part for part in street_parts if part)
    if fallback_line and norm.get('internal'):
        fallback_line += ' ' + str(norm['internal'])
    line1 = _usable_component(enriched.get('delivery_line_1'))
    if line1 and norm.get('internal'):
        original_building, inline_unit = split_secondary(line1)
        if inline_unit and _unit_key(inline_unit) != _unit_key(norm['internal']):
            # The raw delivery line can contain an apartment that conflicts
            # with an explicit addr2. Do not persist the contradictory unit
            # under an ID canonicalized from the explicit secondary.
            line1 = ' '.join((original_building, str(norm['internal'])))
    return {
        'delivery_line_1': line1 or _usable_component(fallback_line),
        'delivery_line_2': _usable_component(enriched.get('delivery_line_2')),
        'city_name': _usable_component(norm.get('location')),
        'state_abbreviation': _usable_component(norm.get('stateAbbrev')),
        'zipcode': postal_zip,
        'plus4_code': postal_zip4,
        'full_zipcode': full_zipcode(postal_zip, postal_zip4),
        'latitude': _value(enriched.get('latitude')),
        'longitude': _value(enriched.get('longitude')),
        'last_line': ' '.join(part for part in (
            _usable_component(norm.get('location')),
            _usable_component(norm.get('stateAbbrev')),
            full_zipcode(postal_zip, postal_zip4) or postal_zip,
        ) if part) or None,
        'primary_number': primary,
        'street_name': _usable_component(norm.get('streetName')),
        'street_predirection': _usable_component(norm.get('predirAbbrev')),
        'street_suffix': _usable_component(norm.get('streetTypeAbbrev')),
        'street_postdirection': _usable_component(norm.get('postdirAbbrev')),
        'hashed_address_id': None,  # filled after canonical UUID assignment
    }


def ensure_norm_addy_attributes(conn: Any) -> dict[str, str]:
    """Require the Smarty + norm_addy JSONB attributes from the bootstrap SQL."""
    from sqlalchemy import text

    found = conn.execute(text("""
        SELECT id::text AS id, name, data_type::text AS dtype
        FROM sales.attributes
        WHERE entity_type_id = CAST(:etype AS uuid)
          AND name = ANY(:names)
    """), {'etype': str(ADDRESS_TYPE_ID), 'names': list(STORED_ADDRESS_TYPES)}).mappings()
    mapped = {row['name']: row['id'] for row in found
              if STORED_ADDRESS_TYPES.get(row['name']) == row['dtype']}
    if set(mapped) != set(STORED_ADDRESS_TYPES):
        raise RuntimeError(
            'Apply sales Smarty + norm_addy JSONB schema bootstrap first; '
            f'missing attributes: {set(STORED_ADDRESS_TYPES) - set(mapped)}'
        )
    return mapped


def upsert_norm_addy_records(conn: Any, records: list[dict[str, Any]]) -> int:
    """Create/reuse canonical address IDs; upsert Smarty values + norm_addy JSONB.

    Called inside the caller's DB transaction. Existing non-null fields remain
    available when another source supplies only a partial address.
    """
    from sqlalchemy import bindparam, text
    from sqlalchemy.dialects.postgresql import JSONB

    canonical: dict[str, dict[str, Any]] = {}
    for row in records:
        aid = row.get('address_id')
        if not aid:
            continue
        aid = str(aid)
        if aid not in canonical:
            canonical[aid] = dict(row)
            canonical[aid]['_smarty'] = dict(row.get('_smarty') or {})
            continue
        retained = canonical[aid]
        for name in NORM_ADDY_TYPES:
            if retained.get(name) in (None, '') and row.get(name) not in (None, ''):
                retained[name] = row[name]
        stored_smarty = retained['_smarty']
        for name, value in (row.get('_smarty') or {}).items():
            if stored_smarty.get(name) in (None, '') and value not in (None, ''):
                stored_smarty[name] = value
    if not canonical:
        return 0

    attrs = ensure_norm_addy_attributes(conn)
    conn.execute(text("""
        INSERT INTO sales.entities (id, entity_type_id, parent_entity_id)
        VALUES (CAST(:id AS uuid), CAST(:etype AS uuid), NULL)
        ON CONFLICT (id) DO NOTHING
    """), [{'id': aid, 'etype': str(ADDRESS_TYPE_ID)} for aid in canonical])
    owners = conn.execute(text("""
        SELECT id::text AS id, entity_type_id::text AS entity_type_id
        FROM sales.entities WHERE id = ANY(CAST(:ids AS uuid[]))
    """), {'ids': list(canonical)}).mappings().all()
    if len(owners) != len(canonical) or any(
        owner['entity_type_id'] != str(ADDRESS_TYPE_ID) for owner in owners
    ):
        raise RuntimeError('Canonical address UUID already belongs to another entity type')

    eav_rows = []
    for aid, row in canonical.items():
        # Use only the 11 canonical address components, never parser status.
        payload = {key: val for key in NORM_ADDY_TYPES
                   if (val := row.get(key)) is not None and val != ''}
        if not payload.get('streetName'):
            raise RuntimeError('Geocoder canonical address has no identifiable street')
        eav_rows.append({
            'entity_id': aid, 'attribute_id': attrs['norm_addy'],
            'value_string': None, 'value_jsonb': payload,
        })
        smarty = row.get('_smarty') or {}
        for field in SMARTY_FIELDS:
            value = aid.upper() if field == 'hashed_address_id' else smarty.get(field)
            if value is None or value == '':
                continue
            eav_rows.append({
                'entity_id': aid, 'attribute_id': attrs[field],
                'value_string': str(value), 'value_jsonb': None,
            })

    if eav_rows:
        stmt = text("""
            INSERT INTO sales.entity_attribute_values
                (entity_id, attribute_id, value_string, value_jsonb)
            VALUES (CAST(:entity_id AS uuid), CAST(:attribute_id AS uuid),
                    :value_string, :value_jsonb)
            ON CONFLICT (entity_id, attribute_id) DO UPDATE SET
                value_string = COALESCE(EXCLUDED.value_string,
                                        sales.entity_attribute_values.value_string),
                value_jsonb = CASE WHEN EXCLUDED.value_jsonb IS NOT NULL THEN
                    COALESCE(sales.entity_attribute_values.value_jsonb, '{}'::jsonb)
                    || EXCLUDED.value_jsonb
                    ELSE sales.entity_attribute_values.value_jsonb END
        """).bindparams(bindparam('value_jsonb', type_=JSONB()))
        conn.execute(stmt, eav_rows)
    return len(canonical)

"""Shared sales address IDs; Smarty EAV plus norm_addy JSONB.

Identity follows AddrAll's single enrichment path: autocomplete normalized_address
when completed, otherwise addresses.normalize -> addresses.standardize. Ingestion,
preview and backfill must all use this helper, not separate ID policies.
"""
from __future__ import annotations

import json
import re
import uuid
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

import pandas as pd

from libs.utils.addrall import (
    NORM_FIELDS,
    as_text,
    clean_addresses,
    normalize_zip4,
    normalize_zipcode,
    full_zipcode,
    split_secondary,
)

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.generate_ids import (
    NAMESPACE_ADDRESS,
)

ADDRESS_TYPE_ID = uuid.UUID('fe694dd2-2dc4-452f-910c-7023438bb0ac')
ADDRESS_ID_VERSION = 'geocoder_norm_addy_v2'

# Exact PostGIS norm_addy JSON keys; these are NOT separate EAV attributes.
NORM_ADDY_TYPES = {
    'address': 'numeric',
    'predirAbbrev': 'string',
    'streetName': 'string',
    'streetTypeAbbrev': 'string',
    'postdirAbbrev': 'string',
    'internal': 'string',
    'location': 'string',
    'stateAbbrev': 'string',
    'zip': 'string',
    'zip4': 'string',
    'address_alphanumeric': 'string',
}

# Preserve the existing EAV query contract for sales.fn_find_matching_addresses
# and other Smarty-based consumers. These are individual string EAV values.
# The structured address is one JSONB EAV attribute named 'norm_addy'.
SMARTY_FIELDS = (
    'delivery_line_1', 'delivery_line_2', 'city_name',
    'state_abbreviation', 'zipcode', 'plus4_code', 'full_zipcode',
    'latitude', 'longitude', 'last_line', 'primary_number', 'street_name',
    'street_predirection', 'street_suffix', 'street_postdirection',
    'hashed_address_id',
)
STORED_ADDRESS_TYPES = {**{name: 'string' for name in SMARTY_FIELDS},
                        'norm_addy': 'jsonb'}


# Keep the existing UUID seed for identical 11-field norm_addy results.
# The provider changed from geocode to autocomplete; the structured identity
# format did not. A change in the actual normalized components changes the ID.
ID_FIELDS = tuple(NORM_ADDY_TYPES)

_SECONDARY_WHITESPACE = re.compile(r'\s+')
_UNIT_ALIASES = re.compile(r'^(?:APARTMENT|APT\.?|UNIT|#)\s*#?\s*', re.I)
_SUITE_ALIASES = re.compile(r'^(?:SUITE|STE\.?)\s*#?\s*', re.I)


def _value(value: Any) -> str | None:
    text = as_text(value)
    return text.upper() if text else None


def _unit_key(value: Any) -> str:
    text = _SECONDARY_WHITESPACE.sub(' ', as_text(value).upper()).strip()
    text = _UNIT_ALIASES.sub('APT ', text)
    text = _SUITE_ALIASES.sub('STE ', text)
    return text.strip()


_UNUSABLE_STREETS = frozenset({
    '-', '--', '---', '.', '...', '?', '???', '/', '\\',
    '\u2014', '\u2013', '\ufffd', '\u00e2\u20ac\u201d',
    '\u00e2\u20ac\u201c', 'N/A', 'NA', 'NULL', 'NONE',
})


def _usable_component(value: Any) -> str | None:
    """Suppress known empty/garbled placeholders in derived address fields."""
    text = _value(value)
    return (text if text and text not in _UNUSABLE_STREETS
            and re.search(r'[A-Z0-9]', text, flags=re.ASCII) else None)


def _normalized_house_number(value: Any) -> int | float | None:
    """Serialize the numeric PostGIS field as a JSON number, not a string."""
    raw = as_text(value)
    if not raw:
        return None
    try:
        number = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f'Unexpected AddrAll numeric house number: {raw!r}') from exc
    if not number.is_finite():
        raise ValueError(f'Non-finite AddrAll house number: {raw!r}')
    if number == number.to_integral_value():
        return int(number)
    rendered = float(number)
    if Decimal(str(rendered)) != number:
        raise ValueError(f'AddrAll house number loses JSON precision: {raw!r}')
    return rendered


# Use the normalized-address projection emitted by the installed AddrAll.
# The address source (autocomplete vs. normalize/standardize) is decided inside
# clean_addresses(), not here. PostGIS JSON keys and AddrAll column aliases
# differ only by their documented camelCase/snake_case spellings.
_POSTGIS_TO_ADDRALL = {
    'address': 'address',
    'predirAbbrev': 'predir_abbrev',
    'streetName': 'street_name',
    'streetTypeAbbrev': 'street_type_abbrev',
    'postdirAbbrev': 'postdir_abbrev',
    'internal': 'internal',
    'location': 'location',
    'stateAbbrev': 'state_abbrev',
    'zip': 'zip',
    'zip4': 'zip4',
    'address_alphanumeric': 'address_alphanumeric',
}
if tuple(_POSTGIS_TO_ADDRALL[field] for field in NORM_ADDY_TYPES) != tuple(
    field for field in NORM_FIELDS if field != 'parsed'
):
    raise RuntimeError('AddrAll norm_addy fields differ from the sales ID contract')
_NORMALIZED_FIELDS = {
    field: f'addrall_norm_{alias}'
    for field, alias in _POSTGIS_TO_ADDRALL.items()
}


def _norm_values(values: Mapping[str, Any]) -> dict[str, Any]:
    """Keep the provider's components, without address-equivalence heuristics."""
    return {
        field: (_normalized_house_number(values.get(field)) if field == 'address'
                else (str(values[field]) if values.get(field) is not None else None))
        for field in NORM_ADDY_TYPES
    }


def norm_addy_from_enriched(enriched: Mapping[str, Any], _raw: Mapping[str, Any]) -> dict[str, Any]:
    """Project the FINAL normalized composite selected by AddrAll.

    Both autocomplete hits and normalize/standardize misses expose the same
    `addrall_norm_*` columns. Never rerun the database functions here. For a
    completed address, those columns alone define identity; source-address
    and display fallbacks must not override an autocomplete result.
    """
    normalized = _norm_values({
        field: enriched.get(alias) for field, alias in _NORMALIZED_FIELDS.items()
    })
    if enriched.get('addrall_match_status') == 'completed':
        if not any(normalized.get(field) not in (None, '') for field in ID_FIELDS):
            raise ValueError('AddrAll completed an address without normalized_address')
        return normalized

    # AddrAll itself fills the Smarty-compatible columns from its standardized
    # output, followed by raw input only where standardization left gaps.
    # Reuse those existing results for incomplete/unmatched inputs.
    if not normalized['internal']:
        normalized['internal'] = _usable_component(enriched.get('delivery_line_2'))
    if not normalized['streetName'] and enriched.get('delivery_line_1'):
        normalized['streetName'] = _usable_component(enriched['delivery_line_1'])
    for key, column in (('location', 'city_name'),
                        ('stateAbbrev', 'state_abbreviation'),
                        ('zip', 'zipcode'), ('zip4', 'plus4_code')):
        if normalized[key] is None and enriched.get(column) is not None:
            normalized[key] = as_text(enriched[column]) or None
    return normalized


def canonical_address_id(address: Mapping[str, Any]) -> str | None:
    """UUIDv5 over the 11 final AddrAll components (not the source address).

    `NAMESPACE_ADDRESS` is the original 30000000-... UUID namespace. Do not
    overwrite any UUID bytes to enforce a visible prefix. An autocomplete miss
    with usable standardization data is still a valid address identity.
    """
    if not any(address.get(field) not in (None, '') for field in ID_FIELDS):
        # Truly empty input contains no identity information to hash.
        return None
    payload = {
        field: address.get(field)
        for field in ID_FIELDS
    }
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False,
                         separators=(',', ':'), allow_nan=False)
    return str(uuid.uuid5(NAMESPACE_ADDRESS, ADDRESS_ID_VERSION + '|' + encoded))


def enrich_norm_addy(
    source: pd.DataFrame,
    *,
    street_col: str | None = 'street',
    addr2_col: str | None = 'addr2',
    city_col: str | None = 'city',
    state_col: str | None = 'state',
    zip_col: str | None = 'zipcode',
    zip4_col: str | None = None,
    batch_size: int = 200,
    connection: Any = None,
) -> list[dict[str, Any]]:
    """Use the production AddrAll path for every incoming or historical row.

    Autocomplete and normalize/standardize fallback live inside clean_addresses().
    Sales only projects its returned structure and creates deterministic UUIDs.
    The migration and preview call this same function.
    """
    def source_values(col: str | None) -> list[Any]:
        return source[col].tolist() if col and col in source else [None] * len(source)

    inputs = pd.DataFrame({
        'street': source_values(street_col),
        'addr2': source_values(addr2_col),
        'city': source_values(city_col),
        'state': source_values(state_col),
        'zipcode': source_values(zip_col),
        'zip4': source_values(zip4_col),
        'latitude': source_values('latitude'),
        'longitude': source_values('longitude'),
    })
    if source.empty:
        return []
    for name in ('primary_number', 'street_name', 'street_suffix',
                 'street_predirection', 'street_postdirection'):
        if name in source:
            inputs[name] = source[name].tolist()
    # The Azure project uses psycopg3 via SQLAlchemy. Supply its DBAPI
    # connection instead of opening AddrAll's matchback-specific psycopg2 pool.
    # A preview supplies its own read-only DBAPI connection here and keeps the
    # entire autocomplete/normalize/standardize SQL within that transaction.
    if connection is None:
        from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import _ENGINE
        with _ENGINE.connect() as db_conn:
            return enrich_norm_addy(
                source, street_col=street_col, addr2_col=addr2_col,
                city_col=city_col, state_col=state_col, zip_col=zip_col,
                zip4_col=zip4_col, batch_size=batch_size,
                connection=db_conn.connection.driver_connection,
            )
    cleaned = clean_addresses(
        inputs, 'street', 'city', 'state', 'zipcode',
        ADDRESS2='addr2', ZIP4='zip4',
        batch_size=batch_size, keep_invalid=True,
        use_autocomplete=True, compact=False,
        connection=connection,
    )
    if len(cleaned) != len(source):
        raise ValueError('AddrAll changed the number of address rows')
    records = []
    for raw, enriched in zip(inputs.to_dict('records'), cleaned.to_dict('records')):
        norm = norm_addy_from_enriched(enriched, raw)
        norm['address_id'] = canonical_address_id(norm)
        norm['_smarty'] = smarty_from_enriched(enriched, norm)
        records.append(norm)
    return records


def smarty_from_enriched(enriched: Mapping[str, Any], norm: Mapping[str, Any]) -> dict[str, str | None]:
    """Project AddrAll's real Smarty-compatible columns, not parser telemetry.

    Address filters must use the same canonical locality/ZIP as the stored
    norm_addy. A source-only ZIP+4 cannot be attached to a changed ZIP5.
    """
    postal_zip = normalize_zipcode(norm.get('zip'))
    postal_zip4 = normalize_zip4(enriched.get('plus4_code')) or normalize_zip4(norm.get('zip4'))
    primary = _value(norm.get('address_alphanumeric')) or (
        str(norm['address']) if norm.get('address') is not None else None
    )
    street_parts = (
        primary, _value(norm.get('predirAbbrev')), _value(norm.get('streetName')),
        _value(norm.get('streetTypeAbbrev')), _value(norm.get('postdirAbbrev')),
    )
    fallback_line = ' '.join(part for part in street_parts if part)
    if fallback_line and norm.get('internal'):
        fallback_line += ' ' + str(norm['internal'])
    line1 = _usable_component(enriched.get('delivery_line_1'))
    if line1 and norm.get('internal'):
        original_building, inline_unit = split_secondary(line1)
        if inline_unit and _unit_key(inline_unit) != _unit_key(norm['internal']):
            # The raw delivery line can contain an apartment that conflicts
            # with an explicit addr2. Do not persist the contradictory unit
            # under an ID canonicalized from the explicit secondary.
            line1 = ' '.join((original_building, str(norm['internal'])))
    return {
        'delivery_line_1': line1 or _usable_component(fallback_line),
        'delivery_line_2': _usable_component(enriched.get('delivery_line_2')),
        'city_name': _usable_component(norm.get('location')),
        'state_abbreviation': _usable_component(norm.get('stateAbbrev')),
        'zipcode': postal_zip,
        'plus4_code': postal_zip4,
        'full_zipcode': full_zipcode(postal_zip, postal_zip4),
        'latitude': _value(enriched.get('latitude')),
        'longitude': _value(enriched.get('longitude')),
        'last_line': ' '.join(part for part in (
            _usable_component(norm.get('location')),
            _usable_component(norm.get('stateAbbrev')),
            full_zipcode(postal_zip, postal_zip4) or postal_zip,
        ) if part) or None,
        'primary_number': primary,
        'street_name': _usable_component(norm.get('streetName')),
        'street_predirection': _usable_component(norm.get('predirAbbrev')),
        'street_suffix': _usable_component(norm.get('streetTypeAbbrev')),
        'street_postdirection': _usable_component(norm.get('postdirAbbrev')),
        'hashed_address_id': None,  # filled after canonical UUID assignment
    }


def ensure_norm_addy_attributes(conn: Any) -> dict[str, str]:
    """Require the Smarty + norm_addy JSONB attributes from the bootstrap SQL."""
    from sqlalchemy import text

    found = conn.execute(text("""
        SELECT id::text AS id, name, data_type::text AS dtype
        FROM sales.attributes
        WHERE entity_type_id = CAST(:etype AS uuid)
          AND name = ANY(:names)
    """), {'etype': str(ADDRESS_TYPE_ID), 'names': list(STORED_ADDRESS_TYPES)}).mappings()
    mapped = {row['name']: row['id'] for row in found
              if STORED_ADDRESS_TYPES.get(row['name']) == row['dtype']}
    if set(mapped) != set(STORED_ADDRESS_TYPES):
        raise RuntimeError(
            'Apply sales Smarty + norm_addy JSONB schema bootstrap first; '
            f'missing attributes: {set(STORED_ADDRESS_TYPES) - set(mapped)}'
        )
    return mapped


def upsert_norm_addy_records(conn: Any, records: list[dict[str, Any]]) -> int:
    """Create/reuse canonical address IDs; upsert Smarty values + norm_addy JSONB.

    Called inside the caller's DB transaction. Existing non-null fields remain
    available when another source supplies only a partial address.
    """
    from sqlalchemy import bindparam, text
    from sqlalchemy.dialects.postgresql import JSONB

    canonical: dict[str, dict[str, Any]] = {}
    for row in records:
        aid = row.get('address_id')
        if not aid:
            continue
        aid = str(aid)
        if aid not in canonical:
            canonical[aid] = dict(row)
            canonical[aid]['_smarty'] = dict(row.get('_smarty') or {})
            continue
        retained = canonical[aid]
        for name in NORM_ADDY_TYPES:
            if retained.get(name) in (None, '') and row.get(name) not in (None, ''):
                retained[name] = row[name]
        stored_smarty = retained['_smarty']
        for name, value in (row.get('_smarty') or {}).items():
            if stored_smarty.get(name) in (None, '') and value not in (None, ''):
                stored_smarty[name] = value
    if not canonical:
        return 0

    attrs = ensure_norm_addy_attributes(conn)
    conn.execute(text("""
        INSERT INTO sales.entities (id, entity_type_id, parent_entity_id)
        VALUES (CAST(:id AS uuid), CAST(:etype AS uuid), NULL)
        ON CONFLICT (id) DO NOTHING
    """), [{'id': aid, 'etype': str(ADDRESS_TYPE_ID)} for aid in canonical])
    owners = conn.execute(text("""
        SELECT id::text AS id, entity_type_id::text AS entity_type_id
        FROM sales.entities WHERE id = ANY(CAST(:ids AS uuid[]))
    """), {'ids': list(canonical)}).mappings().all()
    if len(owners) != len(canonical) or any(
        owner['entity_type_id'] != str(ADDRESS_TYPE_ID) for owner in owners
    ):
        raise RuntimeError('Canonical address UUID already belongs to another entity type')

    # New ingestion may have written the target address before the migration.
    # Such entities are valid; verify their stored structured identity rather
    # than refusing snapshot or overwriting conflicting JSONB on conflict.
    existing = conn.execute(text("""
        SELECT v.entity_id::text AS id, v.value_jsonb AS norm
        FROM sales.entity_attribute_values v
        WHERE v.entity_id = ANY(CAST(:ids AS uuid[]))
          AND v.attribute_id = CAST(:attr_id AS uuid)
    """), {'ids': list(canonical), 'attr_id': attrs['norm_addy']}).mappings().all()
    for stored in existing:
        aid = stored['id']
        expected = {k: v for k in NORM_ADDY_TYPES
                    if (v := canonical[aid].get(k)) is not None and v != ''}
        if stored['norm'] != expected:
            raise RuntimeError(f'Existing canonical UUID has a different norm_addy: {aid}')

    eav_rows = []
    for aid, row in canonical.items():
        # Use only the 11 canonical address components, never parser status.
        payload = {key: val for key in NORM_ADDY_TYPES
                   if (val := row.get(key)) is not None and val != ''}
        if not any(row.get(key) not in (None, '') for key in ID_FIELDS):
            raise RuntimeError('Cannot persist an entirely empty address')
        eav_rows.append({
            'entity_id': aid, 'attribute_id': attrs['norm_addy'],
            'value_string': None, 'value_jsonb': payload,
        })
        smarty = row.get('_smarty') or {}
        for field in SMARTY_FIELDS:
            value = aid.upper() if field == 'hashed_address_id' else smarty.get(field)
            if value is None or value == '':
                continue
            eav_rows.append({
                'entity_id': aid, 'attribute_id': attrs[field],
                'value_string': str(value), 'value_jsonb': None,
            })

    if eav_rows:
        stmt = text("""
            INSERT INTO sales.entity_attribute_values
                (entity_id, attribute_id, value_string, value_jsonb)
            VALUES (CAST(:entity_id AS uuid), CAST(:attribute_id AS uuid),
                    :value_string, :value_jsonb)
            ON CONFLICT (entity_id, attribute_id) DO UPDATE SET
                value_string = COALESCE(EXCLUDED.value_string,
                                        sales.entity_attribute_values.value_string),
                value_jsonb = CASE WHEN EXCLUDED.value_jsonb IS NOT NULL THEN
                    COALESCE(sales.entity_attribute_values.value_jsonb, '{}'::jsonb)
                    || EXCLUDED.value_jsonb
                    ELSE sales.entity_attribute_values.value_jsonb END
        """).bindparams(bindparam('value_jsonb', type_=JSONB()))
        conn.execute(stmt, eav_rows)
    return len(canonical)
