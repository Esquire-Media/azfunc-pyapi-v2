from azure.durable_functions import Blueprint
from sqlalchemy import create_engine, MetaData, Table, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
import uuid
import pandas as pd
import os
from math import ceil
import logging
from sqlalchemy.exc import OperationalError
from functools import lru_cache
import time
from libs.utils.smarty import bulk_validate
from libs.utils.text import format_zipcode
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.generate_ids import (
    NAMESPACE_ADDRESS,
    generate_deterministic_id
)
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import qtbl

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

ADDRESS_TYPE_ID = uuid.UUID("fe694dd2-2dc4-452f-910c-7023438bb0ac")

# Cache the engine for reuse within the function app process
@lru_cache()
def _engine():
    return create_engine(
        os.environ["DATABIND_SQL_KEYSTONE"].replace("psycopg2", "psycopg"),
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        future=True,
    )

# Retry wrapper for transient connection errors (e.g. DNS failures)
def safe_engine_connect(engine, retries=3, delay=2):
    for attempt in range(retries):
        try:
            return engine.connect()
        except OperationalError as e:
            if "Temporary failure in name resolution" in str(e) or "could not translate host name" in str(e):
                if attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))  # exponential backoff
                else:
                    raise
            else:
                raise

# ---------------------------------------------------------------------------------
# (New) Activity: plan distinct-address batches with stable ORDER BY + OFFSET/LIMIT
# ---------------------------------------------------------------------------------
@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_planAddressBatches(settings: dict):
    """
    Returns ranges = [{'offset': int, 'limit': int}, ...] using DISTINCT over the
    selected address columns with a stable ORDER BY. No shared temp tables.
    Also ensures the scope address_id column exists (mirrors original behavior).
    """
    scope      = settings["scope"]
    addr_map   = settings["fields"][scope]
    batch_size = int(settings.get("batch_size", 500))
    staging    = settings["staging_table"]

    # Ensure the "{scope}_address_id" exists (as in original activity):contentReference[oaicite:4]{index=4}
    col_name = f"{scope}_address_id"
    with safe_engine_connect(_engine()) as conn:
        conn.execute(text(
            f'ALTER TABLE {qtbl(staging)} '
            f'ADD COLUMN IF NOT EXISTS "{col_name}" UUID;'
        ))

    # Build column list (skip blanks) — matches original stream_batches intent:contentReference[oaicite:5]{index=5}
    cols = [addr_map[k] for k in ["street","addr2","city","state","zipcode"] if (addr_map.get(k,'') != '')]
    if not cols:
        # Degenerate case: nothing to validate
        return {"ranges": []}

    quoted_cols = [f'"{c}"' for c in cols]
    select_list = ", ".join(quoted_cols)
    order_by    = ", ".join(quoted_cols)  # stable ordering over the same raw columns

    # Total distinct rows
    with safe_engine_connect(_engine()) as conn:
        total = conn.execute(text(f"""
            SELECT COUNT(*) FROM (SELECT DISTINCT {select_list}
                                    FROM {qtbl(staging)}) t
        """)).scalar() or 0

    if total == 0:
        return {"ranges": []}

    # Produce ranges for fan-out
    ranges = []
    num_batches = ceil(total / batch_size)
    for i in range(num_batches):
        ranges.append({"offset": i*batch_size, "limit": batch_size})

    logger.info(f"[LOG] Address plan scope={scope}, cols={cols}, total={total}, batches={num_batches}")

    # After calculating total distinct addresses
    min_p, max_p = 2, 10
    if total < 5000:
        suggested_parallelism = min_p
    elif total < 50000:
        suggested_parallelism = 10
    elif total < 250000:
        suggested_parallelism = 25
    else:
        suggested_parallelism = max_p

    return {
        "ranges": ranges,
        "cols": cols,
        "total": total,
        "batch_size": batch_size,
        "suggested_parallelism": suggested_parallelism
    }

# ---------------------------------------------------------------------------------
# (New) Activity: process a single slice using the DISTINCT ORDER BY window
# ---------------------------------------------------------------------------------
@bp.activity_trigger(input_name="payload")
def activity_salesIngestor_enrichAddresses_batch(payload: dict):
    scope        = payload["scope"]
    staging      = payload["staging_table"]
    addr_map     = payload["fields"][scope]
    offset       = int(payload["range"]["offset"])
    limit        = int(payload["range"]["limit"])

    cols = [addr_map[k] for k in ["street","addr2","city","state","zipcode"] if (addr_map.get(k,'') != '')]
    quoted_cols = [f'"{c}"' for c in cols]
    select_list = ", ".join(quoted_cols)
    order_by    = ", ".join(quoted_cols)

    eng = _engine()
    with eng.connect() as conn:
        rs = conn.execute(
            text(f"""
                SELECT {select_list}
                  FROM (
                        SELECT DISTINCT {select_list}
                          FROM {qtbl(staging)}
                       ) d
                 ORDER BY {order_by}
                 OFFSET :off LIMIT :lim
            """),
            {"off": offset, "lim": limit}
        )
        df = pd.DataFrame(rs.fetchall(), columns=[c.strip('"') for c in cols])

    # Reuse your existing fast, set-based batch logic:contentReference[oaicite:6]{index=6}
    return process_batch_fast(
        raw_df=df,
        addr_map=addr_map,
        scope=scope,
        staging_table=qtbl(staging)
    )

# ---------------------------------------------------------------------------------
# (Existing) Activity — retained for compatibility; no longer loops batches here.
# You may keep it unused or call it for small jobs; the sub-orchestrator now fans out.
# ---------------------------------------------------------------------------------
@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_enrichAddresses(settings: dict):
    """
    Original activity retained. For parallel runs, the sub-orchestrator now calls:
      - activity_salesIngestor_planAddressBatches
      - activity_salesIngestor_enrichAddresses_batch (N times in parallel)
    """
    logger.info(msg=f"[LOG] Enriching {settings['scope']} Addresses (single-activity path)")
    # For backward compatibility, do a single pass using one big DISTINCT and call process_batch_fast once.
    scope      = settings["scope"]
    addr_map   = settings["fields"][scope]

    eng = _engine()
    # Ensure the column exists as in the original
    col_name = f"{scope}_address_id"
    with eng.begin() as conn:
        conn.execute(text(
            f'ALTER TABLE {qtbl(settings["staging_table"])} '
            f'ADD COLUMN IF NOT EXISTS "{col_name}" UUID;'
        ))

    cols = [addr_map[k] for k in ["street","addr2","city","state","zipcode"] if (addr_map.get(k,'') != '')]
    if not cols:
        return f"{scope} enrichment complete (no address columns)"

    quoted_cols = [f'"{c}"' for c in cols]
    select_list = ", ".join(quoted_cols)
    order_by    = ", ".join(quoted_cols)

    with eng.connect() as conn:
        rs = conn.execute(text(f"""
            SELECT DISTINCT {select_list}
              FROM {qtbl(settings["staging_table"])}
             ORDER BY {order_by}
        """))
        df = pd.DataFrame(rs.fetchall(), columns=[c.strip('"') for c in cols])

    return process_batch_fast(
        raw_df=df,
        addr_map=addr_map,
        scope=scope,
        staging_table=qtbl(settings["staging_table"])
    )

# ---------------------------------------------------------------------------------
# (Existing) Core fast batch logic — unchanged in spirit:contentReference[oaicite:7]{index=7}
# ---------------------------------------------------------------------------------
def process_batch_fast(
    raw_df: pd.DataFrame,
    addr_map: dict,
    scope: str,
    staging_table: str,
):
    """
    A fast, set-based batch UPDATE that mirrors your original logic but is
    now called by parallel batch activities. Idempotent via address_id.
    """
    col_name = f"{scope}_address_id"

    # Ensure required columns exist in the batch frame
    for k in ['street', 'city', 'state', 'zipcode']:
        col = addr_map.get(k)
        if col and (col not in raw_df.columns):
            raw_df[col] = ""

    # 1) validate & standardize using the external bulk API:contentReference[oaicite:8]{index=8}
    cleaned = bulk_validate(
        raw_df,
        address_col = addr_map['street'] if addr_map.get('street') else None,
        addr2_col   = addr_map.get('addr2') if addr_map.get('addr2') else None,
        city_col    = addr_map.get('city') if addr_map.get('city') else None,
        state_col   = addr_map.get('state') if addr_map.get('state') else None,
        zip_col     = addr_map.get('zipcode') if addr_map.get('zipcode') else None
    )
    cleaned['address_id'] = cleaned.apply(
        lambda entry: generate_deterministic_id(
            NAMESPACE_ADDRESS,
            [
              entry.get('delivery_line_1', ''),
              entry.get('delivery_line_2', ''),
              entry.get('city_name', ''),
              entry.get('state_abbreviation', ''),
              entry.get('zipcode', '')
            ]
        ),
        axis=1
    )

    # 2) raw + uuid
    batch = raw_df.copy()
    batch['address_id'] = cleaned['address_id'].values
    records = batch.to_dict('records')

    # 3) VALUES(...) + params for set-based UPDATE
    vals_sql = []
    params   = {}
    for i, rec in enumerate(records, start=1):
        phs = []
        # street is required
        street_col = addr_map.get('street')
        params[f"street_{i}"] = rec.get(street_col, "") if street_col else None
        phs.append(f":street_{i}")

        for key in ("city","state","zipcode"):
            col = addr_map.get(key)
            params[f"{key}_{i}"] = rec.get(col) if col else None
            phs.append(f":{key}_{i}")

        params[f"address_id_{i}"] = str(rec['address_id'])
        phs.append(f":address_id_{i}")

        vals_sql.append(f"({', '.join(phs)})")

    if not vals_sql:
        return f"{scope} enrichment complete (empty batch)"

    values_clause = ",\n    ".join(vals_sql)

    where_parts = []
    if addr_map.get("street"):
        where_parts.append(f'st."{addr_map["street"]}" = m.street')
    for key in ("city","state","zipcode"):
        col = addr_map.get(key)
        if col:
            where_parts.append(f'st."{col}" = m.{key}')
    where_sql = " AND ".join(where_parts) if where_parts else "TRUE"

    sql = f"""
    WITH mapping (street, city, state, zipcode, address_id) AS (
      VALUES
      {values_clause}
    )
    UPDATE {staging_table} AS st
       SET "{col_name}" = m.address_id::uuid
      FROM mapping AS m
     WHERE {where_sql};
    """

    eng = _engine()
    meta = MetaData()

    with eng.begin() as conn:
        conn.execute(text(sql), params)

    # 6) upsert into entities (same as original):contentReference[oaicite:9]{index=9}
    _ENT    = Table("entities",       meta, autoload_with=eng, schema='sales')
    unique_ids = cleaned['address_id'].unique().tolist()
    rows = [
        {
          'id':               str(aid),
          'entity_type_id':   ADDRESS_TYPE_ID,
          'parent_entity_id': None
        }
        for aid in unique_ids
    ]
    if rows:
        stmt = pg_insert(_ENT).values(rows)
        with eng.begin() as conn2:
            conn2.execute(stmt.on_conflict_do_nothing(index_elements=['id']))

    upsert_address_attributes(cleaned)

    return f"{scope} enrichment complete"

def upsert_address_attributes(cleaned: pd.DataFrame):
    """
    cleaned: DataFrame with columns
      ['delivery_line_1','city_name','state_abbreviation','zipcode','address_id', 'latitude', 'longitude', ...]
    """
    eng = _engine()

    # 1) Ensure attribute definitions exist (unchanged):contentReference[oaicite:10]{index=10}
    ATTRIBUTE_NAMES = [
        'delivery_line_1',
        'delivery_line_2',
        'city_name',
        'state_abbreviation',
        'zipcode',
        'plus4_code',
        'latitude',
        'longitude',
        'addressee',
        'default_city_name',
        'last_line',
        'delivery_point_barcode',
        'urbanization',
        'primary_number',
        'street_name',
        'street_predirection',
        'street_postdirection',
        'street_suffix',
        'secondary_number',
        'secondary_designator',
        'extra_secondary_number',
        'extra_secondary_designator',
        'pmb_designator',
        'pmb_number',
        'delivery_point',
        'delivery_point_check_digit',
        'record_type',
        'zip_type',
        'county_fips',
        'county_name',
        'carrier_route',
        'congressional_district',
        'building_default_indicator',
        'rdi',
        'elot_sequence',
        'elot_sort',
        'coordinate_license',
        'precision',
        'time_zone',
        'utc_offset',
        'obeys_dst',
        'is_ews_match',
        'dpv_match_code',
        'dpv_footnotes',
        'cmra',
        'vacant',
        'active',
        'dpv_no_stat',
        'footnotes',
        'lacs_link_code',
        'lacs_link_indicator',
        'is_suite_link_match',
        'enhanced_match'
    ]

    rows = [
        {
            'entity_type_id': ADDRESS_TYPE_ID,
            'name':           name,
            'data_type':      'string'
        }
        for name in ATTRIBUTE_NAMES
    ]
    ATTR = Table('attributes', MetaData(), autoload_with=eng, schema='sales')
    stmt = pg_insert(ATTR).values(rows).on_conflict_do_nothing(index_elements=['entity_type_id', 'name', 'data_type'])

    with eng.begin() as conn:
        conn.execute(stmt)

    # 2) Re‐reflect attributes and EAV tables (unchanged):contentReference[oaicite:11]{index=11}
    meta     = MetaData()
    ATTR     = Table('attributes',            meta, autoload_with=eng, schema='sales')
    EAV      = Table('entity_attribute_values', meta, autoload_with=eng, schema='sales')

    # 3) Retrieve the attribute_ids
    with eng.connect() as conn:
        rows = conn.execute(text("""
          SELECT name, id
            FROM sales.attributes
           WHERE entity_type_id = :etype
             AND name = ANY(:names)
        """), {
          'etype': ADDRESS_TYPE_ID,
          'names': ATTRIBUTE_NAMES
        }).mappings().all()
    attr_map = {r['name']: r['id'] for r in rows}

    # 4) Build the list of EAV rows (unchanged):contentReference[oaicite:12]{index=12}
    cleaned["address_id"]    = cleaned["address_id"].astype(str)
    for col in ATTRIBUTE_NAMES:
        if col in cleaned.columns:
            cleaned[col] = cleaned[col].astype(str)
    if 'zipcode' in cleaned.columns:
        cleaned['zipcode'] = cleaned['zipcode'].apply(format_zipcode)

    eav_rows = []
    for entry in cleaned[ATTRIBUTE_NAMES + ['address_id']].dropna(how='any').itertuples(index=False):

        aid = str(entry.address_id)
        for col in ATTRIBUTE_NAMES:
            val = getattr(entry, col, None)
            if val:
                eav_rows.append({
                    'entity_id':    aid,
                    'attribute_id': attr_map[col],
                    'value_string': str(val).upper()
                })

    unique = {}
    for row in eav_rows:
        key = (row['entity_id'], row['attribute_id'])
        unique[key] = row
    eav_rows = list(unique.values())

    # Safe batch sizing (unchanged):contentReference[oaicite:13]{index=13}
    cols_per_row = 3
    max_params = 65000
    batch_size = max(1000, (max_params // cols_per_row) - 1000)  # ≈ 20k

    with eng.begin() as conn:
        for i in range(0, len(eav_rows), batch_size):
            chunk = eav_rows[i:i+batch_size]
            if not chunk:
                continue
            stmt = pg_insert(EAV).values(chunk)
            upsert = stmt.on_conflict_do_update(
                index_elements=['entity_id', 'attribute_id'],
                set_={'value_string': stmt.excluded.value_string}
            )
            conn.execute(upsert)
