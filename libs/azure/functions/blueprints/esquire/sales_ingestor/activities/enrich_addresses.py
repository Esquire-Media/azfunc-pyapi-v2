from azure.durable_functions import Blueprint
from sqlalchemy import create_engine, text
import pandas as pd
import os
from math import ceil
import logging
from sqlalchemy.exc import OperationalError
from functools import lru_cache
import time

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.address_norm_addy import (
    enrich_norm_addy,
    upsert_norm_addy_records,
)

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.field_mapping import (
    normalize_fields_to_standardized,
)

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@lru_cache()
def _engine():
    return create_engine(
        os.environ["DATABIND_SQL_KEYSTONE"].replace("psycopg2", "psycopg"),
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        future=True,
    )


def safe_engine_connect(engine, retries=3, delay=2):
    for attempt in range(retries):
        try:
            return engine.connect()
        except OperationalError as e:
            if "Temporary failure in name resolution" in str(e) or "could not translate host name" in str(e):
                if attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))
                else:
                    raise
            else:
                raise


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_planAddressBatches(settings: dict):
    standardized_fields = normalize_fields_to_standardized(settings["fields"])

    scope = settings["scope"]
    addr_map = standardized_fields[scope]
    batch_size = int(settings.get("batch_size", 200))
    staging = settings["staging_table"]

    col_name = f"{scope}_address_id"
    logging.warning(
        f"""
                ALTER TABLE {qtbl(settings["staging_table"])}
                ADD COLUMN IF NOT EXISTS "{col_name}" UUID;
            """,
        extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}},
    )

    with _engine().begin() as conn:
        conn.execute(
            text(
                f"""
                ALTER TABLE {qtbl(settings["staging_table"])}
                ADD COLUMN IF NOT EXISTS "{col_name}" UUID;
                """
            )
        )
        conn.execute(
            text(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'sales'
                  AND table_name = '{staging.split('.')[-1]}'
                  AND column_name = '{col_name}';
                """
            )
        ).fetchone()

    expected_keys = ["street", "addr2", "city", "state", "zipcode", "zip4"]
    cols = [
        addr_map[k]
        for k in expected_keys
        if k in addr_map and addr_map[k]
    ]
    if not cols:
        return {"ranges": []}
    
    # ensure columns actually exist in staging
    with safe_engine_connect(_engine()) as conn:
        existing_cols = conn.execute(
            text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'sales'
                AND table_name = '{staging.split('.')[-1]}'
            """)
        ).scalars().all()
    cols = [c for c in cols if c in existing_cols]

    if not cols:
        return {"ranges": []}

    quoted_cols = [f'"{c}"' for c in cols]
    select_list = ", ".join(quoted_cols)
    order_by = ", ".join(quoted_cols)

    with safe_engine_connect(_engine()) as conn:
        total = conn.execute(
            text(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT {select_list}
                    FROM {qtbl(staging)}
                ) t
                """
            )
        ).scalar() or 0

    if total == 0:
        return {"ranges": []}

    ranges = []
    num_batches = ceil(total / batch_size)
    for i in range(num_batches):
        ranges.append({"offset": i * batch_size, "limit": batch_size})

    logger.warning(
        f"[LOG] Address plan scope={scope}, cols={cols}, total={total}, batches={num_batches}",
        extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}},
    )

    # AddrAll performs multiple batched SQL passes: prevent host fan-out from
    # multiplying its connection and statement cost uncontrollably.
    suggested_parallelism = max(1, min(4, int(settings.get('parallelism', 2))))

    return {
        "ranges": ranges,
        "cols": cols,
        "total": total,
        "batch_size": batch_size,
        "suggested_parallelism": suggested_parallelism,
    }


@bp.activity_trigger(input_name="payload")
def activity_salesIngestor_enrichAddresses_batch(payload: dict):
    standardized_fields = normalize_fields_to_standardized(payload["fields"])

    scope = payload["scope"]
    staging = payload["staging_table"]
    addr_map = standardized_fields[scope]
    offset = int(payload["range"]["offset"])
    limit = int(payload["range"]["limit"])

    expected_keys = ["street", "addr2", "city", "state", "zipcode", "zip4"]
    cols = [
        addr_map[k]
        for k in expected_keys
        if k in addr_map and addr_map[k]
    ]
    # The plan already checked which mapped headers really exist in staging.
    cols = payload.get('cols', cols)
    addr_map = {key: (column if column in cols else None)
                for key, column in addr_map.items()}
    quoted_cols = [f'"{c}"' for c in cols]
    select_list = ", ".join(quoted_cols)
    order_by = ", ".join(quoted_cols)

    eng = _engine()
    with eng.connect() as conn:
        rs = conn.execute(
            text(
                f"""
                SELECT {select_list}
                  FROM (
                        SELECT DISTINCT {select_list}
                          FROM {qtbl(staging)}
                       ) d
                 ORDER BY {order_by}
                 OFFSET :off LIMIT :lim
                """
            ),
            {"off": offset, "lim": limit},
        )
        df = pd.DataFrame(rs.fetchall(), columns=[c.strip('"') for c in cols])

    return process_batch_fast(
        raw_df=df,
        addr_map=addr_map,
        scope=scope,
        staging_table=qtbl(staging),
    )


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_enrichAddresses(settings: dict):
    """Legacy single-activity entry point, bounded to planned SQL pages."""
    settings = dict(settings)
    settings.setdefault('staging_table', settings.get('table_name'))
    plan = activity_salesIngestor_planAddressBatches(settings)
    for address_range in plan['ranges']:
        activity_salesIngestor_enrichAddresses_batch({
            'fields': settings['fields'],
            'scope': settings['scope'],
            'staging_table': settings['staging_table'],
            'range': address_range,
            'cols': plan['cols'],
            'metadata': settings['metadata'],
        })
    return f"{settings['scope']} enrichment complete"


def _bind_value(value):
    """Return a Python NULL for Pandas NA values, without stringifying them."""
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _quoted_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def process_batch_fast(
    raw_df: pd.DataFrame,
    addr_map: dict,
    scope: str,
    staging_table: str,
):
    """Upsert canonical norm_addy entities, then assign their IDs to staging.

    Original staged fields remain untouched. Include addr2 and optional ZIP4
    in the row match; use IS NOT DISTINCT FROM for SQL NULLs. Clear stale IDs
    when an input has no identifiable address.
    """
    if raw_df.empty:
        return f'{scope} enrichment complete (0 addresses)'
    col_name = f'{scope}_address_id'
    fields = ('street', 'addr2', 'city', 'state', 'zipcode', 'zip4')
    records = enrich_norm_addy(
        raw_df,
        street_col=addr_map.get('street'),
        addr2_col=addr_map.get('addr2'),
        city_col=addr_map.get('city'),
        state_col=addr_map.get('state'),
        zip_col=addr_map.get('zipcode'),
        zip4_col=addr_map.get('zip4'),
        batch_size=200,
    )
    mapped_cols = {key: addr_map.get(key) for key in fields}
    source_rows = raw_df.to_dict('records')
    mapping = [
        {**{key: _bind_value(raw.get(mapped_cols[key])) if mapped_cols[key] else None
             for key in fields}, 'address_id': enriched['address_id']}
        for raw, enriched in zip(source_rows, records)
    ]

    with _engine().begin() as conn:
        upsert_norm_addy_records(conn, records)
        for start in range(0, len(mapping), 100):
            chunk = mapping[start:start + 100]
            params = {}
            placeholders = []
            for index, row in enumerate(chunk):
                names = []
                for field in (*fields, 'address_id'):
                    param = f'{field}_{index}'
                    params[param] = row[field]
                    names.append(f':{param}')
                placeholders.append('(' + ', '.join(names) + ')')
            if not placeholders:
                continue
            predicate = ' AND '.join(
                f'st.{_quoted_identifier(mapped_cols[field])} '
                f'IS NOT DISTINCT FROM m.{field}'
                for field in fields if mapped_cols[field]
            )
            if not predicate:
                raise ValueError('No mapped address columns exist in staging')
            statement = f'''
                WITH mapping (street, addr2, city, state, zipcode, zip4, address_id) AS (
                    VALUES {', '.join(placeholders)}
                )
                UPDATE {staging_table} AS st
                SET {_quoted_identifier(col_name)} = CAST(m.address_id AS uuid)
                FROM mapping AS m
                WHERE {predicate}
            '''
            conn.execute(text(statement), params)
    return f'{scope} enrichment complete'
