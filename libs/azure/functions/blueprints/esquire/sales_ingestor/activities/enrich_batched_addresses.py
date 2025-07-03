
from sqlalchemy import create_engine, MetaData, Table, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
import uuid
import pandas as pd
from libs.utils.smarty import bulk_validate
from azure.durable_functions import Blueprint, activity_trigger
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.generate_ids import (
    NAMESPACE_ADDRESS,
    generate_deterministic_id
)
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import qtbl

# Shared engine + metadata
_ENGINE = create_engine("postgresql://user:pass@host:5432/your_db")
_META   = MetaData(bind=_ENGINE)
_STG    = Table("staging_table", _META, autoload_with=_ENGINE)
_ENT    = Table("entities",       _META, autoload_with=_ENGINE)
_ET     = Table("entity_types",   _META, autoload_with=_ENGINE)

NAMESPACE_ADDRESS = uuid.UUID("30000000-0000-0000-0000-000000000000")
ADDRESS_TYPE_ID = uuid.UUID("fe694dd2-2dc4-452f-910c-7023438bb0ac")

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def enrich_addresses_in_batches(settings: dict) -> str:
    """
    settings = {
      "scope":         "billing" | "shipping",
      "address_map":   { "street": "...", "addr2": "...", ... },
      "staging_table": "schema.my_staging",
      "tenant_id":     "...",
      "batch_size":    500  # optional override
    }
    """

    scope      = settings["scope"]
    addr_map   = settings["fields"][scope]
    batch_size = settings.get("batch_size", 500)

    col_name = f"{scope}_address_id"
    with _ENGINE.begin() as conn:
        conn.execute(text(
            f'ALTER TABLE {qtbl(settings["staging_table"])} '
            f'ADD COLUMN IF NOT EXISTS "{col_name}" UUID;'
        ))

    # reflect staging table if changed
    stg = Table(settings["staging_table"], _META, autoload_with=_ENGINE, schema='sales')

    def stream_batches():
        # build DISTINCT-stmt on the five raw columns
        cols = [stg.c[addr_map[k]] for k in ["street","addr2","city","state","zipcode"] if addr_map[k]]
        stmt = select(*cols).distinct()
        with _ENGINE.connect() as conn:
            rs = conn.execution_options(stream_results=True).execute(stmt)
            while True:
                rows = rs.fetchmany(batch_size)
                if not rows:
                    break
                yield pd.DataFrame(rows, columns=[c.name for c in cols])

    with _ENGINE.begin() as conn:
        for df in stream_batches():
            process_batch_fast(
                raw_df=df,
                addr_map=settings['fields'][scope],
                scope=settings['scope'],
                staging_table=qtbl(settings["staging_table"])
            )

    return f"{scope} enrichment complete"



def process_batch_fast(
    raw_df: pd.DataFrame,
    addr_map: dict,
    scope: str,
    staging_table: str,
):
    """
    A fast, set-based batch UPDATE that mirrors the
    working logic from enrich_slow.py :contentReference[oaicite:0]{index=0}
    but avoids per-row round trips.
    """
    col_name = f"{scope}_address_id"

    # 1) standardize & uuid—as in enrich_slow.py
    cleaned = bulk_validate(
        raw_df,
        address_col = addr_map['street'],
        city_col    = addr_map['city'],
        state_col   = addr_map['state'],
        zip_col     = addr_map['zipcode']
    )
    cleaned['address_id'] = cleaned.apply(
        lambda entry: generate_deterministic_id(
            NAMESPACE_ADDRESS,
            [
              entry['delivery_line_1'],
              entry['city_name'],
              entry['state_abbreviation'],
              entry['zipcode']
            ]
        ),
        axis=1
    )

    # 2) bring raw inputs + new UUID into one list of dicts
    #    raw_df columns are the original raw staging names (e.g. "Address1", "City", etc.)
    batch = raw_df.copy()
    batch['address_id'] = cleaned['address_id'].values
    records = batch.to_dict('records')

    # 3) build VALUES(...) plus bind-params
    vals_sql = []
    params   = {}
    for i, rec in enumerate(records, start=1):
        phs = []
        # street is required
        params[f"street_{i}"] = rec[addr_map['street']]
        phs.append(f":street_{i}")

        # optional fields: only include the ones you have in addr_map
        for key in ("city","state","zipcode"):
            col = addr_map.get(key)
            params[f"{key}_{i}"] = rec[col] if col else None
            phs.append(f":{key}_{i}")

        # final address_id placeholder
        params[f"address_id_{i}"] = str(rec['address_id'])
        phs.append(f":address_id_{i}")

        vals_sql.append(f"({', '.join(phs)})")

    values_clause = ",\n    ".join(vals_sql)

    # 4) build the WHERE predicates exactly like your slow version
    where_parts = []
    # street
    where_parts.append(f'st."{addr_map["street"]}" = m.street')
    # city, state, zipcode if provided
    for key in ("city","state","zipcode"):
        col = addr_map.get(key)
        if col:
            where_parts.append(f'st."{col}" = m.{key}')

    where_sql = " AND ".join(where_parts)

    # 5) one single CTE+UPDATE
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

    with _ENGINE.begin() as conn:
        conn.execute(text(sql), params)

    # 6) upsert into entities (same as slow method)
    unique_ids = cleaned['address_id'].unique().tolist()
    rows = [
        {
          'id':               str(aid),
          'entity_type_id':   ADDRESS_TYPE_ID,
          'parent_entity_id': None
        }
        for aid in unique_ids
    ]
    stmt = pg_insert(_ENT).values(rows)
    with _ENGINE.begin() as conn2:
        conn2.execute(stmt.on_conflict_do_nothing(index_elements=['id']))

    return f"{scope} enrichment complete"