from azure.durable_functions import Blueprint
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import JSONB, TEXT as PG_TEXT, UUID as PG_UUID
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import logging

bp = Blueprint()
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_eavPrelude(settings: dict):
    """
    One-time setup for a given upload:
      • sales_batch entity + metadata EAV
      • Attribute definitions for transaction vs line_item based on column classification
      • Client header mapping aligned with column types
    """
    staging_table = qtbl(settings['staging_table'])
    fields_map    = settings['fields']
    order_col     = fields_map['order_info']['order_num']

    sql = f"""

    -- 1) Create or get the sales_batch entity
    WITH inserted_sales_batch AS (
        INSERT INTO entities (id, entity_type_id)
        VALUES (
            :upload_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'sales_batch')
        )
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    ),
    sales_batch_entity AS (
        SELECT id FROM inserted_sales_batch
        UNION ALL
        SELECT id FROM entities WHERE id = :upload_id
    ),

    -- 2) Metadata → attributes → EAV (idempotent)
    metadata_fields AS (
        SELECT key AS name, value
        FROM jsonb_each_text(:metadata)
    ),
    sales_batch_attributes AS (
        SELECT
            gen_random_uuid() AS id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'sales_batch'),
            name,
            'string'::attr_data_type,
            NULL::text
        FROM metadata_fields
    ),
    insert_sales_batch_attributes AS (
        INSERT INTO attributes (id, entity_type_id, name, data_type, description)
        SELECT * FROM sales_batch_attributes
        ON CONFLICT (entity_type_id, name, data_type) DO NOTHING
        RETURNING id, name
    ),
    sb_attribute_info AS (
        SELECT a.id AS attribute_id, mf.value
        FROM attributes a
        JOIN metadata_fields mf ON a.name = mf.name
        WHERE a.entity_type_id = (SELECT entity_type_id FROM entity_types WHERE name = 'sales_batch')
    ),
    upsert_sales_batch_metadata AS (
        INSERT INTO entity_attribute_values (entity_id, attribute_id, value_string)
        SELECT (SELECT id FROM sales_batch_entity), attribute_id, value
        FROM sb_attribute_info
        ON CONFLICT (entity_id, attribute_id) DO UPDATE
        SET value_string = EXCLUDED.value_string
    ),

    -- 3) Column classification (global — same as your one-shot)
    staging_column_types AS (
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'sales' AND table_name = '{settings['staging_table']}'
    ),
    flattened_staging AS (
        SELECT s."{order_col}", col.key AS column_name, col.value AS column_value
        FROM {staging_table} s,
             LATERAL jsonb_each_text(to_jsonb(s)) col
    ),
    column_consistency AS (
        SELECT column_name,
               BOOL_AND(COUNT(DISTINCT column_value) = 1) OVER (PARTITION BY column_name) AS always_constant
        FROM flattened_staging
        GROUP BY "{order_col}", column_name
    ),
    column_classification AS (
        SELECT
            c.column_name,
            format_type(a.atttypid, a.atttypmod) AS data_type,
            cc.always_constant
        FROM information_schema.columns c
        JOIN pg_class cls ON cls.relname = c.table_name AND cls.relnamespace = 'sales'::regnamespace
        JOIN pg_attribute a ON a.attrelid = cls.oid AND a.attname = c.column_name
        JOIN column_consistency cc ON c.column_name = cc.column_name
        WHERE c.table_schema = 'sales'
          AND c.table_name  = '{settings['staging_table']}'
          AND a.attnum > 0 AND NOT a.attisdropped
    ),

    -- 4) Attribute definitions (same rules & data types as your updated EAV)
    transaction_attributes AS (
        SELECT
            gen_random_uuid() AS id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'transaction'),
            column_name AS name,
            CASE
                WHEN data_type IN ('character varying','text') THEN 'string'
                WHEN data_type IN ('numeric','integer','bigint','real','double precision') THEN 'numeric'
                WHEN data_type = 'boolean' THEN 'boolean'
                WHEN data_type LIKE 'timestamp%' THEN 'timestamptz'
                WHEN data_type IN ('json','jsonb') THEN 'jsonb'
                ELSE 'string'
            END::attr_data_type,
            NULL::text
        FROM column_classification
        WHERE always_constant = TRUE
    ),
    line_item_attributes AS (
        SELECT
            gen_random_uuid() AS id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'line_item'),
            column_name AS name,
            CASE
                WHEN data_type IN ('character varying','text') THEN 'string'
                WHEN data_type IN ('numeric','integer','bigint','real','double precision') THEN 'numeric'
                WHEN data_type = 'boolean' THEN 'boolean'
                WHEN data_type LIKE 'timestamp%' THEN 'timestamptz'
                WHEN data_type IN ('json','jsonb') THEN 'jsonb'
                ELSE 'string'
            END::attr_data_type,
            NULL::text
        FROM column_classification
        WHERE always_constant = FALSE
    ),
    insert_new_attributes AS (
        INSERT INTO attributes (id, entity_type_id, name, data_type, description)
        SELECT * FROM transaction_attributes
        UNION ALL
        SELECT * FROM line_item_attributes
        ON CONFLICT (entity_type_id, name, data_type) DO NOTHING
        RETURNING id, name, entity_type_id, data_type
    ),

    -- 5) Client header map aligned with column types (same logic as your updated EAV)
    fields_cte AS ( SELECT :fields AS fields_json ),
    fields_mapping AS ( SELECT key, value FROM fields_cte, jsonb_each_text(fields_json) ),
    client_header_mappings AS (
        SELECT
            :tenant_id AS tenant_id,
            fm.key AS mapped_header,
            a.id  AS attribute_id
        FROM fields_mapping fm
        JOIN column_classification cc
          ON cc.column_name = fm.value
        JOIN (
            SELECT id, name, entity_type_id, data_type FROM attributes
            UNION ALL
            SELECT id, name, entity_type_id, data_type FROM insert_new_attributes
        ) a
          ON a.name = fm.value
         AND (
                (cc.data_type IN ('character varying','text') AND a.data_type = 'string')
             OR (cc.data_type IN ('numeric','integer','bigint','real','double precision') AND a.data_type = 'numeric')
             OR (cc.data_type = 'boolean' AND a.data_type = 'boolean')
             OR (cc.data_type LIKE 'timestamp%' AND a.data_type = 'timestamptz')
             OR (cc.data_type IN ('json','jsonb') AND a.data_type = 'jsonb')
         )
         AND a.entity_type_id IN (
            (SELECT entity_type_id FROM entity_types WHERE name = 'transaction'),
            (SELECT entity_type_id FROM entity_types WHERE name = 'line_item')
         )
    )
    INSERT INTO client_header_map (tenant_id, mapped_header, attribute_id)
    SELECT tenant_id, mapped_header, attribute_id
    FROM client_header_mappings
    ON CONFLICT (tenant_id, mapped_header) DO NOTHING;
    """

    def flatten_fields(d, parent_key='', result=None):
        # same helper you ship in your EAV function (inlined here to avoid imports) :contentReference[oaicite:4]{index=4}
        if parent_key not in ['billing', 'shipping']:
            parent_key = ''
        if result is None:
            result = {}
        for k, v in d.items():
            new_key = f"{parent_key}_{k}" if parent_key else k
            if isinstance(v, dict):
                flatten_fields(v, new_key, result)
            elif isinstance(v, str) and v:
                result[new_key] = v
        return result

    stmt = text(sql).bindparams(
        bindparam("upload_id", value=settings['metadata']['upload_id'], type_=PG_UUID),
        bindparam("tenant_id", value=settings['metadata']['tenant_id'], type_=PG_TEXT),
        bindparam("fields",   value=flatten_fields(settings["fields"]), type_=JSONB),
        bindparam("metadata", value=flatten_fields(settings["metadata"]), type_=JSONB),
    )


    with db() as conn:
        conn.execute(text("SET search_path TO sales"))
        # Server-side protections
        conn.execute(text("SET LOCAL lock_timeout = '2s';"))
        conn.execute(text("SET LOCAL statement_timeout = '5min';"))
        conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = '1min';"))
        conn.execute(text("SET application_name = 'sales_ingestor_eav_prelude';"))

        # Try to take a per-upload advisory *transaction* lock
        got = conn.execute(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:k, 0))"),
            {"k": settings['metadata']['upload_id']}
        ).scalar()

        if not got:
            # Another attempt is doing Prelude for this upload; just skip.
            logger.info("[LOG] Prelude already in progress; skipping.")
            return "skipped"

        # We hold the advisory lock until transaction ends; now do the work.
        conn.execute(stmt)
        logger.info("[LOG] EAV Prelude complete")
        return "ok"
