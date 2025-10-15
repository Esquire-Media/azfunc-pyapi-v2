from azure.durable_functions import Blueprint
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, INTEGER as PG_INT
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import logging

bp = Blueprint()
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_eavTransformChunk(settings: dict):
    """
    Chunked EAV transform.
    Processes rows in staging where chunk_id = :chunk_id.
    Prelude must have already created sales_batch + attributes.
    """
    staging_table = qtbl(settings["staging_table"])
    upload_id     = settings["metadata"]["upload_id"]
    chunk_id      = int(settings["chunk_id"])
    order_col     = settings["fields"]["order_info"]["order_num"]


    sql = f"""
    WITH staging_subset AS (
        SELECT * FROM {staging_table} s WHERE s.chunk_id = :chunk_id
    ),

    transaction_data AS (
        SELECT DISTINCT s."{order_col}" AS order_key,
               md5(:upload_id || '|' || s."{order_col}"::text)::uuid AS txn_id
        FROM staging_subset s
    ),
    upsert_transactions AS (
        INSERT INTO entities (id, entity_type_id, parent_entity_id)
        SELECT
            txn_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'transaction'),
            :upload_id
        FROM transaction_data
        ON CONFLICT (id) DO NOTHING
    ),

    line_item_data AS (
        SELECT
            s.*,
            md5(:upload_id || '|' || s."{order_col}"::text || '|' || to_jsonb(s)::text)::uuid AS line_item_id,
            td.txn_id AS transaction_entity_id
        FROM staging_subset s
        JOIN transaction_data td ON s."{order_col}" = td.order_key
    ),
    upsert_line_items AS (
        INSERT INTO entities (id, entity_type_id, parent_entity_id)
        SELECT
            lid.line_item_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'line_item'),
            lid.transaction_entity_id
        FROM line_item_data lid
        ON CONFLICT (id) DO NOTHING
    ),

    attribute_info AS (
    WITH column_types AS (
        SELECT
        c.column_name,
        CASE
            WHEN format_type(a.atttypid, a.atttypmod) IN ('character varying','text') THEN 'string'
            WHEN format_type(a.atttypid, a.atttypmod) IN ('numeric','integer','bigint','real','double precision') THEN 'numeric'
            WHEN format_type(a.atttypid, a.atttypmod) = 'boolean' THEN 'boolean'
            WHEN format_type(a.atttypid, a.atttypmod) LIKE 'timestamp%' THEN 'timestamptz'
            WHEN format_type(a.atttypid, a.atttypmod) IN ('json','jsonb') THEN 'jsonb'
            ELSE 'string'
        END::attr_data_type AS col_attr_type
        FROM information_schema.columns c
        JOIN pg_class cls
        ON cls.relname = '{settings["staging_table"]}'
        AND cls.relnamespace = 'sales'::regnamespace
        JOIN pg_attribute a
        ON a.attrelid = cls.oid
        AND a.attname  = c.column_name
        AND a.attnum > 0 AND NOT a.attisdropped
        WHERE c.table_schema = 'sales'
        AND c.table_name  = '{settings["staging_table"]}'
    )
    SELECT a.id AS attribute_id,
            a.name AS attribute_name,
            a.entity_type_id,
            a.data_type
    FROM attributes a
    JOIN column_types ct
        ON ct.column_name = a.name
    AND ct.col_attr_type = a.data_type
    WHERE a.entity_type_id IN (
        (SELECT entity_type_id FROM entity_types WHERE name = 'transaction'),
        (SELECT entity_type_id FROM entity_types WHERE name = 'line_item')
    )
    ),


    unpivoted_txn AS (
        SELECT
            lid.transaction_entity_id AS entity_id,
            ai.attribute_id,
            ai.data_type,
            MIN(to_jsonb(lid) ->> ai.attribute_name) AS column_value
        FROM line_item_data AS lid
        JOIN attribute_info AS ai
          ON ai.entity_type_id = (SELECT entity_type_id FROM entity_types WHERE name = 'transaction')
         AND to_jsonb(lid) ? ai.attribute_name
        GROUP BY lid.transaction_entity_id, ai.attribute_id, ai.data_type
    ),
    unpivoted_line AS (
        SELECT
            lid.line_item_id AS entity_id,
            ai.attribute_id,
            ai.data_type,
            to_jsonb(lid) ->> ai.attribute_name AS column_value
        FROM line_item_data AS lid
        JOIN attribute_info AS ai
          ON ai.entity_type_id = (SELECT entity_type_id FROM entity_types WHERE name = 'line_item')
         AND to_jsonb(lid) ? ai.attribute_name
    ),
    attribute_values AS (
        SELECT DISTINCT ON (entity_id, attribute_id)
            entity_id, attribute_id, data_type, column_value
        FROM (
            SELECT * FROM unpivoted_txn
            UNION ALL
            SELECT * FROM unpivoted_line
        ) u
        WHERE column_value IS NOT NULL
        ORDER BY entity_id, attribute_id
    )

    INSERT INTO entity_attribute_values (
        entity_id,
        attribute_id,
        value_string,
        value_numeric,
        value_boolean,
        value_ts,
        value_jsonb
    )
    SELECT
        av.entity_id,
        av.attribute_id,
        CASE WHEN av.data_type = 'string' THEN av.column_value ELSE NULL END,
        CASE
            WHEN av.data_type = 'numeric' THEN
                NULLIF(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(av.column_value, '[\\$,]', '', 'g'),
                        '^\\((.*)\\)$', '-\\1'
                    ), ''
                )::numeric
            ELSE NULL
        END,
        CASE
            WHEN av.data_type = 'boolean' AND lower(av.column_value) IN ('true','false')
            THEN lower(av.column_value)::boolean
            ELSE NULL
        END,
        CASE WHEN av.data_type = 'timestamptz' THEN av.column_value::timestamptz ELSE NULL END,
        CASE WHEN av.data_type = 'jsonb' THEN av.column_value::jsonb ELSE NULL END
    FROM attribute_values av
    ON CONFLICT (entity_id, attribute_id) DO UPDATE
    SET value_string = EXCLUDED.value_string,
        value_numeric = EXCLUDED.value_numeric,
        value_boolean = EXCLUDED.value_boolean,
        value_ts     = EXCLUDED.value_ts,
        value_jsonb  = EXCLUDED.value_jsonb;
    """

    with db() as conn:
        # separate execute for search_path
        conn.execute(text("SET search_path TO sales"))

        # timeouts
        conn.execute(text("SET LOCAL lock_timeout = '2s'"))
        conn.execute(text("SET LOCAL statement_timeout = '10min'"))
        conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = '2min'"))
        conn.execute(text("SET application_name = 'sales_ingestor_eav_chunk'"))

        # advisory lock per chunk
        lock_key_expr = f"{upload_id}|{chunk_id}"
        got = conn.execute(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:k, 0))"),
            {"k": lock_key_expr}
        ).scalar()
        if not got:
            logger.info(f"[LOG] Chunk {chunk_id} already processing; skipping.")
            return "skipped"

        stmt = text(sql).bindparams(
            bindparam("upload_id", value=upload_id, type_=PG_UUID),
            bindparam("chunk_id", value=chunk_id, type_=PG_INT)
        )
        conn.execute(stmt)

    logger.info(f"[LOG] EAV chunk {chunk_id} complete.")
    return f"chunk {chunk_id} processed"
