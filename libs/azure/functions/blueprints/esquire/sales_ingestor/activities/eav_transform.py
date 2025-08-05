
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import uuid
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB, TEXT
import logging
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_eavTransform(settings: dict):

    """
    Runs the one‐shot EAV load. 
    """
    logger.info(msg="[LOG] Transforming into EAV tables")

    staging_table = qtbl(settings['staging_table'])
    fields_map    = settings['fields']
    order_col     = fields_map['order_info']['order_num']

    sql = f"""
    -- 1. Create or get the sales_batch entity
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

    -- Metadata attributes for sales_batch
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
        ON CONFLICT (entity_type_id, name) DO NOTHING
        RETURNING id, name
    ),
    sb_attribute_info AS (
        SELECT a.id AS attribute_id, mf.value
        FROM attributes a
        JOIN metadata_fields mf ON a.name = mf.name
        WHERE a.entity_type_id = (SELECT entity_type_id FROM entity_types WHERE name = 'sales_batch')
    ),
    insert_sales_batch_metadata AS (
        INSERT INTO entity_attribute_values (
            entity_id,
            attribute_id,
            value_string
        )
        SELECT
            (SELECT id FROM sales_batch_entity),
            attribute_id,
            value
        FROM sb_attribute_info
        ON CONFLICT (entity_id, attribute_id) DO UPDATE
        SET value_string = EXCLUDED.value_string
    ),

    -- 2. Create transaction entities
    transaction_data AS (
        SELECT DISTINCT s."{order_col}", gen_random_uuid() AS txn_id
        FROM {staging_table} s
    ),
    inserted_transactions AS (
        INSERT INTO entities (id, entity_type_id, parent_entity_id)
        SELECT
            txn_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'transaction'),
            (SELECT id FROM sales_batch_entity)
        FROM transaction_data
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    ),
    transaction_entities AS (
        SELECT txn_id AS id FROM transaction_data
    ),

    -- 3. Create line items
    line_item_data AS (
        SELECT
            s.*,
            gen_random_uuid() AS line_item_id,
            tx.txn_id AS transaction_entity_id
        FROM {staging_table} s
        JOIN transaction_data tx ON s."{order_col}" = tx."{order_col}"
    ),
    inserted_line_items AS (
        INSERT INTO entities (id, entity_type_id, parent_entity_id)
        SELECT
            line_item_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'line_item'),
            transaction_entity_id
        FROM line_item_data
        ON CONFLICT (id) DO NOTHING
        RETURNING id
    ),
    line_item_entities AS (
        SELECT line_item_id AS id FROM line_item_data
    ),

    -- 4. Attribute classification
    staging_column_types AS (
        SELECT
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = 'sales' AND table_name = '{settings['staging_table']}'
    ),
    flattened_staging AS (
        SELECT
            s."{order_col}",
            col.key AS column_name,
            col.value AS column_value
        FROM {staging_table} s,
        LATERAL jsonb_each_text(to_jsonb(s)) col
    ),
    column_consistency AS (
        SELECT
            column_name,
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
        AND c.table_name = '{settings['staging_table']}'
        AND a.attnum > 0
        AND NOT a.attisdropped
    ),

    -- 5. Generate attribute definitions
    transaction_attributes AS (
        SELECT
            gen_random_uuid() AS id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'transaction'),
            column_name AS name,
            CASE
                WHEN data_type IN ('character varying', 'text') THEN 'string'
                WHEN data_type IN ('numeric', 'integer', 'bigint', 'real', 'double precision') THEN 'numeric'
                WHEN data_type = 'boolean' THEN 'boolean'
                WHEN data_type = 'timestamp with time zone' THEN 'timestamptz'
                WHEN data_type IN ('json', 'jsonb') THEN 'jsonb'
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
                WHEN data_type IN ('character varying', 'text') THEN 'string'
                WHEN data_type IN ('numeric', 'integer', 'bigint', 'real', 'double precision') THEN 'numeric'
                WHEN data_type = 'boolean' THEN 'boolean'
                WHEN data_type = 'timestamp with time zone' THEN 'timestamptz'
                WHEN data_type IN ('json', 'jsonb') THEN 'jsonb'
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
        ON CONFLICT (entity_type_id, name) DO NOTHING
        RETURNING id, name, entity_type_id, data_type
    ),
    attribute_info AS (
        SELECT id AS attribute_id, name AS attribute_name, entity_type_id, data_type
        FROM attributes
    ),

    -- 6. Map headers
    fields_cte AS (
        SELECT :fields AS fields_json
    ),
    fields_mapping AS (
        SELECT key, value
        FROM fields_cte, jsonb_each_text(fields_json)
    ),
    client_header_mappings AS (
        SELECT
            :tenant_id AS tenant_id,
            fm.key AS mapped_header,
            a.id AS attribute_id
        FROM fields_mapping fm
        JOIN attributes a ON a.name = fm.value
    ),
    insert_client_header_map AS (
        INSERT INTO client_header_map (tenant_id, mapped_header, attribute_id)
        SELECT tenant_id, mapped_header, attribute_id
        FROM client_header_mappings
        ON CONFLICT (tenant_id, mapped_header) DO NOTHING
    ),

    -- 7. Unpivot values
    unpivoted_attributes AS (
        SELECT
            CASE
                WHEN ai.entity_type_id = (SELECT entity_type_id FROM entity_types WHERE name = 'transaction')
                THEN lid.transaction_entity_id
                ELSE lid.line_item_id
            END AS entity_id,
            ai.attribute_id,
            ai.data_type,
            row_to_json(lid) ->> ai.attribute_name AS column_value
        FROM line_item_data lid
        JOIN attribute_info ai ON TRUE
    ),
    attribute_values AS (
        SELECT
            entity_id,
            attribute_id,
            data_type,
            column_value
        FROM unpivoted_attributes
    )

    -- 8. Insert final values
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
        CASE WHEN av.data_type = 'numeric' THEN av.column_value::numeric ELSE NULL END,
        CASE WHEN av.data_type = 'boolean' THEN av.column_value::boolean ELSE NULL END,
        CASE WHEN av.data_type = 'timestamptz' THEN av.column_value::timestamptz ELSE NULL END,
        CASE WHEN av.data_type = 'jsonb' THEN av.column_value::jsonb ELSE NULL END
    FROM attribute_values av
    ON CONFLICT (entity_id, attribute_id) DO UPDATE
    SET
        value_string = EXCLUDED.value_string,
        value_numeric = EXCLUDED.value_numeric,
        value_boolean = EXCLUDED.value_boolean,
        value_ts = EXCLUDED.value_ts,
        value_jsonb = EXCLUDED.value_jsonb;


    """

    # bind the things that we can
    # can't bind table or field names, hence the f-string we have above
    stmt = text(sql).bindparams(
        bindparam("upload_id", value=settings['metadata']['upload_id'], type_=PG_UUID),
        bindparam("tenant_id", value=settings['metadata']['tenant_id'], type_=TEXT),
        bindparam("fields", value=flatten_fields(settings["fields"]), type_=JSONB),
        bindparam("metadata", value=flatten_fields(settings["metadata"]), type_=JSONB)
    )

    # set up the pathing and run it
    with db() as conn:
        conn.execute(text("SET search_path TO sales"))
        conn.execute(stmt)


def flatten_fields(d, parent_key='', result=None):
    """
    Utility function for handling headers since I need a flat bit of json for one particular thing

    E.G.:
    {'billing': {
        'street': 'Address1',
        'addr2': '',
        'city': 'City',
        'state': 'State',
        'zipcode': 'PostalCodeID'
        }
    } 
    ->
    {'billing_street': 'Address1',
    'billing_city': 'City',
    'billing_state': 'State',
    'billing_zipcode': 'PostalCodeID'}
    """

    # only append if it's a shipping or billing field, no others should be duplicated
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