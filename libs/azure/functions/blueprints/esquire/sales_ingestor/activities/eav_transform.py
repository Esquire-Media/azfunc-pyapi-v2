
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from sqlalchemy import text
import uuid
from sqlalchemy import text, bindparam
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB, TEXT
import logging
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.WARNING)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_eavTransform(settings: dict):

    """
    Runs the one‐shot EAV load. 
    """
    logger.warning(msg="[LOG] Transforming into EAV tables")

    staging_table = qtbl(settings['staging_table'])
    fields_map    = settings['fields']
    order_col     = fields_map['order_info']['order_num']

    sql = f"""
    -- Create a new sales_batch entity
    WITH sales_batch_entity AS (
        INSERT INTO entities (id, entity_type_id)
        VALUES (
            :upload_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'sales_batch')
        )
        RETURNING id
    ),

    -- Create transaction entities for each unique transaction value
    transaction_data AS (
        SELECT DISTINCT s.{order_col}, gen_random_uuid() AS txn_id
        FROM {staging_table} s
    ),
    insert_transaction_entities AS (
        INSERT INTO entities (id, entity_type_id, parent_entity_id)
        SELECT
            txn_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'transaction'),
            (SELECT id FROM sales_batch_entity)
        FROM transaction_data
        RETURNING id
    ),
    -- Similarly set up the line items
    line_item_data AS (
        SELECT
            s.*,
            gen_random_uuid() AS line_item_id,
            tx.txn_id AS transaction_entity_id
        FROM {staging_table} s
        JOIN transaction_data tx ON s.{order_col} = tx.{order_col}
    ),
    inserted_line_items AS (
        INSERT INTO entities (id, entity_type_id, parent_entity_id)
        SELECT
            line_item_id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'line_item'),
            transaction_entity_id
        FROM line_item_data
        RETURNING id
    ),

    -- Insert attributes if they don't exist
    -- Pull column types from information_schema
    staging_column_types AS (
        SELECT
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = 'sales' AND table_name = '{settings['staging_table']}'
    ),

    -- Determine constancy per column across transactions (are attributes line item or trx?)
    flattened_staging AS (
        SELECT
            s.{order_col},
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
        GROUP BY {order_col}, column_name
    ),
    column_classification AS (
        SELECT
            sc.column_name,
            sc.data_type,
            cc.always_constant
        FROM staging_column_types sc
        JOIN column_consistency cc ON sc.column_name = cc.column_name
    ),

    -- Generate attribute rows for transactions
    transaction_attributes AS (
        SELECT
            gen_random_uuid() AS id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'transaction') AS entity_type_id,
            column_name AS name,
            CASE
                WHEN data_type IN ('character varying', 'text') THEN 'string'
                WHEN data_type IN ('numeric', 'integer', 'bigint', 'real', 'double precision') THEN 'numeric'
                WHEN data_type = 'boolean' THEN 'boolean'
                WHEN data_type = 'timestamp with time zone' THEN 'timestamptz'
                WHEN data_type IN ('json', 'jsonb') THEN 'jsonb'
                ELSE 'string'
            END::attr_data_type AS data_type,
            NULL::text AS description
        FROM column_classification
        WHERE always_constant = TRUE
    ),

    -- Generate attribute rows for line_items
    line_item_attributes AS (
        SELECT
            gen_random_uuid() AS id,
            (SELECT entity_type_id FROM entity_types WHERE name = 'line_item') AS entity_type_id,
            column_name AS name,
            CASE
                WHEN data_type IN ('character varying', 'text') THEN 'string'
                WHEN data_type IN ('numeric', 'integer', 'bigint', 'real', 'double precision') THEN 'numeric'
                WHEN data_type = 'boolean' THEN 'boolean'
                WHEN data_type = 'timestamp with time zone' THEN 'timestamptz'
                WHEN data_type IN ('json', 'jsonb') THEN 'jsonb'
                ELSE 'string'
            END::attr_data_type AS data_type,
            NULL::text AS description
        FROM column_classification
        WHERE always_constant = FALSE
    ),

    -- Insert into attributes
    insert_new_attributes AS (
        INSERT INTO attributes (id, entity_type_id, name, data_type, description)
        SELECT * FROM transaction_attributes
        UNION ALL
        SELECT * FROM line_item_attributes
        ON CONFLICT (entity_type_id, name) DO NOTHING
        RETURNING id, name, entity_type_id, data_type
    ),

    -- Final attribute reference for downstream use
    attribute_info AS (
        SELECT id AS attribute_id, name AS attribute_name, entity_type_id, data_type
        FROM attributes
    ),

    -- Populate client_header_map for attributes specified in settings['fields']
    fields_cte AS (
        SELECT :fields AS fields_json
    ),
    fields_mapping AS (
        SELECT key, value
        FROM fields_cte, jsonb_each_text(fields_cte.fields_json)
    ),
    client_header_mappings AS (
        SELECT
            CAST(:tenant_id AS UUID) AS tenant_id,
            fm.key AS mapped_header,
            a.id AS attribute_id
        FROM fields_mapping fm
        JOIN attributes a ON a.name = fm.value
    ),
    insert_client_header_map AS (
        INSERT INTO client_header_map (tenant_id, mapped_header, attribute_id)
        SELECT tenant_id, mapped_header, attribute_id
        FROM client_header_mappings
        ON CONFLICT DO NOTHING
    ),

    -- Unpivot line_item_data into attribute-value pairs
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

    -- get the last bit of attribute values. and insert into entity_attribute_values
    -- doing this as a subquery broke for some reason so CTE it is!
    attribute_values AS (
        SELECT
            entity_id,
            attribute_id,
            data_type,
            column_value
        FROM unpivoted_attributes
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
        CASE WHEN av.data_type = 'numeric' THEN av.column_value::numeric ELSE NULL END,
        CASE WHEN av.data_type = 'boolean' THEN av.column_value::boolean ELSE NULL END,
        CASE WHEN av.data_type = 'timestamptz' THEN av.column_value::timestamptz ELSE NULL END,
        CASE WHEN av.data_type = 'jsonb' THEN av.column_value::jsonb ELSE NULL END
    FROM attribute_values av;

    """

    # bind the things that we can
    # can't bind table or field names, hence the f-string we have above
    stmt = text(sql).bindparams(
        bindparam("upload_id", value=settings['metadata']['upload_id'], type_=PG_UUID),
        bindparam("tenant_id", value=settings['metadata']['tenant_id'], type_=TEXT),
        bindparam("fields", value=flatten_fields(settings["fields"]), type_=JSONB)
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