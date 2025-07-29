
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import pandas as pd
from sqlalchemy import text
import logging
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_inferDataTypes(settings: dict):

    table_name = settings['table_name']

    logger.info(msg=f"[LOG] Inferring data types for staging table {qtbl(table_name)}")

    with db() as conn:
        # get the inferred types 
        inferred_types = infer_schema_to_df(conn, table_name)

        # and remove excessive ones, converting to a dict
        inferred_types_dict = cleanup_inferred_schema(
            settings,
            inferred_types
            )
        
        date_types = {"DATE", "TIMESTAMP", "DATETIME"}
        if not any(dtype.upper() in date_types for dtype in inferred_types_dict.values()):
            logger.error(msg=f"[LOG] No date fields were able to be inferred.")

            raise TypeError("No date fields were able to be inferred.")

        # make all our alters
        alter_statements = generate_alter_statements(
            table_name = qtbl(table_name),
            inferred_schema = inferred_types_dict,
            settings=settings
        )

        # actually run the alters
        apply_alter_statements(conn, alter_statements)

        logger.info(msg=f"Field types inferred: {inferred_types_dict}")

def apply_alter_statements(conn, alter_statements: list):
    for stmt in alter_statements:
        conn.execute(text(stmt))

def cleanup_inferred_schema(settings, inferred_types):
    return inferred_types[
                (inferred_types['suggested_type'] != 'TEXT') &
                (~inferred_types['column_name'].isin([
                    settings['fields']['billing']['zipcode'],
                    settings['fields']['shipping']['zipcode'],
                    settings['fields']['order_info']['sale_date']
                ]))
            ].set_index('column_name')['suggested_type'].to_dict()

def infer_schema_to_df(conn, staging_table: str) -> pd.DataFrame:
    # 1. Format the SQL with your target table
    sql = f"""
    DROP TABLE IF EXISTS tmp_type_inference_results;

    CREATE TEMP TABLE tmp_type_inference_results (
        column_name TEXT,
        total_rows BIGINT,
        null_count BIGINT,
        non_null_count BIGINT,
        bool_ratio FLOAT,
        int_ratio FLOAT,
        float_ratio FLOAT,
        datetime_ratio FLOAT,
        suggested_type TEXT
    );

    DO $$
    DECLARE
        r RECORD;
        dyn_sql TEXT;
    BEGIN
        FOR r IN
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'sales'
              AND table_name = '{staging_table}'
        LOOP
            dyn_sql := format($f$
                INSERT INTO tmp_type_inference_results
                WITH base AS (
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(*) FILTER (WHERE (%1$I)::TEXT IS NULL OR (%1$I)::TEXT = '') AS null_count,
                        COUNT(*) FILTER (WHERE (%1$I)::TEXT IS NOT NULL AND (%1$I)::TEXT <> '') AS non_null_count,
                        COUNT(*) FILTER (WHERE LOWER((%1$I)::TEXT) IN ('true','false','0','1'))::FLOAT AS bool_matches,
                        COUNT(*) FILTER (WHERE (%1$I)::TEXT ~ '^[+-]?\d+$')::FLOAT AS int_matches,
                        COUNT(*) FILTER (WHERE (%1$I)::TEXT ~ '^[+-]?(\d+\.\d*|\.\d+)([eE][+-]?\d+)?$')::FLOAT AS float_matches,
                        COUNT(*) FILTER (WHERE (%1$I)::TEXT ~ '^\d{4}-\d{2}-\d{2}$')::FLOAT AS datetime_matches
                    FROM sales."{staging_table}"
                )
                SELECT
                    '%1$s' AS column_name,
                    total_rows,
                    null_count,
                    non_null_count,
                    bool_matches / GREATEST(non_null_count, 1) AS bool_ratio,
                    int_matches / GREATEST(non_null_count, 1) AS int_ratio,
                    float_matches / GREATEST(non_null_count, 1) AS float_ratio,
                    datetime_matches / GREATEST(non_null_count, 1) AS datetime_ratio,
                    CASE
                        WHEN bool_matches = non_null_count THEN 'BOOLEAN'
                        WHEN int_matches = non_null_count THEN 'INTEGER'
                        WHEN int_matches + float_matches = non_null_count THEN 'NUMERIC'
                        WHEN datetime_matches = non_null_count THEN 'TIMESTAMP'
                        ELSE 'TEXT'
                    END AS suggested_type
                FROM base;
            $f$, r.column_name);

            EXECUTE dyn_sql;
        END LOOP;
    END $$;
    """

    # 2. Execute the DO block
    conn.execute(text(sql))

    # 3. Fetch results into DataFrame
    return pd.read_sql("SELECT column_name, suggested_type FROM tmp_type_inference_results", conn)

def generate_alter_statements(inferred_schema: dict, table_name: str, settings: dict = None):
    billing_zip     = settings['fields']['billing']['zipcode']
    shipping_zip    = settings['fields']['shipping']['zipcode']
    sale_date       = settings['fields']['order_info']['sale_date']
    order_number    = settings['fields']['order_info']['order_num']

    alter_statements = []

    for col, pg_type in inferred_schema.items():
        # Always skip if still TEXT
        if pg_type == 'TEXT':
            continue

        # Force zip codes to remain TEXT
        if col in {billing_zip, shipping_zip} and pg_type in {'INTEGER', 'NUMERIC'}:
            continue

        # Sale date can only become TIMESTAMP
        if col == sale_date and pg_type != 'TIMESTAMP':
            continue

        if col == order_number:
            continue

        stmt = (
            f'ALTER TABLE {table_name} '
            f'ALTER COLUMN "{col}" TYPE {pg_type} '
            f'USING "{col}"::{pg_type};'
        )
        alter_statements.append(stmt)

    return alter_statements


