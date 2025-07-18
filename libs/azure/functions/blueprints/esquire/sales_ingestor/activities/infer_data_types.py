
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import pandas as pd
from sqlalchemy import text
import logging
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.WARNING)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_inferDataTypes(settings: dict):

    table_name = settings['table_name']

    logger.warning(msg=f"[LOG] Inferring data types for staging table {qtbl(table_name)}")

    with db() as conn:
        # get the inferred types 
        inferred_types = infer_schema_to_df(conn, table_name)

        # and remove excessive ones, converting to a dict
        inferred_types_dict = cleanup_inferred_schema(
            settings,
            inferred_types
            )

        # make all our alters
        alter_statements = generate_alter_table_sql(
            table_name = qtbl(table_name),
            inferred_schema = inferred_types_dict
        )

        # actually run the alters
        apply_alter_statements(conn, alter_statements)

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
                        COUNT(*) FILTER (WHERE (%1$I)::TEXT ~ '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$')::FLOAT AS datetime_matches
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

def generate_alter_table_sql(table_name: str, inferred_schema: dict) -> list:
    return [
        f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE {pg_type} USING {col}::{pg_type};"
        for col, pg_type in inferred_schema.items()
        if pg_type != 'TEXT'
    ]
