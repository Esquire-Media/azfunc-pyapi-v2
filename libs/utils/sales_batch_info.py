
from pathlib import Path
from typing import Any
import os

import orjson
import pandas as pd
from sqlalchemy import create_engine, text

def load_local_settings(path: str | os.PathLike[str] = "local.settings.json") -> dict[str, Any]:
    settings_path = Path(path)
    if not settings_path.is_file():
        raise ConfigError(f"Settings file not found: {settings_path}")

    try:
        payload = orjson.loads(settings_path.read_bytes())
    except Exception as exc:
        raise ConfigError(f"Invalid JSON in settings file: {settings_path}") from exc

    values = payload.get("Values")
    if not isinstance(values, dict):
        raise ConfigError("local.settings.json must contain a top-level 'Values' object.")

    os.environ.update({k: str(v) for k, v in values.items()})
    return values

class ConfigError(RuntimeError):
    pass


def get_sales_batch_mappings(
    tenant_id: str,
    query_type: str,
) -> pd.DataFrame:
    engine = create_engine(
        os.environ["DATABIND_SQL_KEYSTONE"],
        pool_pre_ping=True,
    )

    queries = {
        "distinct_mappings": """
            WITH batch AS (
                SELECT DISTINCT
                    e.id AS batch_id
                FROM sales.entities e
                JOIN sales.entity_attribute_values eav
                    ON eav.entity_id = e.id
                JOIN sales.attributes a
                    ON a.id = eav.attribute_id
                LEFT JOIN sales.client_header_map chm
                    ON chm.attribute_id = a.id
                   AND chm.tenant_id = :tenant_id
                WHERE e.entity_type_id = (
                    SELECT entity_type_id
                    FROM sales.entity_types
                    WHERE name = 'sales_batch'
                )
                  AND COALESCE(chm.mapped_header, a.name) = 'tenant_id'
                  AND eav.value_string = :tenant_id
            )

            SELECT DISTINCT
                sbhm.original_attribute_name,
                a.name AS mapped_attribute_name
            FROM batch b
            JOIN sales.sales_batch_header_map sbhm
                ON sbhm.sales_batch_id = b.batch_id
            JOIN sales.attributes a
                ON a.id = sbhm.standardized_attribute_id
            ORDER BY
                a.name,
                sbhm.original_attribute_name;
        """,

        "batch_metdata_mappings": """
            DO $$
            DECLARE
                v_tenant_id text := :tenant_id;
                v_columns   text;
                v_sql       text;
            BEGIN

                WITH batch AS (
                    SELECT DISTINCT
                        e.id AS batch_id
                    FROM sales.entities e
                    JOIN sales.entity_attribute_values eav
                        ON eav.entity_id = e.id
                    JOIN sales.attributes a
                        ON a.id = eav.attribute_id
                    LEFT JOIN sales.client_header_map chm
                        ON chm.attribute_id = a.id
                       AND chm.tenant_id = v_tenant_id
                    WHERE e.entity_type_id = (
                        SELECT entity_type_id
                        FROM sales.entity_types
                        WHERE name = 'sales_batch'
                    )
                      AND COALESCE(chm.mapped_header, a.name) = 'tenant_id'
                      AND eav.value_string = v_tenant_id
                ),
                metadata_attributes AS (
                    SELECT DISTINCT
                        COALESCE(chm.mapped_header, a.name) AS attribute_name
                    FROM batch b
                    JOIN sales.entity_attribute_values eav
                        ON eav.entity_id = b.batch_id
                    JOIN sales.attributes a
                        ON a.id = eav.attribute_id
                    LEFT JOIN sales.client_header_map chm
                        ON chm.attribute_id = a.id
                       AND chm.tenant_id = v_tenant_id
                )
                SELECT string_agg(
                    format(
                        'MAX(CASE WHEN metadata_name = %L THEN metadata_value END) AS %I',
                        attribute_name,
                        attribute_name
                    ),
                    E',\\n        '
                    ORDER BY attribute_name
                )
                INTO v_columns
                FROM metadata_attributes;


                v_sql := format($query$

                    DROP TABLE IF EXISTS temp_sales_batch_mappings;

                    CREATE TEMP TABLE temp_sales_batch_mappings AS

                    WITH batch AS (
                        SELECT DISTINCT
                            e.id AS batch_id
                        FROM sales.entities e
                        JOIN sales.entity_attribute_values eav
                            ON eav.entity_id = e.id
                        JOIN sales.attributes a
                            ON a.id = eav.attribute_id
                        LEFT JOIN sales.client_header_map chm
                            ON chm.attribute_id = a.id
                           AND chm.tenant_id = %L
                        WHERE e.entity_type_id = (
                            SELECT entity_type_id
                            FROM sales.entity_types
                            WHERE name = 'sales_batch'
                        )
                          AND COALESCE(chm.mapped_header, a.name) = 'tenant_id'
                          AND eav.value_string = %L
                    ),

                    batch_metadata AS (
                        SELECT
                            b.batch_id,
                            COALESCE(chm.mapped_header, a.name) AS metadata_name,
                            COALESCE(
                                eav.value_string,
                                eav.value_numeric::text,
                                eav.value_ts::text,
                                eav.value_boolean::text
                            ) AS metadata_value
                        FROM batch b
                        JOIN sales.entity_attribute_values eav
                            ON eav.entity_id = b.batch_id
                        JOIN sales.attributes a
                            ON a.id = eav.attribute_id
                        LEFT JOIN sales.client_header_map chm
                            ON chm.attribute_id = a.id
                           AND chm.tenant_id = %L
                    ),

                    metadata_wide AS (
                        SELECT
                            batch_id,
                            %s
                        FROM batch_metadata
                        GROUP BY batch_id
                    )

                    SELECT
                        sbhm.sales_batch_id,
                        mw.*,
                        sbhm.original_attribute_name,
                        a.name AS mapped_attribute_name
                    FROM sales.sales_batch_header_map sbhm
                    JOIN batch b
                        ON b.batch_id = sbhm.sales_batch_id
                    JOIN metadata_wide mw
                        ON mw.batch_id = sbhm.sales_batch_id
                    JOIN sales.attributes a
                        ON a.id = sbhm.standardized_attribute_id
                    ORDER BY
                        sbhm.sales_batch_id,
                        sbhm.original_attribute_name;

                $query$,
                    v_tenant_id,
                    v_tenant_id,
                    v_tenant_id,
                    v_columns
                );

                EXECUTE v_sql;

            END $$;

            SELECT *
            FROM temp_sales_batch_mappings;
        """,

        "batch_metadata_mappings_simple": """
            WITH batch AS (
                SELECT DISTINCT
                    e.id AS batch_id
                FROM sales.entities e
                JOIN sales.entity_attribute_values eav
                    ON eav.entity_id = e.id
                JOIN sales.attributes a
                    ON a.id = eav.attribute_id
                LEFT JOIN sales.client_header_map chm
                    ON chm.attribute_id = a.id
                   AND chm.tenant_id = :tenant_id
                WHERE e.entity_type_id = (
                    SELECT entity_type_id
                    FROM sales.entity_types
                    WHERE name = 'sales_batch'
                )
                  AND COALESCE(chm.mapped_header, a.name) = 'tenant_id'
                  AND eav.value_string = :tenant_id
            ),

            batch_metadata AS (
                SELECT
                    b.batch_id,
                    COALESCE(chm.mapped_header, a.name) AS attribute_name,
                    a.id AS attribute_id,
                    eav.value_string,
                    eav.value_numeric,
                    eav.value_ts,
                    eav.value_boolean
                FROM batch b
                JOIN sales.entity_attribute_values eav
                    ON eav.entity_id = b.batch_id
                JOIN sales.attributes a
                    ON a.id = eav.attribute_id
                LEFT JOIN sales.client_header_map chm
                    ON chm.attribute_id = a.id
                   AND chm.tenant_id = :tenant_id
            ),

            metadata_wide AS (
                SELECT
                    batch_id,
                    jsonb_object_agg(
                        attribute_name,
                        COALESCE(
                            to_jsonb(value_string),
                            to_jsonb(value_numeric),
                            to_jsonb(value_ts),
                            to_jsonb(value_boolean)
                        )
                    ) AS metadata
                FROM batch_metadata
                GROUP BY batch_id
            )

            SELECT
                sbhm.sales_batch_id,
                mw.metadata,
                sbhm.original_attribute_name,
                a.name AS mapped_attribute_name,
                a.id AS mapped_attribute_id
            FROM sales.sales_batch_header_map sbhm
            JOIN batch b
                ON b.batch_id = sbhm.sales_batch_id
            JOIN metadata_wide mw
                ON mw.batch_id = sbhm.sales_batch_id
            JOIN sales.attributes a
                ON a.id = sbhm.standardized_attribute_id
            ORDER BY
                sbhm.sales_batch_id,
                sbhm.original_attribute_name;
        """,
    }

    if query_type not in queries:
        raise ValueError(
            f"Invalid query_type {query_type!r}. "
            f"Expected one of: {', '.join(queries)}"
        )

    with engine.connect() as connection:
        return pd.read_sql_query(
            text(queries[query_type]),
            connection,
            params={"tenant_id": tenant_id},
        )