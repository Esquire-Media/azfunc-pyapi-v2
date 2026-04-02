from azure.durable_functions import Blueprint
from sqlalchemy import text
import logging

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.field_mapping import (
    normalize_fields_to_standardized,
)

bp = Blueprint()
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_standardizeColumns(settings: dict):
    table_name = settings["table_name"]
    table = qtbl(table_name)

    normalized_fields = normalize_fields_to_standardized(settings["fields"])

    select_clauses = []
    used_raw_cols = set()
    standardized_cols = set()

    # 1. Build SELECT from mapping
    for group_name, group in settings["fields"].items():
        for logical_name, raw_name in group.items():
            std_name = normalized_fields[group_name][logical_name]

            if not std_name:
                continue

            standardized_cols.add(std_name)

            if raw_name and raw_name.strip():
                raw = raw_name.strip()
                used_raw_cols.add(raw)

                select_clauses.append(f'r."{raw}" AS "{std_name}"')
            else:
                select_clauses.append(f'NULL::text AS "{std_name}"')

    # 2. Add passthrough columns (unmapped raw columns)
    with db() as conn:
        existing_cols = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'sales'
                  AND table_name = :table
                ORDER BY ordinal_position
            """),
            {"table": table_name},
        ).scalars().all()

    for col in existing_cols:
        if col not in used_raw_cols and col not in standardized_cols:
            select_clauses.append(f'r."{col}"')

    tmp_table = f"{table_name}_tmp"

    sql = f"""
    DROP TABLE IF EXISTS {qtbl(tmp_table)};

    CREATE UNLOGGED TABLE {qtbl(tmp_table)} AS
    SELECT
        {', '.join(select_clauses)}
    FROM {table} r;

    DROP TABLE {table};
    ALTER TABLE {qtbl(tmp_table)} RENAME TO "{table_name}";
    """

    with db() as conn:
        conn.execute(text("SET search_path TO sales"))
        conn.execute(text(sql))

        order_col = normalized_fields["order_info"]["order_num"]
        conn.exec_driver_sql(
            f'CREATE INDEX IF NOT EXISTS idx_{table_name.replace("-","")}_order '
            f'ON {qtbl(table_name)} ("{order_col}");'
        )

        conn.commit()

    logger.info(f"[LOG] Standardized columns for {table_name}")

    return {}