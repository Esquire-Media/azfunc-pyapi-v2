from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from sqlalchemy import text
import logging

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_cleanup(settings: dict):
    table_name = settings['staging_table']  # e.g. staging_<uuid>

    logger.info(msg=f"[LOG] Cleaning up staging table {qtbl(table_name)}")

    with db() as conn:
        # Short timeouts so we don't hang on a lingering lock
        conn.execute(text("SET LOCAL lock_timeout = '2s';"))
        conn.execute(text("SET LOCAL statement_timeout = '30s';"))

        # 1) Drop the exact, schema-qualified table first (quotes handled by qtbl)
        conn.execute(text(f"DROP TABLE IF EXISTS {qtbl(table_name)};"))
        
        # 2) Safety net: drop any sibling tables in `sales` that match the same name
        rows = conn.execute(
            text("""
            SELECT schemaname, tablename
              FROM pg_tables
             WHERE schemaname = 'sales'
               AND (tablename = :tname OR tablename LIKE :tname_like)
        """), {
            "tname": table_name, 
            "tname_like": f"{table_name}%"
            }).mappings().all()

        # Drop each found table with identifier-quoting
        for r in rows:
            fq = f"\"{r['schemaname']}\".\"{r['tablename']}\""
            try:
                logger.info(msg=f"[LOG] Dropping residual table {fq}")
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {fq} CASCADE;")
            except Exception as e:
                logger.warning(msg=f"[WARN] Failed to drop table {fq}: {str(e)}")


        # 3) Optional: also try current search_path (harmless if not present)
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS \"{table_name}\";")

        conn.commit()

