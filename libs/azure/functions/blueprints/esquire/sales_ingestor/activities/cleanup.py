
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from sqlalchemy import text
import logging
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_cleanup(settings: dict):

    table_name = settings['staging_table']

    logger.info(msg=f"[LOG] Cleaning up staging table {qtbl(table_name)}")

    with db() as conn:
        # Drop without a trailing semicolon, via SQLAlchemy’s text()
        conn.execute(text(f"DROP TABLE IF EXISTS {qtbl(table_name)};"))
        conn.execute(text(f"""DO $$
                DECLARE
                    r RECORD;
                BEGIN
                    FOR r IN
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = 'sales'
                        AND table_name LIKE '{table_name.split("sales.")[-1]}'
                    LOOP
                        EXECUTE format('DROP TABLE sales.%I;', r.table_name);
                    END LOOP;
                END $$;
                """))
        conn.commit()
