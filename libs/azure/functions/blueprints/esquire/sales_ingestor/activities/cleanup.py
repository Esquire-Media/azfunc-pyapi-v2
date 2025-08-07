
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
        conn = conn.execution_options(isolation_level="AUTOCOMMIT")
        # Drop without a trailing semicolon, via SQLAlchemy’s text()
        conn.execute(text(f"DROP TABLE IF EXISTS {qtbl(table_name)}"))