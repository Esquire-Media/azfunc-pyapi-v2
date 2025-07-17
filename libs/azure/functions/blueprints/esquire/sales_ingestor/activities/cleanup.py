
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import logging
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.WARNING)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_cleanup(settings: dict):
    logger.warning(msg="[LOG] Cleaning up staging table")

    table_name = settings['table_name']

    with db() as conn:
        ddl = f"DROP TABLE IF EXISTS {qtbl(table_name)};"
        conn.exec_driver_sql(ddl)
