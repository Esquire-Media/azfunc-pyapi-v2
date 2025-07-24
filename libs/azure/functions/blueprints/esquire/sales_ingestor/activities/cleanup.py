
from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import logging
logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_cleanup(settings: dict):

    table_name = settings['staging_table']

    logger.info(msg=f"[LOG] Cleaning up staging table {qtbl(table_name)}")

    with db() as conn:
        ddl = f"DROP TABLE IF EXISTS {qtbl(table_name)};"
        conn.exec_driver_sql(ddl)
