
from azure.durable_functions import Blueprint
import logging
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from sqlalchemy import text, bindparam


logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_intermediate_processing(settings: dict):

    logger.info(msg="[LOG] Intermediate Cleanup")

    staging_table = qtbl(settings['staging_table'])
    fields_map    = settings['fields']
    order_col     = fields_map['order_info']['order_num']

    # Remove rows where there is no order number
    # happens sometimes with fill fields
    stmt = text(f"""
        WITH deleted AS (
            DELETE FROM {staging_table}
            WHERE "{order_col}" IS NULL OR TRIM("{order_col}") = ''
            RETURNING *
        )
        SELECT COUNT(*) FROM deleted;
    """)

    with db() as conn:
        conn.execute(text("SET search_path TO sales"))
        deleted_count = conn.execute(stmt).scalar()
        if deleted_count > 0:
            logger.info(f"[LOG] Deleted {deleted_count} rows with empty {order_col}")