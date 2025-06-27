from azure.durable_functions import Blueprint, activity_trigger
from sqlalchemy import text
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def get_distinct_addresses(settings: dict) -> list[str]:
    sql = text(
        f'SELECT DISTINCT "{settings["address_field"]}" FROM "{settings["table"]}"'
    )
    with db() as conn:
        return [row[0] for row in conn.execute(sql)]
