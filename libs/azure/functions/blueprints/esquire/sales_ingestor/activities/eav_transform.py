import pandas as pd
from azure.durable_functions import Blueprint, activity_trigger
from sqlalchemy import text
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def eav_transform(settings: dict):
    order = settings["order"]
    table = settings["table"]
    order_col = settings["order_field"]

    with db() as conn:
        df = pd.read_sql(
            text(f'SELECT * FROM "{table}" WHERE "{order_col}" = :o'),
            con=conn, params=dict(o=order)
        )

    eav_rows = [
        (order, col, val)
        for row in df.itertuples(index=False)
        for col, val in row._asdict().items()
        if val not in (None, "")
    ]
    if eav_rows:
        with db() as conn:
            conn.execute(
                text(
                    "INSERT INTO order_eav(order_id, attr, val) "
                    "VALUES (:1, :2, :3)"
                ),
                eav_rows
            )
