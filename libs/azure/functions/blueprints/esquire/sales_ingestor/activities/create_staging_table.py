
from azure.durable_functions import Blueprint, activity_trigger
import logging
import pyarrow as pa
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.arrow_ingest import _pg_type

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def create_staging_table(settings) -> None:
    table_name = settings['table_name']
    schema = settings['schema']

    with db() as conn:
        cols = [f'"{f.name}" {_pg_type(f)}' for f in schema]
        ddl = f"CREATE TABLE IF NOT EXISTS {qtbl(table_name)} ({', '.join(cols)});"
        conn.exec_driver_sql(ddl)
