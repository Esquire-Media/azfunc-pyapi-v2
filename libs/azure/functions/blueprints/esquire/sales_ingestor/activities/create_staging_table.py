
from azure.durable_functions import Blueprint
import logging
import os
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.arrow_ingest import _pg_type
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.blob import  _arrow_reader
from azure.storage.blob import BlobClient

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_createStagingTable(settings: dict):
    logger.info(msg="[LOG] Creating Staging Table")

    blob_path = settings['metadata']['upload_id']
    conn_str = os.environ['SALES_INGEST_CONN_STR']
    chunk_size = 10 * 1024 * 1024
    container = 'ingest'

    blob = BlobClient.from_connection_string(
        conn_str,
        container_name=container,
        blob_name=blob_path,
        max_chunk_get_size=chunk_size,
        max_single_get_size=chunk_size,
    )

    reader = _arrow_reader(blob, chunk_size)

    table_name = settings['table_name']
    schema = reader.schema

    with db() as conn:
        # drop in case if was left over
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {qtbl(table_name)};")

        cols = [f'"{f.name}" {_pg_type(f)}' for f in schema]
        ddl = f"CREATE TABLE {qtbl(table_name)} ({', '.join(cols)});"
        conn.exec_driver_sql(ddl)
