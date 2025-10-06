
from azure.durable_functions import Blueprint
import logging
import os
import pyarrow as pa
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.arrow_ingest import _pg_type
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.blob import  _arrow_reader
from azure.storage.blob import BlobClient

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_createStagingTable(settings: dict):
    logger.info(msg=f"[LOG] Creating Staging Table {qtbl(settings['table_name'])}")

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

    # NEW: normalize dictionary<...> to value types for stable TEXT DDL
    norm_fields = _normalized_fields_for_ddl(schema)

    with db() as conn:
        # drop in case it was left over
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {qtbl(table_name)};")

        cols = [f'"{f.name}" {_pg_type(f)}' for f in norm_fields]
        ddl = f"CREATE TABLE {qtbl(table_name)} ({', '.join(cols)});"
        conn.exec_driver_sql(ddl)

    logger.info(msg=f"[LOG] Created Staging Table {qtbl(settings['table_name'])}")

def _normalized_fields_for_ddl(schema: pa.Schema):
    """
    For any dictionary-encoded column, use its value_type for DDL mapping.
    This keeps dictionary<string> columns as TEXT in Postgres.
    """
    norm = []
    for f in schema:
        if pa.types.is_dictionary(f.type):
            # preserve name/nullability/metadata; swap type for value_type
            norm.append(pa.field(f.name, f.type.value_type, nullable=f.nullable, metadata=f.metadata))
        else:
            norm.append(f)
    return norm