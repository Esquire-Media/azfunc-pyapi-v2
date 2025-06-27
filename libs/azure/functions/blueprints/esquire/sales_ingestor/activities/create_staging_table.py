import logging, pyarrow as pa
from azure.storage.blob import BlobClient
from azure.durable_functions import Blueprint, activity_trigger
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.type_map import PG_TYPES

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def create_staging_table(settings: dict):
    """DDL for the temp staging table that matches the Arrow schema."""
    blob = BlobClient.from_blob_url(settings["blob_url"])
    with pa.ipc.open_file(blob.download_blob()) as rdr:
        cols = ",\n".join(
            f'"{f.name}" {PG_TYPES.get(str(f.type), "text")}'
            for f in rdr.schema
        )
    ddl = f'CREATE TEMP TABLE "{settings["table"]}" (\n{cols}\n);'
    with db() as conn:
        conn.exec_driver_sql(ddl)
    logging.info("Created staging table %s", settings["table"])
