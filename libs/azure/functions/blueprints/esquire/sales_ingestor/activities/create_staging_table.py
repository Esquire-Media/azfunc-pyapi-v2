
from azure.durable_functions import Blueprint
import logging
import os
import pyarrow as pa
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.arrow_ingest import _pg_type
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.blob import _arrow_reader
from azure.storage.blob import BlobClient
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.field_mapping import (
    build_raw_to_standardized_map,
    normalize_fields_to_standardized,
)

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_createStagingTable(settings: dict):
    logger.info(
        msg=f"[LOG] Creating Staging Table {qtbl(settings['table_name'])}",
        extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}},
    )

    blob_path = settings["metadata"]["blob_id"]
    conn_str = os.environ["SALES_INGEST_CONN_STR"]
    chunk_size = 10 * 1024 * 1024
    container = "ingest"

    blob = BlobClient.from_connection_string(
        conn_str,
        container_name=container,
        blob_name=blob_path,
        max_chunk_get_size=chunk_size,
        max_single_get_size=chunk_size,
    )

    reader = _arrow_reader(blob, chunk_size)
    raw_to_standardized = build_raw_to_standardized_map(settings["fields"])
    normalized_fields = normalize_fields_to_standardized(settings["fields"])

    missing_headers = [
        raw_header
        for raw_header in raw_to_standardized.keys()
        if raw_header not in reader.schema.names
    ]
    if missing_headers:
        raise ValueError(
            f"Mapped headers were not found in uploaded file schema: {missing_headers}"
        )

    table_name = settings["table_name"]
    schema = reader.schema
    norm_fields = _normalized_fields_for_ddl(schema)

    with db() as conn:
        conn.exec_driver_sql(f"DROP TABLE IF EXISTS {qtbl(table_name)};")

        cols = []
        for field in norm_fields:
            raw_name = field.name
            standardized_name = raw_to_standardized.get(raw_name, raw_name)
            cols.append(f'"{standardized_name}" {_pg_type(field)}')

        ddl = f"CREATE UNLOGGED TABLE {qtbl(table_name)} ({', '.join(cols)});"
        conn.exec_driver_sql(ddl)

        order_col = normalized_fields["order_info"]["order_num"]
        conn.exec_driver_sql(
            f'CREATE INDEX IF NOT EXISTS idx_{table_name.replace("-","")}_order '
            f'ON {qtbl(table_name)} ("{order_col}");'
        )

    logger.info(
        msg=f"[LOG] Created Staging Table {qtbl(settings['table_name'])}",
        extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}},
    )


def _normalized_fields_for_ddl(schema: pa.Schema):
    """
    For any dictionary-encoded column, use its value_type for DDL mapping.
    Escape identifier names but keep Field objects.
    """
    norm = []
    for field in schema:
        name = _escape_ident(field.name)

        if pa.types.is_dictionary(field.type):
            norm.append(
                pa.field(
                    name,
                    field.type.value_type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        else:
            norm.append(
                pa.field(
                    name,
                    field.type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )

    return norm


def _escape_ident(name: str) -> str:
    return name.replace("%", "%%")