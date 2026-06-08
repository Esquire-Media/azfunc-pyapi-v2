from azure.durable_functions import Blueprint
import pyarrow as pa
import os
import io
import csv
import json
import psycopg
import logging
from azure.storage.blob import BlobClient

from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.arrow_ingest import _pg_type
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.blob import _arrow_reader
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.field_mapping import (
    build_raw_to_standardized_map,
)

logger = logging.getLogger("salesIngestor.logger")
logger.setLevel(logging.INFO)

bp = Blueprint()


@bp.activity_trigger(input_name="settings")
def activity_salesIngestor_streamArrow(settings: dict):
    logger.info(
        msg=f"[LOG] Streaming blob to staging table {qtbl(settings['table_name'])}",
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

    table_name = settings["table_name"]
    conninfo = os.environ["DATABIND_SQL_KEYSTONE"].replace("+psycopg2", "")

    arrow_cols = [f'"{name}"' for name in reader.schema.names]
    cols_list = ", ".join(arrow_cols)
    copy_sql = f"COPY {qtbl(table_name)} ({cols_list}) FROM STDIN (FORMAT CSV)"

    if any(pa.types.is_dictionary(field.type) for field in reader.schema):
        logger.info(
            "[LOG] Detected dictionary-encoded columns; decoding to TEXT during stream.",
            extra={"context": {"PartitionKey": settings["metadata"]["upload_id"]}},
        )

    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as cp:
                for batch in _iter_batches(reader):
                    buf = _copy_buffer(batch)
                    cp.write(buf.read())
    
    return {}


def _copy_buffer(record_batch: pa.RecordBatch) -> io.BytesIO:
    """
    Convert a RecordBatch into a CSV buffer, decoding dictionary arrays to values.
    JSON/JSONB values get json.dumps(...) exactly as before.
    """
    decoded_batch = _decode_dictionary_columns(record_batch)

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")

    norm_fields = _normalized_fields_for_mapping(decoded_batch.schema)
    pg_types = [_pg_type(field) for field in norm_fields]
    col_lists = [col.to_pylist() for col in decoded_batch.columns]

    for i in range(decoded_batch.num_rows):
        row = []
        for val, pg_type in zip((col[i] for col in col_lists), pg_types):
            if val is None:
                row.append("")
            elif pg_type in ("JSON", "JSONB"):
                row.append(json.dumps(val))
            else:
                row.append(val)
        writer.writerow(row)

    buf.seek(0)
    return io.BytesIO(buf.getvalue().encode())


def _iter_batches(reader: pa.RecordBatchReader):
    if hasattr(reader, "__iter__"):
        yield from reader
    else:
        for i in range(reader.num_record_batches):
            yield reader.get_batch(i)


def _normalized_fields_for_mapping(schema: pa.Schema):
    """
    Mirror the DDL normalization: dictionary<...> -> value_type.
    Used to compute pg_types for JSON/JSONB handling during COPY.
    """
    norm = []
    for field in schema:
        if pa.types.is_dictionary(field.type):
            norm.append(
                pa.field(
                    field.name,
                    field.type.value_type,
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
            )
        else:
            norm.append(field)
    return norm


def _decode_dictionary_columns(batch: pa.RecordBatch) -> pa.RecordBatch:
    """
    Return a batch where any dictionary columns are replaced with decoded arrays.
    No-op for non-dictionary columns.
    """
    cols = []
    for arr in batch.columns:
        if pa.types.is_dictionary(arr.type):
            cols.append(arr.dictionary_decode())
        else:
            cols.append(arr)

    return pa.record_batch(cols, names=[field.name for field in batch.schema])