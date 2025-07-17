
from azure.durable_functions import Blueprint
import pyarrow as pa
import os
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.arrow_ingest import _pg_type
import io
import csv
import psycopg
import pyarrow as pa
import json

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def stream_arrow(settings: dict):
    
    table_name = settings['table_name']
    reader = settings['reader']
    conninfo = os.environ['DATABIND_SQL_KEYSTONE_DEV'].replace("+psycopg2", "")

    copy_sql = f"COPY {qtbl(table_name)} FROM STDIN (FORMAT CSV)"
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as cp:
                for batch in _iter_batches(reader):
                    buf = _copy_buffer(batch)
                    cp.write(buf.read())


def _copy_buffer(record_batch: pa.RecordBatch) -> io.BytesIO:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")

    schema = record_batch.schema
    columns = record_batch.columns
    pg_types = [_pg_type(f) for f in schema]

    # Convert all columns to pylist up front
    col_lists = [c.to_pylist() for c in columns]

    for i in range(record_batch.num_rows):
        row = []
        for val, pg_type in zip((col[i] for col in col_lists), pg_types):
            if val is None:
                row.append("")
            elif pg_type in ("JSON", "JSONB"):
                row.append(json.dumps(val))  # Ensures valid JSON encoding
            else:
                row.append(val)
        writer.writerow(row)

    buf.seek(0)
    return io.BytesIO(buf.getvalue().encode())


def _iter_batches(reader: pa.RecordBatchReader):
    """Stream batches no matter the concrete reader."""
    if hasattr(reader, "__iter__"):          # StreamReader
        yield from reader
    else:                                    # FileReader
        for i in range(reader.num_record_batches):
            yield reader.get_batch(i)


