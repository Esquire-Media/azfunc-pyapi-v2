
from azure.durable_functions import Blueprint, activity_trigger
import pyarrow as pa
import os
from libs.azure.functions.blueprints.esquire.sales_ingestor.utility.db import db, qtbl
import io
import csv
import psycopg
import pyarrow as pa

bp = Blueprint()

@bp.activity_trigger(input_name="settings")
def activity_copy_binary(settings):
    
    table_name = settings['table_name']
    reader = settings['reader']
    conninfo = os.environ['DATABIND_SQL_KEYSTONE_DEV'].replace("+psycopg2", "")

    copy_sql = f"COPY {qtbl(table_name)} FROM STDIN (FORMAT BINARY)"
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as cp:
                for batch in _iter_batches(reader):
                    for row in _rows(batch):
                        cp.write_row(row)


def _copy_buffer(record_batch: pa.RecordBatch) -> io.BytesIO:
    """Arrow RecordBatch → in-RAM CSV buffer ready for COPY FROM STDIN."""
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    cols = [c.to_pylist() for c in record_batch.columns]
    for i in range(record_batch.num_rows):
        writer.writerow(col[i] for col in cols)
    buf.seek(0)
    return io.BytesIO(buf.getvalue().encode())   # psycopg2 wants bytes


def activity_copy_batches(conn, table_name: str, reader: pa.RecordBatchReader):
    cursor = conn.connection.cursor()            # raw psycopg2 cursor
    for batch in _iter_batches(reader):
        if not batch.num_rows:
            continue
        buf = _copy_buffer(batch)
        cursor.copy_expert(
            f"COPY {qtbl(table_name)} FROM STDIN WITH (FORMAT csv)",
            file=buf
        )

def _iter_batches(reader: pa.RecordBatchReader):
    """Stream batches no matter the concrete reader."""
    if hasattr(reader, "__iter__"):          # StreamReader
        yield from reader
    else:                                    # FileReader
        for i in range(reader.num_record_batches):
            yield reader.get_batch(i)

def _rows(batch: pa.RecordBatch):
    cols = [c.to_pylist() for c in batch.columns]
    for i in range(batch.num_rows):
        yield tuple(col[i] for col in cols)

