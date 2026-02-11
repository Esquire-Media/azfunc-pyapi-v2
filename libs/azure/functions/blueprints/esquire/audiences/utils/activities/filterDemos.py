# File: /libs/azure/functions/blueprints/esquire/audiences/utils/activities/filterDemos.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.azure.functions.blueprints.esquire.audiences.builder.activities.fetchAudience import _canonicalize_jsonlogic
from libs.utils.azure_storage import init_blob_client
import os
import csv
from azure.storage.blob import BlobSasPermissions, generate_blob_sas
from datetime import datetime
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiences_filterDemographics(ingress: dict) -> str:
    """
    Durable-safe streaming demographics filter.
    """

    import uuid
    import logging
    from azure.storage.blob import BlobClient

    source_url = ingress["source_url"]
    demo_filter = ingress["demographicsFilter"]
    destination = ingress["destination"]

    # 1. Compile filter
    # use sql as in the rest of audience automation to ensure it's consistent
    # kind of an intermediate, narrower level
    where_sql = jsonlogic_to_sql(
        _canonicalize_jsonlogic(demo_filter)
    )
    # then turn it into pythony dict-handling goodness
    predicate = compile_sql_where_predicate(where_sql)

    # 2. Build destination path
    blob_name = f"{destination['blob_prefix']}/{uuid.uuid4().hex}.csv"

    # 3. Open blob streams
    source_blob = BlobClient.from_blob_url(source_url)
    dest_blob = init_blob_client(
        conn_str=os.environ[destination["conn_str"]],
        container_name=destination["container_name"],
        blob_name=blob_name,
    )


    downloader = source_blob.download_blob()

    reader = csv.DictReader(
        iter_csv_lines_from_blob(downloader)
    )

    def output_generator():
        import io, csv

        buf = io.StringIO()
        writer = csv.writer(buf)

        writer.writerow(["deviceid"])
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)

        for row in reader:
            if predicate(row):
                writer.writerow([row['hashed device id']])
                yield buf.getvalue()
                buf.seek(0); buf.truncate(0)

    dest_blob.upload_blob(
        data=output_generator(),
        overwrite=True,
    )

    sas_token = generate_blob_sas(
        account_name=dest_blob.account_name,
        container_name=dest_blob.container_name,
        blob_name=dest_blob.blob_name,
        account_key=dest_blob.credential.account_key,  # type: ignore[attr-defined]
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    return f"{unquote(dest_blob.url)}?{sas_token}"

def compile_sql_where_predicate(where_sql: str):
    import re

    expr = where_sql
    expr = re.sub(r"\bAND\b", "and", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bOR\b", "or", expr, flags=re.IGNORECASE)

    expr = expr.replace("!=", "!=")
    expr = re.sub(r"(?<![<>=!])=(?!=)", "==", expr)

    # Replace quoted identifiers
    expr = re.sub(
        r'"([^"]+)"',
        lambda m: f'_val(row.get("{m.group(1)}"))',
        expr,
    )

    code = compile(expr, "<demographics-filter>", "eval")

    def _val(v):
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            # normalize '1', '1.0', '0', '0.0'
            if v.replace(".", "", 1).isdigit():
                try:
                    i = int(float(v))
                    return i
                except ValueError:
                    return v
        return v

    def predicate(row: dict) -> bool:
        try:
            return bool(eval(code, {"row": row, "_val": _val}))
        except Exception:
            return False

    return predicate



import codecs
from typing import Iterator

def iter_csv_lines_from_blob(downloader, encoding: str = "utf-8") -> Iterator[str]:
    decoder = codecs.getincrementaldecoder(encoding)()
    buffer = ""

    for chunk in downloader.chunks():
        buffer += decoder.decode(chunk)
        while True:
            newline = buffer.find("\n")
            if newline < 0:
                break
            line = buffer[: newline + 1]
            buffer = buffer[newline + 1 :]
            yield line

    buffer += decoder.decode(b"", final=True)
    if buffer:
        yield buffer