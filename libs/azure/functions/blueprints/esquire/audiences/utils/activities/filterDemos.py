# File: /libs/azure/functions/blueprints/esquire/audiences/utils/activities/filterDemos.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.azure.functions.blueprints.esquire.audiences.builder.activities.fetchAudience import _canonicalize_jsonlogic
from libs.utils.azure_storage import init_blob_client
import os

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
    demo_filter = ingress["demoDataFilterRaw"]
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

    with source_blob.download_blob().open() as src, \
         dest_blob.upload_blob(overwrite=True).open() as dst:

        count = stream_filter_demographics_csv(
            src,
            dst,
            predicate,
        )

    logging.info(
        "[DEMOS FILTER] completed",
        extra={
            "matched": count,
            "output": blob_name,
        },
    )

    return (
        f"https://{destination['container_name']}.blob.core.windows.net/"
        f"{blob_name}"
    )

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



def stream_filter_demographics_csv(
    source_stream,
    output_stream,
    where_predicate,
    *,
    flush_every: int = 5000,
) -> int:
    """
    Streams a demographics CSV and writes matching device_ids to output_stream.

    Parameters
    ----------
    source_stream : file-like
        Opened input CSV stream
    output_stream : file-like
        Opened writable output stream
    where_predicate : Callable[[dict], bool]
        Compiled predicate applied to each CSV row
    flush_every : int
        Flush output every N rows

    Returns
    -------
    int
        Number of matching device_ids written
    """

    import csv

    reader = csv.DictReader(source_stream)
    writer = csv.writer(output_stream)

    # Write header
    writer.writerow(["deviceid"])

    matched = 0
    buffered = 0

    for row in reader:
        try:
            if where_predicate(row):
                writer.writerow([row["device_id"]])
                matched += 1
                buffered += 1

                if buffered >= flush_every:
                    output_stream.flush()
                    buffered = 0
        except Exception:
            # defensive: skip malformed rows
            continue

    if buffered:
        output_stream.flush()

    return matched
