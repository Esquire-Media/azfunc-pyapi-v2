import csv
import logging
import os
import tempfile
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, Optional, Set

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    DelimitedTextDialect,
    generate_blob_sas,
)
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote

bp = Blueprint()

# Constant for the "anonymous" UUID you want to ignore
_ANONYMOUS_UUID = "00000000-0000-0000-0000-000000000000"


def _get_input_blob(source: Any) -> BlobClient:
    """
    Build a BlobClient for the input blob based on the 'source' entry in ingress.
    'source' can be a blob URL string or a dict with connection info.
    """
    if isinstance(source, str):
        return BlobClient.from_blob_url(source)

    return BlobClient.from_connection_string(
        conn_str=os.environ[source["conn_str"]],
        container_name=source["container_name"],
        blob_name=source["blob_name"],
    )


def _get_output_blob(ingress: Dict[str, Any], input_blob: BlobClient) -> BlobClient:
    """
    Build a BlobClient for the destination blob based on the 'destination'
    entry in ingress and the input blob name.
    """
    destination = ingress["destination"]
    blob_name = "{}/{}".format(
        destination["blob_prefix"],
        os.path.basename(input_blob.blob_name),
    )

    return BlobClient.from_connection_string(
        conn_str=os.environ[destination["conn_str"]],
        container_name=destination["container_name"],
        blob_name=blob_name,
    )


def _determine_device_column(
    input_blob: BlobClient,
    dialect: DelimitedTextDialect,
) -> str:
    """
    Use blob query to infer which column holds the device ID.

    Prefers an exact 'deviceid' header (case-insensitive),
    otherwise falls back to the first header containing 'device'.
    """
    query_response = input_blob.query_blob(
        "SELECT * FROM BlobStorage",
        blob_format=dialect,
    )

    # Ensure we have an iterator for next()
    records_iter: Iterator[bytes] = iter(query_response.records())

    try:
        header_bytes = next(records_iter)
    except StopIteration:
        raise ValueError("Source blob is empty; cannot determine device column.")

    header_line = header_bytes.decode("utf-8").strip("\r\n")

    # Split raw header for compatibility with existing behavior.
    # Column names typically won't contain the delimiter.
    raw_columns = header_line.split(dialect.delimiter)

    def _normalize(col: str) -> str:
        return col.strip().strip('"').lower()

    # First try exact 'deviceid'
    for col in raw_columns:
        if _normalize(col) == "deviceid":
            return col

    # Then any column containing 'device'
    for col in raw_columns:
        if "device" in _normalize(col):
            return col

    raise ValueError(
        f"Unable to determine device column. Headers found: {raw_columns!r}"
    )


def _parse_single_value(
    record_bytes: bytes,
    dialect: DelimitedTextDialect,
) -> str:
    """
    Parse a single-column CSV record (result of SELECT <column>) and return its value.
    """
    text = record_bytes.decode("utf-8")
    reader = csv.reader(
        [text],
        delimiter=dialect.delimiter,
        quotechar=dialect.quotechar,
    )
    row = next(reader, [])
    if not row:
        return ""
    return row[0]


def _iter_clean_device_ids(
    input_blob: BlobClient,
    dialect: DelimitedTextDialect,
    device_column: str,
) -> Iterable[str]:
    """
    Stream device IDs from the blob query, cleaning, filtering, and de-duplicating
    on the fly. Yields lowercased, valid, unique device IDs.
    """
    query = input_blob.query_blob(
        f"SELECT {device_column} FROM BlobStorage",
        blob_format=dialect,
    )

    # Ensure we have an iterator for next()
    records_iter: Iterator[bytes] = iter(query.records())

    seen: Set[str] = set()

    def normalize_and_filter(raw: str) -> Optional[str]:
        deviceid = raw.strip().strip('"').lower()
        if not deviceid:
            return None
        # UUID-only: must be 36 chars and not anonymous UUID
        if len(deviceid) != 36:
            return None
        if deviceid == _ANONYMOUS_UUID:
            return None
        if deviceid in seen:
            return None
        seen.add(deviceid)
        return deviceid

    try:
        first_record = next(records_iter)
    except StopIteration:
        # No data rows at all
        return

    # Detect whether the first record is a header (single column containing 'device')
    first_value = _parse_single_value(first_record, dialect)
    if "device" not in first_value.strip().strip('"').lower():
        # First row is actual data, not header
        maybe_device = normalize_and_filter(first_value)
        if maybe_device:
            yield maybe_device

    # Process remaining records as data
    for record in records_iter:
        raw_value = _parse_single_value(record, dialect)
        maybe_device = normalize_and_filter(raw_value)
        if maybe_device:
            yield maybe_device


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_finalize(ingress: Dict[str, Any]) -> str:
    """
    Finalizes the audience data by streaming, filtering, and renaming device IDs.

    This activity:
    - Reads the source blob via query, projecting only the device ID column.
    - Streams rows to avoid loading the whole CSV into memory.
    - Converts device IDs to lowercase, removes duplicates, and filters invalid IDs.
    - Writes the final data into a temporary file and uploads it to the destination blob.
    - Returns a SAS URL for the destination blob.
    """
    logging.info("[AudienceBuilder] Starting finalize activity.")

    # Initialize the source and destination BlobClients
    input_blob = _get_input_blob(ingress["source"])
    output_blob = _get_output_blob(ingress, input_blob)

    # Define the dialect for CSV format
    dialect = DelimitedTextDialect(
        delimiter=",",
        quotechar='"',
        lineterminator="\n",
        has_header=True,
    )

    # Identify the device ID column from headers
    device_column = _determine_device_column(input_blob, dialect)
    logging.info(
        "[AudienceBuilder] Using device column '%s' from blob '%s'.",
        device_column,
        input_blob.blob_name,
    )

    # Stream-clean device IDs into a temporary file to minimize RAM usage
    with tempfile.TemporaryFile(mode="w+b") as tmp:
        # Write CSV header
        tmp.write(b"deviceid\n")

        # Stream rows
        count = 0
        for device_id in _iter_clean_device_ids(input_blob, dialect, device_column):
            tmp.write(f"{device_id}\n".encode("utf-8"))
            count += 1

        logging.info(
            "[AudienceBuilder] Wrote %d unique, valid device IDs to temp file.",
            count,
        )

        # Rewind and upload the file contents to the destination blob
        tmp.seek(0)
        logging.info(
            "[AudienceBuilder] Uploading to output blob '%s'.",
            output_blob.blob_name,
        )
        output_blob.upload_blob(tmp, overwrite=True)

    # Generate a SAS token for the destination blob with read permissions
    sas_token = generate_blob_sas(
        account_name=output_blob.account_name,
        container_name=output_blob.container_name,
        blob_name=output_blob.blob_name,
        account_key=output_blob.credential.account_key,  # type: ignore[attr-defined]
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    sas_url = f"{unquote(output_blob.url)}?{sas_token}"
    logging.info(
        "[AudienceBuilder] Finalized audience written to '%s'.",
        output_blob.blob_name,
    )

    return sas_url
