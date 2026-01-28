import csv
import logging
import os
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Union

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    DelimitedTextDialect,
    generate_blob_sas,
)
from dateutil.relativedelta import relativedelta
from urllib.parse import unquote
import base64

bp = Blueprint()

# Constant for the "anonymous" UUID you want to ignore
_ANONYMOUS_UUID = "00000000-0000-0000-0000-000000000000"

# Append blob block size limit is 4 MiB. Keep headroom.
_APPEND_FLUSH_BYTES = 3_500_000


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


def _get_output_blob_name(
    ingress: Dict[str, Any],
    first_input_blob: BlobClient,
    sources_count: int,
) -> str:
    """
    Preserve existing naming for single-source finalize:
      {destination.blob_prefix}/{basename(input_blob.blob_name)}

    For folded batches (multi-source), generate a deterministic batch name:
      {destination.blob_prefix}/final_{batch_index:05d}.csv

    batch_index is expected to be provided by the orchestrator for folded batches.
    """
    destination = ingress["destination"]
    prefix = destination["blob_prefix"]

    batch_index = ingress.get("batch_index", None)

    if sources_count > 1 and batch_index is None:
        raise ValueError(
            "batch_index must be provided when finalizing multiple sources"
        )

    if batch_index is not None:
        idx = int(batch_index)
        return f"{prefix}/final_{idx:05d}.csv"

    return "{}/{}".format(prefix, os.path.basename(first_input_blob.blob_name))


def _get_output_blob(ingress: Dict[str, Any], blob_name: str) -> BlobClient:
    """
    Build a BlobClient for the destination blob based on the 'destination' entry in ingress.
    """
    destination = ingress["destination"]
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

    raise ValueError(f"Unable to determine device column. Headers found: {raw_columns!r}")


def _parse_single_value(record_bytes: bytes, dialect: DelimitedTextDialect) -> str:
    text = record_bytes.decode("utf-8")
    reader = csv.reader([text], delimiter=dialect.delimiter, quotechar=dialect.quotechar)
    row = next(reader, [])
    return row[0] if row else ""


def _iter_clean_device_ids(
    input_blob: BlobClient,
    dialect: DelimitedTextDialect,
    device_column: str,
) -> Iterable[str]:
    """
    Stream device IDs from the blob query, cleaning, filtering, and de-duplicating
    on the fly (per source blob). Yields lowercased, valid, unique device IDs.

    NOTE: De-duplication is per source blob to keep memory bounded. If you need
    global de-dupe across sources, do that in a separate scalable step (e.g. partitioned
    on-disk de-dupe or external store), not with a single in-memory set.
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

    # Detect header row (single column containing 'device')
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


def _normalize_sources(ingress: Dict[str, Any]) -> List[Any]:
    """
    Backwards compatible:
      - ingress["source"] can be a single source OR list of sources
      - we always return List[source]
    """
    if "source" not in ingress:
        raise ValueError("Finalize called without 'source' in ingress.")

    raw = ingress["source"]
    if isinstance(raw, list):
        return raw
    return [raw]


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_finalize(ingress: Dict[str, Any]) -> str:
    """
    Finalizes one or more source blobs into a single standardized CSV (Block Blob)
    with header 'deviceid', while avoiding RAM blowups.

    Backwards-compatible behavior:
      - ingress["source"] can be a single source or a list of sources
      - optional ingress["batch_index"] enables deterministic naming for folded batches

    Output is written as an Block Blob with streaming append, not a temp file.
    """
    sources = _normalize_sources(ingress)
    if not sources:
        raise ValueError("Finalize called with empty source list.")

    # Dialect used by blob query (same as before)
    dialect = DelimitedTextDialect(
        delimiter=",",
        quotechar='"',
        lineterminator="\n",
        has_header=True,
    )

    first_input_blob = _get_input_blob(sources[0])
    output_blob_name = _get_output_blob_name(ingress, first_input_blob, len(sources))
    output_blob = _get_output_blob(ingress, output_blob_name)

    logging.info(
        "[AudienceBuilder] Finalize starting. sources=%d output=%s",
        len(sources),
        output_blob.blob_name,
    )

    # Idempotency: delete existing output blob if present
    try:
        output_blob.delete_blob()
    except Exception:
        pass

    block_ids: List[str] = []
    block_index = 0
    buf = bytearray()

    def _stage_buffer() -> None:
        nonlocal block_index
        if not buf:
            return
        block_id = base64.b64encode(f"{block_index:08d}".encode("utf-8")).decode("ascii")
        output_blob.stage_block(block_id=block_id, data=bytes(buf))
        block_ids.append(block_id)
        block_index += 1
        buf.clear()

    # Write header first
    buf.extend(b"deviceid\n")

    for source in sources:
        input_blob = _get_input_blob(source)

        device_column = _determine_device_column(input_blob, dialect)

        for device_id in _iter_clean_device_ids(input_blob, dialect, device_column):
            buf.extend(f"{device_id}\n".encode("utf-8"))

            if len(buf) >= _APPEND_FLUSH_BYTES:
                _stage_buffer()

    # Final flush
    _stage_buffer()

    # Commit staged blocks → creates Block Blob
    output_blob.commit_block_list(block_ids)

    logging.info(
        "[AudienceBuilder] Finalize wrote %d blocks to final blob '%s'.",
        len(block_ids),
        output_blob.blob_name,
    )

    sas_token = generate_blob_sas(
        account_name=output_blob.account_name,
        container_name=output_blob.container_name,
        blob_name=output_blob.blob_name,
        account_key=output_blob.credential.account_key,  # type: ignore[attr-defined]
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + relativedelta(days=2),
    )

    return f"{unquote(output_blob.url)}?{sas_token}"
