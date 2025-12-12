from __future__ import annotations

from typing import Any, Dict, Union

import csv
import os
import tempfile
import uuid

import fsspec
from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient

bp = Blueprint()


def _build_blob_client(source: Union[str, Dict[str, Any]]) -> BlobClient:
    """
    Build a BlobClient from either:
      - a blob URL string, or
      - a dict describing the binding configuration.

    Dict example:
        {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": "<container>",
            "blob_name": "<blob-name>.txt",
        }
    """
    if isinstance(source, str):
        # Direct Blob SAS/URL
        return BlobClient.from_blob_url(source)

    conn_str_env_name = source["conn_str"]
    return BlobClient.from_connection_string(
        conn_str=os.environ[conn_str_env_name],
        container_name=source["container_name"],
        blob_name=source["blob_name"],
    )


def _resolve_s3_path(destination: Dict[str, Any]) -> str:
    """
    Build the full s3:// URL from destination dict.

    Destination example:
    {
        "access_key": "...",
        "secret_key": "...",
        "bucket": "beeswax-data-us-east-1",
        # Optional; if omitted, the default Buyer Cloud pattern is used:
        #   user-list/dsp/<account_id>/segment-<uuid>.txt
        "object_key": "user-list/dsp/<account_id>/segment-<uuid>.txt",
        # Optional override for account_id; otherwise FREEWHEEL_BUZZ_ACCOUNT_ID is used
        "account_id": 1234,
    }

    If object_key is not supplied, a default of:
        user-list/dsp/<account_id>/segment-<uuid>.txt
    is used, matching the Buyer Cloud "Upload Segments to S3" documentation.
    """
    bucket: str = destination["bucket"]

    object_key_raw = destination.get("object_key")
    if object_key_raw:
        object_key = str(object_key_raw)
    else:
        # Derive account_id from destination or env, for the default S3 prefix
        account_id_raw = destination.get("account_id") or os.getenv(
            "FREEWHEEL_BUZZ_ACCOUNT_ID"
        )
        if not account_id_raw:
            raise KeyError(
                "S3 destination requires either 'object_key' or a configured "
                "account_id (destination['account_id'] or FREEWHEEL_BUZZ_ACCOUNT_ID)."
            )

        account_id = str(account_id_raw)
        object_key = f"user-list/dsp/{account_id}/segment-{uuid.uuid4().hex}.txt"

    # Normalize leading slash on object_key
    object_key = object_key.lstrip("/")

    return f"s3://{bucket}/{object_key}"


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_initSegmentBlob(ingress: Dict[str, Any]) -> str:
    """
    Initialize (create/overwrite) the append blob that will hold the final
    Buyer Cloud segment upload payload.

    ingress:
        {
            "segment_blob": {
                "conn_str": "<azure-blob-conn-str-name>",
                "container_name": "<container>",
                "blob_name": "tmp/freewheel/segment-<audience>-<instance>.txt",
            } OR "<full-blob-url>",
        }

    Returns the append-blob URL (for logging/debugging).
    """
    segment_blob = ingress["segment_blob"]
    blob_client = _build_blob_client(segment_blob)

    # Overwrite with a fresh, empty Append Blob.
    # NOTE: This must be called exactly once before any append_block operations.
    blob_client.create_append_blob()

    return blob_client.url


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_generateSegment(ingress: Dict[str, Any]) -> str:
    """
    Convert a source audience CSV (from Azure Blob) into Buyer Cloud segment lines
    and append them into a single shared Azure append blob.

    This activity no longer writes directly to S3; instead it appends into the
    pre-created append blob. Another activity later copies that blob to S3.

    Ingress structure (as used by the orchestrator):

        {
            "audience": {
                "segment": "<segment_key>",  # Buyer Cloud segment key, e.g. "stinger-123"
                "expiration": 1440           # Not used in file; kept for compatibility
            },
            "source": {
                "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
                "container_name": "<container>",
                "blob_name": "<blob-name>.csv",
            } OR "<full-blob-url>",
            "segment_blob": {
                "conn_str": "<azure-blob-conn-str-name>",
                "container_name": "<container>",
                "blob_name": "tmp/freewheel/segment-<audience>-<instance>.txt",
            } OR "<full-blob-url>",
            # Optional, for tuning append block size (bytes); defaults to 4MB.
            "max_append_block_bytes": 4194304,
        }

    Segment upload file format (text file, per docs):
        <user_id>|<segment_key>

    One line per user, no header, UTF-8 encoded, .txt extension.
    """
    source = ingress["source"]
    segment_blob = ingress["segment_blob"]
    audience = ingress["audience"]

    segment_key = str(audience["segment"])

    source_blob_client = _build_blob_client(source)
    segment_blob_client = _build_blob_client(segment_blob)

    # Prepare local temp file to stream the blob into, avoiding large in-memory buffers
    tmp_path: str | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as tmp_file:
            tmp_path = tmp_file.name
            downloader = source_blob_client.download_blob()
            downloader.download_to_stream(tmp_file)

        # Append Blob limitation: each append block is limited in size.
        # Use a conservative default of 4MB per append block to work across
        # all API versions.
        max_append_block_bytes_raw: Any = ingress.get("max_append_block_bytes")
        try:
            max_append_block_bytes = (
                int(max_append_block_bytes_raw)
                if max_append_block_bytes_raw
                else 4 * 1024 * 1024
            )
        except (TypeError, ValueError):
            max_append_block_bytes = 4 * 1024 * 1024

        with open(tmp_path, mode="r", encoding="utf-8", newline="") as in_file:
            reader = csv.DictReader(in_file)

            if not reader.fieldnames:
                raise ValueError(
                    "Source audience file has no header row; cannot infer columns."
                )

            # Allow both "deviceid" and "device_id" for flexibility
            has_deviceid = "deviceid" in reader.fieldnames
            has_device_id = "device_id" in reader.fieldnames

            if not (has_deviceid or has_device_id):
                raise ValueError(
                    "Source audience file is missing required 'deviceid' or 'device_id' column "
                    "needed for Buyer Cloud segment upload."
                )

            buffer: list[bytes] = []
            buffer_bytes = 0

            def flush_buffer() -> None:
                nonlocal buffer, buffer_bytes
                if not buffer:
                    return
                data = b"".join(buffer)
                # Append this chunk to the existing append blob.
                segment_blob_client.append_block(data)
                buffer = []
                buffer_bytes = 0

            for row in reader:
                raw_id = (
                    (row.get("deviceid") if has_deviceid else None)
                    or (row.get("device_id") if has_device_id else None)
                    or ""
                )
                device_id = raw_id.strip()
                if not device_id:
                    # Skip blank / malformed rows quietly
                    continue

                line_bytes = f"{device_id}|{segment_key}\n".encode("utf-8")

                # Keep each append block under the configured limit.
                if buffer_bytes + len(line_bytes) > max_append_block_bytes and buffer:
                    flush_buffer()

                buffer.append(line_bytes)
                buffer_bytes += len(line_bytes)

            # Flush any remaining rows.
            flush_buffer()

        # Return the segment append-blob URL for logging / downstream usage.
        return segment_blob_client.url

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                # Non-fatal; temp file cleanup best-effort.
                pass


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_uploadSegmentToS3(ingress: Dict[str, Any]) -> str:
    """
    Stream the completed append blob (containing all segment rows) into a
    single S3 object, then optionally delete the temporary append blob.

    ingress:
        {
            "segment_blob": {
                "conn_str": "<azure-blob-conn-str-name>",
                "container_name": "<container>",
                "blob_name": "tmp/freewheel/segment-<audience>-<instance>.txt",
            } OR "<full-blob-url>",
            "destination": {
                "access_key": "...",
                "secret_key": "...",
                "bucket": "<S3 bucket>",
                # Optional; if omitted, we use:
                #   user-list/dsp/<account_id>/segment-<uuid>.txt
                "object_key": "user-list/dsp/<account_id>/file_name.txt",
                # Optional; overrides env for default path building:
                "account_id": 1234,
            },
            # Optional, default True: delete the append blob after a successful upload.
            "delete_after_upload": True,
        }

    Returns:
        str: s3:// URL of the created segment object.
    """
    segment_blob = ingress["segment_blob"]
    destination = ingress["destination"]
    delete_after_upload = bool(ingress.get("delete_after_upload", True))

    segment_blob_client = _build_blob_client(segment_blob)

    # Create S3 filesystem with ACL compliance:
    # - bucket-owner-full-control ensures Buyer Cloud owns the object when
    #   customers upload to FreeWheel's bucket in a cross-account setup.
    fs = fsspec.filesystem(
        "s3",
        key=destination["access_key"],
        secret=destination["secret_key"],
        s3_additional_kwargs={"ACL": "bucket-owner-full-control"},
    )

    s3_path = _resolve_s3_path(destination)

    # Stream Azure Append Blob -> S3 object, no large in-memory buffer.
    downloader = segment_blob_client.download_blob()

    with fs.open(s3_path, mode="wb") as s3_file:
        for chunk in downloader.chunks():
            if not chunk:
                continue
            s3_file.write(chunk)

    if delete_after_upload:
        try:
            # Best-effort cleanup of the temporary append blob.
            segment_blob_client.delete_blob(delete_snapshots="include")
        except Exception:
            # Non-fatal; cleanup should not break the pipeline.
            pass

    return s3_path
