"""Stream a source CSV into bounded, independently normalizable blobs.

This activity does not touch Keystone, AddrAll, addresses, or row values.
The destination prefix is supplied by the orchestrator and is scoped to a
stable orchestration instance so retries write the same chunk names.
"""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timedelta, timezone
from time import perf_counter
from typing import Any, Mapping

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobClient,
    BlobSasPermissions,
    ContentSettings,
    generate_blob_sas,
)

from libs.utils.azure_storage import get_cached_blob_client

bp = Blueprint()


class _BlobChunkStream(io.RawIOBase):
    """Expose Azure's streamed byte chunks to TextIOWrapper/csv.reader."""

    def __init__(self, chunks):
        self._chunks = iter(chunks)
        self._pending = memoryview(b"")

    def readable(self) -> bool:
        return True

    def readinto(self, target) -> int:
        while not self._pending:
            try:
                self._pending = memoryview(next(self._chunks))
            except StopIteration:
                return 0
        size = min(len(target), len(self._pending))
        target[:size] = self._pending[:size]
        self._pending = self._pending[size:]
        return size


def _chunk_blob(
    destination: Mapping[str, Any], source_index: int, chunk_index: int
) -> BlobClient:
    setting = str(destination["conn_str"])
    connection_string = os.getenv(setting, setting)
    prefix = str(destination.get("blob_prefix", "")).strip("/")
    filename = f"source-{source_index:05d}/chunk-{chunk_index:06d}.csv"
    blob_name = f"{prefix}/{filename}" if prefix else filename
    return BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=str(destination["container_name"]),
        blob_name=blob_name,
    )


def _read_url(blob: BlobClient) -> str:
    # Match the read-SAS behavior of the existing normalizeAddresses activity.
    token = generate_blob_sas(
        account_name=blob.account_name,
        account_key=blob.credential.account_key,
        container_name=blob.container_name,
        blob_name=blob.blob_name,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.now(timezone.utc) + timedelta(hours=24),
    )
    return f"{blob.url}?{token}"


def split_csv_blob(
    *,
    source_url: str,
    destination: Mapping[str, Any],
    source_index: int,
    rows_per_chunk: int,
) -> dict[str, Any]:
    """Preserve every CSV record and column, including quoted newlines."""
    if rows_per_chunk < 1:
        raise ValueError("rows_per_chunk must be positive")
    if source_index < 0:
        raise ValueError("source_index must be nonnegative")
    started = perf_counter()
    src = get_cached_blob_client(source_url)
    downloader = src.download_blob(max_concurrency=1)
    urls: list[str] = []
    total_rows = 0
    chunk_index = 0

    # csv.reader/writer avoids DictReader losing repeated input column names.
    with io.TextIOWrapper(
        io.BufferedReader(_BlobChunkStream(downloader.chunks())),
        encoding="utf-8-sig",
        newline="",
    ) as text:
        reader = csv.reader(text)
        header = next(reader, None)
        if header is None:
            return {"urls": [], "rows": 0, "chunks": 0}

        rows: list[list[str]] = []

        def flush() -> None:
            nonlocal rows, chunk_index
            out = io.StringIO(newline="")
            writer = csv.writer(out)
            writer.writerow(header)
            writer.writerows(rows)
            blob = _chunk_blob(destination, source_index, chunk_index)
            try:
                blob.upload_blob(
                    out.getvalue().encode("utf-8"),
                    overwrite=True,
                    content_settings=ContentSettings(content_type="text/csv"),
                )
                urls.append(_read_url(blob))
            finally:
                blob.close()
                out.close()
            chunk_index += 1
            rows = []

        for record in reader:
            rows.append(record)
            total_rows += 1
            if len(rows) >= rows_per_chunk:
                flush()

        # A header-only file stays representable as a header-only chunk.
        if rows or chunk_index == 0:
            flush()

    print(
        f"Address CSV split: source_index={source_index}, "
        f"rows={total_rows:,}, chunks={len(urls):,}, "
        f"elapsed={perf_counter() - started:.2f}s",
        flush=True,
    )
    return {"urls": urls, "rows": total_rows, "chunks": len(urls)}


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesNeighbors_splitAddressBlob(
    ingress: Mapping[str, Any],
) -> dict[str, Any]:
    return split_csv_blob(
        source_url=str(ingress["source_url"]),
        destination=ingress["destination"],
        source_index=int(ingress["source_index"]),
        rows_per_chunk=int(
            ingress.get("rows_per_chunk", os.getenv("ADDRESS_NORMALIZATION_ROWS_PER_ACTIVITY", "500"))
        ),
    )
