from __future__ import annotations

import base64
from datetime import datetime
import os
from urllib.parse import unquote
import uuid
from typing import Any, Iterable, Iterator, Mapping, Sequence

from azure.durable_functions import Blueprint
from azure.storage.blob import (
    BlobBlock,
    BlobClient,
    ContentSettings,
    BlobSasPermissions,
    generate_blob_sas,
)
from matplotlib.dates import relativedelta

bp = Blueprint()

# Download chunking is controlled by BlobClient constructor kwargs:
# - max_single_get_size: max size downloaded in a single GET before switching to ranged/chunked GETs
# - max_chunk_get_size: max size of each ranged GET chunk
_DOWNLOAD_CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB
_STAGE_BLOCK_SIZE = 32 * 1024 * 1024  # 32 MiB
_MAX_HEADER_SCAN_BYTES = 2 * 1024 * 1024  # 2 MiB


def _resolve_conn_str(value_or_env_name: str) -> str:
    """
    Accept either:
      - an environment variable name containing the connection string, OR
      - the connection string itself.
    """
    return os.getenv(value_or_env_name, value_or_env_name)


def _new_block_id() -> str:
    # Azure requires Base64 block IDs.
    return base64.b64encode(uuid.uuid4().bytes).decode("ascii")


def _blob_client_from_sas_url(url: str) -> BlobClient:
    # Ensure consistent small-ish chunks; also prevents a single 32MiB read by default.
    return BlobClient.from_blob_url(
        url,
        max_single_get_size=_DOWNLOAD_CHUNK_SIZE,
        max_chunk_get_size=_DOWNLOAD_CHUNK_SIZE,
    )


def _iter_blob_bytes_from_url(url: str) -> Iterator[bytes]:
    src = _blob_client_from_sas_url(url)
    downloader = src.download_blob(max_concurrency=1)
    # chunks() has no chunk_size kwarg in current SDK.
    yield from downloader.chunks()


def _skip_first_line(chunks: Iterable[bytes]) -> Iterator[bytes]:
    """
    Consume bytes until (and including) the first newline, then yield the rest.
    If no newline is found within _MAX_HEADER_SCAN_BYTES, treat as header-only and yield nothing.
    """
    buf = bytearray()

    for chunk in chunks:
        if not chunk:
            continue

        if buf is not None:
            buf.extend(chunk)
            nl = buf.find(b"\n")
            if nl == -1:
                if len(buf) >= _MAX_HEADER_SCAN_BYTES:
                    return
                continue

            remainder = memoryview(buf)[nl + 1 :]
            if len(remainder):
                yield remainder.tobytes()
            buf = None  # type: ignore[assignment]
            continue

        yield chunk


def _iter_merged_csv_bytes(source_urls: Sequence[str]) -> Iterator[bytes]:
    """
    Stream merge:
      - first file: include header
      - subsequent files: skip header line
      - prevents row concatenation if a prior file doesn't end with '\n' by inserting one
        before the next file's first data chunk (only if the next file actually has data)
    """
    last_byte: int | None = None
    first = True

    for url in source_urls:
        if first:
            for chunk in _iter_blob_bytes_from_url(url):
                if chunk:
                    last_byte = chunk[-1]
                    yield chunk
            first = False
            continue

        data_iter = _skip_first_line(_iter_blob_bytes_from_url(url))
        try:
            first_data_chunk = next(data_iter)
        except StopIteration:
            continue

        if last_byte is not None and last_byte != 0x0A:  # b"\n"
            yield b"\n"
            last_byte = 0x0A

        if first_data_chunk:
            last_byte = first_data_chunk[-1]
            yield first_data_chunk

        for chunk in data_iter:
            if chunk:
                last_byte = chunk[-1]
                yield chunk


def _stage_iterable_as_blocks(
    dest: BlobClient, data_parts: Iterable[bytes], *, block_size: int
) -> list[BlobBlock]:
    """
    Stage a stream of bytes as a sequence of blocks and return the ordered BlobBlock list
    required by commit_block_list() in newer azure-storage-blob versions.
    """
    blocks: list[BlobBlock] = []
    buffer = bytearray()
    start = 0

    def _stage_bytes(data: bytes) -> None:
        block_id = _new_block_id()
        dest.stage_block(block_id=block_id, data=data)
        blocks.append(BlobBlock(block_id=block_id))

    for part in data_parts:
        if not part:
            continue

        buffer.extend(part)

        while (len(buffer) - start) >= block_size:
            _stage_bytes(bytes(buffer[start : start + block_size]))
            start += block_size

        # Compact occasionally so the front-gap doesn't grow unbounded.
        if start and start >= (4 * block_size):
            del buffer[:start]
            start = 0

    remaining = len(buffer) - start
    if remaining:
        _stage_bytes(bytes(buffer[start:]))

    return blocks


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_mergeSources(ingress: Mapping[str, Any]) -> str:
    source_urls_raw = ingress.get("source_urls")
    if not isinstance(source_urls_raw, list) or not all(
        isinstance(u, str) for u in source_urls_raw
    ):
        raise ValueError("ingress['source_urls'] must be a list[str]")

    source_urls: list[str] = source_urls_raw
    if not source_urls:
        raise ValueError("No source URLs provided")

    destination = ingress.get("destination")
    if not isinstance(destination, dict):
        raise ValueError("ingress['destination'] must be an object")

    conn_str_key = destination.get("conn_str")
    container_name = destination.get("container_name")
    blob_prefix = destination.get("blob_prefix", "")

    if not isinstance(conn_str_key, str) or not conn_str_key:
        raise ValueError("destination['conn_str'] must be a non-empty string")
    if not isinstance(container_name, str) or not container_name:
        raise ValueError("destination['container_name'] must be a non-empty string")
    if not isinstance(blob_prefix, str):
        raise ValueError("destination['blob_prefix'] must be a string")

    blob_name = destination.get("blob_name")
    if blob_name is None:
        prefix = blob_prefix.strip("/")
        blob_name = (
            f"{prefix}/{uuid.uuid4().hex}.csv" if prefix else f"{uuid.uuid4().hex}.csv"
        )
    elif not isinstance(blob_name, str) or not blob_name:
        raise ValueError(
            "destination['blob_name'] must be a non-empty string when provided"
        )

    conn_str = _resolve_conn_str(conn_str_key)

    dest_blob = BlobClient.from_connection_string(
        conn_str=conn_str,
        container_name=container_name,
        blob_name=blob_name,
    )

    merged_stream = _iter_merged_csv_bytes(source_urls)
    block_list = _stage_iterable_as_blocks(
        dest_blob, merged_stream, block_size=_STAGE_BLOCK_SIZE
    )

    if not block_list:
        dest_blob.upload_blob(
            b"",
            overwrite=True,
            content_settings=ContentSettings(content_type="text/csv"),
        )
        return (
            unquote(dest_blob.url)
            + "?"
            + generate_blob_sas(
                account_name=dest_blob.account_name,
                account_key=dest_blob.credential.account_key,
                container_name=dest_blob.container_name,
                blob_name=dest_blob.blob_name,
                permission=BlobSasPermissions(read=True, write=True),
                expiry=datetime.utcnow() + relativedelta(days=2),
            )
        )

    dest_blob.commit_block_list(
        block_list,
        content_settings=ContentSettings(content_type="text/csv"),
    )

    return (
        unquote(dest_blob.url)
        + "?"
        + generate_blob_sas(
            account_name=dest_blob.account_name,
            account_key=dest_blob.credential.account_key,
            container_name=dest_blob.container_name,
            blob_name=dest_blob.blob_name,
            permission=BlobSasPermissions(read=True, write=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    )
