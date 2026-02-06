from __future__ import annotations

import csv
import io
import logging
import os
import uuid
from typing import Any, Iterable, Iterator, Mapping, Sequence

import fastavro
import fsspec
from azure.durable_functions import Blueprint
from azure.storage.blob import BlobClient
from libs.utils.azure_storage import get_cached_blob_client, init_blob_client

bp = Blueprint()
logger = logging.getLogger(__name__)

# Define the schema as a global variable
SCHEMA: dict[str, Any] = {
    "namespace": "xandr.avro",
    "name": "user",
    "type": "record",
    "fields": [
        {
            "name": "uid",
            "doc": "User ID. Can be one of anid, ifa, xfa, external_id, device_id type.",
            "type": [
                {
                    "name": "device_id",
                    "type": "record",
                    "doc": "Mobile device ID record.",
                    "fields": [
                        {"name": "id", "type": "string", "doc": "Mobile device ID."},
                        {
                            "name": "domain",
                            "type": {
                                "name": "domain",
                                "type": "enum",
                                "doc": "Mobile device domain.",
                                "symbols": [
                                    "idfa",
                                    "sha1udid",
                                    "md5udid",
                                    "openudid",
                                    "aaid",
                                    "windowsadid",
                                    "rida",
                                ],
                            },
                        },
                    ],
                },
            ],
        },
        {
            "name": "segments",
            "doc": "Array of segments.",
            "type": {
                "type": "array",
                "doc": "Element of the segments array.",
                "items": {
                    "name": "segment",
                    "type": "record",
                    "fields": [
                        {
                            "name": "id",
                            "type": "int",
                            "doc": "Segment ID. Alternatively, pair of code and member_id can be used.",
                            "default": 0,
                        },
                        {
                            "name": "member_id",
                            "type": "int",
                            "doc": "Segment member ID. Requires segment.code.",
                            "default": 0,
                        },
                        {
                            "name": "expiration",
                            "type": "int",
                            "doc": "Segment expiration in minutes. 0: max expiration (180 days); -2: default expiration; -1: segment removal.",
                            "default": 0,
                        },
                        {
                            "name": "timestamp",
                            "type": "long",
                            "doc": "Defines when segment becomes 'live'. Timestamp in seconds from epoch. 0 enables segment immediately",
                            "default": 0,
                        },
                    ],
                },
            },
        },
    ],
}

# Parse once at import time (saves repeated work and keeps memory stable)
PARSED_SCHEMA = fastavro.parse_schema(SCHEMA)

# Domains are constant and tiny; keep as a tuple to avoid per-row allocations
DEVICE_DOMAINS: tuple[str, str] = ("aaid", "idfa")


class _ChunkIteratorRawIO(io.RawIOBase):
    """
    Minimal raw binary stream adapter around an iterator of bytes chunks.
    Lets us wrap Azure Blob's downloader.chunks() in BufferedReader/TextIOWrapper,
    enabling true streaming CSV parsing with low memory use.
    """

    def __init__(self, chunks: Iterable[bytes]) -> None:
        super().__init__()
        self._it = iter(chunks)
        self._cur = b""
        self._pos = 0

    def readable(self) -> bool:  # pragma: no cover
        return True

    def readinto(self, b: bytearray | memoryview) -> int:
        mv = memoryview(b)
        total = 0

        while total < len(mv):
            if self._pos >= len(self._cur):
                try:
                    self._cur = next(self._it)
                    self._pos = 0
                except StopIteration:
                    break
                if not self._cur:
                    continue

            n = min(len(self._cur) - self._pos, len(mv) - total)
            mv[total : total + n] = self._cur[self._pos : self._pos + n]
            self._pos += n
            total += n

        return total


def _source_blob_from_ingress(ingress: Mapping[str, Any]) -> BlobClient:
    source = ingress.get("source")
    if isinstance(source, str):
        return get_cached_blob_client(source)

    if not isinstance(source, Mapping):
        raise ValueError("ingress['source'] must be a blob url string or a mapping")

    conn_str_env = source.get("conn_str")
    container_name = source.get("container_name")
    blob_name = source.get("blob_name")

    if not isinstance(conn_str_env, str) or not conn_str_env:
        raise ValueError("ingress['source']['conn_str'] must be an env var name string")
    if not isinstance(container_name, str) or not container_name:
        raise ValueError("ingress['source']['container_name'] must be a non-empty string")
    if not isinstance(blob_name, str) or not blob_name:
        raise ValueError("ingress['source']['blob_name'] must be a non-empty string")

    conn_str = os.environ[conn_str_env]
    return init_blob_client(
        conn_str=conn_str,
        container_name=container_name,
        blob_name=blob_name,
    )


def _find_header_index(headers: Sequence[str], wanted: str) -> int:
    wanted_lc = wanted.strip().lower()
    for idx, name in enumerate(headers):
        if name.strip().lower() == wanted_lc:
            return idx
    return -1


def _iter_avro_records_from_csv_stream(
    text_stream: io.TextIOBase,
    *,
    segment_id: int,
    member_id: int,
    expiration_minutes: int,
) -> Iterator[dict[str, Any]]:
    reader = csv.reader(text_stream)
    headers = next(reader, None)
    if headers is None:
        return  # empty file

    deviceid_idx = _find_header_index(headers, "deviceid")
    if deviceid_idx < 0:
        raise ValueError("CSV missing required header column: deviceid")

    # Reuse the same segment object to minimize per-record allocations.
    segment = {
        "id": segment_id,
        "member_id": member_id,
        "expiration": expiration_minutes,
        "timestamp": 0,
    }
    segments = (segment,)  # tuple is fine for Avro arrays

    for row in reader:
        if deviceid_idx >= len(row):
            continue
        device_id = row[deviceid_idx].strip()
        if not device_id:
            continue

        # Emit two records per device_id without building any list in memory.
        for domain in DEVICE_DOMAINS:
            yield {
                "uid": {"id": device_id, "domain": domain},
                "segments": segments,
            }


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_generateAvro(ingress) -> str:
    # Parse and validate small control values up front
    audience = ingress.get("audience")
    if not isinstance(audience, Mapping):
        raise ValueError("ingress['audience'] must be a mapping")

    segment_raw = audience.get("segment")
    expiration_raw = audience.get("expiration")

    try:
        segment_id = int(segment_raw)  # type: ignore[arg-type]
    except Exception as e:
        raise ValueError("ingress['audience']['segment'] must be int-like") from e

    try:
        expiration_minutes = int(expiration_raw)  # type: ignore[arg-type]
    except Exception as e:
        raise ValueError("ingress['audience']['expiration'] must be int-like") from e

    try:
        member_id = int(os.environ["XANDR_MEMBER_ID"])
    except Exception as e:
        raise ValueError("Env var XANDR_MEMBER_ID must be set to an int") from e

    destination = ingress.get("destination")
    if not isinstance(destination, Mapping):
        raise ValueError("ingress['destination'] must be a mapping")

    bucket = destination.get("bucket")
    access_key = destination.get("access_key")
    secret_key = destination.get("secret_key")
    if not isinstance(bucket, str) or not bucket:
        raise ValueError("ingress['destination']['bucket'] must be a non-empty string")
    if not isinstance(access_key, str) or not access_key:
        raise ValueError("ingress['destination']['access_key'] must be a non-empty string")
    if not isinstance(secret_key, str) or not secret_key:
        raise ValueError("ingress['destination']['secret_key'] must be a non-empty string")

    # Source: stream the blob download (max_concurrency=1 reduces buffering/memory)
    source_blob = _source_blob_from_ingress(ingress)
    downloader = source_blob.download_blob(max_concurrency=1)

    # Adapt downloader chunks -> buffered binary -> text stream for csv.reader
    raw = _ChunkIteratorRawIO(downloader.chunks())
    buffered = io.BufferedReader(raw, buffer_size=1024 * 1024)  # 1 MiB
    text_stream = io.TextIOWrapper(buffered, encoding="utf-8-sig", newline="")

    fs = fsspec.filesystem(
        "s3",
        key=access_key,
        secret=secret_key,
    )

    out_key = f"submitted/{uuid.uuid4().hex}.avro"
    out_uri = f"s3://{bucket}/{out_key}"

    # For s3fs/fsspec writes, a smaller block_size reduces peak buffering memory.
    # 5 MiB aligns with common S3 multipart minimum part sizes.
    block_size = 5 * 1024 * 1024

    logger.info("Generating Avro to %s", out_uri)

    try:
        with text_stream:
            records_iter = _iter_avro_records_from_csv_stream(
                text_stream,
                segment_id=segment_id,
                member_id=member_id,
                expiration_minutes=expiration_minutes,
            )

            with fs.open(out_uri, "wb", block_size=block_size) as out:
                fastavro.writer(
                    out,
                    PARSED_SCHEMA,
                    records_iter,
                    validator=False,
                    # keep blocks small to avoid buffering many records at once
                    sync_interval=16_000,
                )
    finally:
        # Ensure underlying buffers are released even if exceptions occur.
        try:
            text_stream.detach()
        except Exception:
            pass

    return ""
