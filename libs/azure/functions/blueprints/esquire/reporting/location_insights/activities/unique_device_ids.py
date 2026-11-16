import base64
import codecs
import csv
import io
import os
from datetime import datetime
from typing import Any, Dict, Iterator, List
from urllib.parse import unquote

from azure.durable_functions import Blueprint
from azure.storage.blob import BlobSasPermissions, ContainerClient, generate_blob_sas
from dateutil.relativedelta import relativedelta

from libs.utils.azure_storage import get_container_client
from libs.utils.external_sort import unique_sorted_lines

bp = Blueprint()

# Column holding the device id in the device observation files.
DEVICE_ID_COLUMN = "deviceid"

# Bytes read from the head of a blob when sniffing its header.
_HEADER_SNIFF_BYTES = 512

# Size of the blocks staged while writing the output CSV.
_BLOCK_BYTES = 4 * 1024 * 1024


class _ChunkReader(io.RawIOBase):
    """A readable stream over an iterable of byte chunks."""

    def __init__(self, chunks):
        super().__init__()
        self._chunks = iter(chunks)
        self._buffer = b""
        self._eof = False

    def readable(self) -> bool:
        return True

    def writable(self) -> bool:
        return False

    def seekable(self) -> bool:
        return False

    def readinto(self, buffer) -> int:
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def read(self, size: int = -1) -> bytes:
        while not self._eof and (size < 0 or len(self._buffer) < size):
            try:
                self._buffer += next(self._chunks)
            except StopIteration:
                self._eof = True
        if size < 0 or size >= len(self._buffer):
            data, self._buffer = self._buffer, b""
        else:
            data, self._buffer = self._buffer[:size], self._buffer[size:]
        return data


def _container(conn_str: str, container_name: str) -> ContainerClient:
    """Build a container client for the runtime container."""
    return get_container_client(
        conn_str=os.environ.get(conn_str, conn_str),
        container_name=container_name,
    )


def _iter_rows(chunks) -> Iterator[List[str]]:
    """
    Parse a CSV from a stream of byte chunks.

    Parameters
    ----------
    chunks : Iterable[bytes]
        The raw bytes of the CSV.

    Yields
    ------
    List[str]
        Each parsed row.
    """
    decoder = codecs.getincrementaldecoder("utf-8")()

    def _lines() -> Iterator[str]:
        buffer = ""
        for chunk in chunks:
            if not chunk:
                continue
            buffer += decoder.decode(chunk)
            while True:
                newline = buffer.find("\n")
                if newline < 0:
                    break
                yield buffer[: newline + 1]
                buffer = buffer[newline + 1 :]
        buffer += decoder.decode(b"", final=True)
        if buffer:
            yield buffer

    for row in csv.reader(_lines()):
        yield row


def _device_id_index(header: List[str]) -> int:
    """Find the device id column in a header row."""
    for position, name in enumerate(header):
        if name.strip().strip('"').lower() == DEVICE_ID_COLUMN:
            return position
    return 0


def _iter_device_ids(blob) -> Iterator[str]:
    """
    Yield the device ids from one device observation blob.

    Parameters
    ----------
    blob : BlobClient
        The observation blob to read.

    Yields
    ------
    str
        The value of the device id column of each row.
    """
    rows = _iter_rows(blob.download_blob().chunks())

    header = next(rows, None)
    if header is None:
        return

    index = _device_id_index(header)
    for row in rows:
        if index < len(row):
            value = row[index].strip()
            if value:
                yield value


def _blob_has_header(blob) -> bool:
    """Check whether a blob starts with a header row."""
    try:
        head = blob.download_blob(offset=0, length=_HEADER_SNIFF_BYTES).readall()
    except Exception:
        return False

    newline = head.find(b"\n")
    if newline == -1:
        return False

    first_line = head[:newline].decode("utf-8", errors="replace").lower()
    return DEVICE_ID_COLUMN in first_line


@bp.activity_trigger(input_name="settings")
def activity_locationInsights_uniqueDeviceIds(settings: Dict[str, Any]) -> List[str]:
    """
    Extract the distinct device ids from the device observation files.

    The observation files are read straight out of blob storage and reduced to
    a sorted, de-duplicated list of device ids, which is written back out as a
    CSV for the demographics request that follows.

    The de-duplication runs through a bounded-memory external sort, so the
    memory footprint depends on the chunk size rather than on how many
    observations there are.

    settings:
        {
            "runtime_container": {
                "conn_str": "AzureWebJobsStorage",
                "container_name": "...",
                "observations_blob": "batch/pull/location/observations",
                "unique_devices_blob": "batch/pull/location/unique_devices",
            },
            "instance_id": "...",
        }
    """
    runtime = settings["runtime_container"]
    container_name = runtime["container_name"]
    conn_str = runtime["conn_str"]

    observations_prefix = runtime["observations_blob"].rstrip("/") + "/"
    unique_devices_prefix = runtime["unique_devices_blob"].rstrip("/") + "/"

    source_container = _container(conn_str, container_name)

    def _device_ids() -> Iterator[str]:
        for blob_props in source_container.list_blobs(
            name_starts_with=observations_prefix
        ):
            if blob_props.name.endswith("/"):
                continue
            blob = source_container.get_blob_client(blob_props)
            if not blob.exists():
                continue
            for value in _iter_device_ids(blob):
                yield value

    # Write the distinct device ids out as the CSV the demographics request
    # expects. The rows are staged as they are produced, so the full list is
    # never held in memory.
    output_name = "{0}{1}.csv".format(
        unique_devices_prefix, settings.get("instance_id", "unique_devices")
    )
    output_blob = source_container.get_blob_client(output_name)

    # Start from a clean blob so a re-run does not leave stale blocks behind.
    try:
        output_blob.delete_blob()
    except Exception:
        pass

    block_ids: List[str] = []
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow([DEVICE_ID_COLUMN])

    def _stage() -> None:
        block_ids.append(
            base64.b64encode("{0:08d}".format(len(block_ids)).encode("utf-8")).decode(
                "ascii"
            )
        )
        output_blob.stage_block(
            block_id=block_ids[-1], data=buffer.getvalue().encode("utf-8")
        )
        buffer.seek(0)
        buffer.truncate(0)

    for value in unique_sorted_lines(_device_ids()):
        writer.writerow([value])
        if buffer.tell() >= _BLOCK_BYTES:
            _stage()

    _stage()
    output_blob.commit_block_list(block_ids)

    return [
        unquote(output_blob.url)
        + "?"
        + generate_blob_sas(
            account_name=output_blob.account_name,
            container_name=output_blob.container_name,
            blob_name=output_blob.blob_name,
            account_key=output_blob.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + relativedelta(days=2),
        )
    ]
