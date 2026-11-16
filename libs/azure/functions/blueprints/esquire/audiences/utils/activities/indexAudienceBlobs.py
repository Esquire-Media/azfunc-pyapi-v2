from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from azure.durable_functions import Blueprint

from libs.azure.functions.blueprints.esquire.audiences.utils.audienceBlobs import (
    MAID_RECORD_BYTES,
    choose_data_offset,
    detect_data_offset,
)
from libs.utils.azure_storage import get_container_client, init_blob_client

bp = Blueprint()

# The builder writes the finalized audience as "final_{index:05d}.csv" blobs.
_FINAL_BLOB_SUFFIX = ".csv"
_FINAL_BLOB_PREFIX = "final_"

# Bytes read when probing a blob for its header row.
_PROBE_BYTES = 256


def _list_blob_sizes(
    conn_str: str,
    container_name: str,
    blob_prefix: str,
) -> List[Tuple[str, int]]:
    """
    List the blobs under a prefix, returning only (name, size) pairs.

    Parameters
    ----------
    conn_str : str
        Connection string (or the name of the environment variable holding it).
    container_name : str
        The container holding the audience blobs.
    blob_prefix : str
        Prefix to list, e.g. ``audiences/{id}/{iso_date}``.

    Returns
    -------
    List[Tuple[str, int]]
        Blob names and sizes, sorted by name.

    Notes
    -----
    Only names and sizes are retained, so the memory cost is proportional to
    the number of blobs in the folder rather than to their content.
    """
    resolved_conn_str = os.environ.get(conn_str, conn_str)
    prefix = blob_prefix.rstrip("/") + "/"

    found: List[Tuple[str, int]] = []
    with get_container_client(
        conn_str=resolved_conn_str, container_name=container_name
    ) as container_client:
        for blob in container_client.list_blobs(name_starts_with=prefix):
            # Skip directory marker blobs.
            if blob.name.endswith("/"):
                continue
            size = blob.size
            if not size:
                continue
            found.append((blob.name, int(size)))

    found.sort(key=lambda item: item[0])
    return found


def _select_blobs(found: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    """
    Narrow a listing down to the finalized audience blobs.

    Parameters
    ----------
    found : List[Tuple[str, int]]
        All blobs found under the prefix.

    Returns
    -------
    List[Tuple[str, int]]
        The finalized audience blobs when there are any, otherwise every blob
        that was found.
    """
    finalized = [
        item
        for item in found
        if item[0].rsplit("/", 1)[-1].startswith(_FINAL_BLOB_PREFIX)
        and item[0].lower().endswith(_FINAL_BLOB_SUFFIX)
    ]
    return finalized if finalized else found


def _probe_data_offset(
    conn_str: str,
    container_name: str,
    blob_name: str,
) -> Optional[int]:
    """
    Read the first bytes of a blob to determine where its records begin.

    Parameters
    ----------
    conn_str : str
        Connection string (or the name of the environment variable holding it).
    container_name : str
        The container holding the audience blobs.
    blob_name : str
        The blob to probe.

    Returns
    -------
    Optional[int]
        The detected offset, or ``None`` when the probe could not be made.
    """
    try:
        blob_client = init_blob_client(
            conn_str=conn_str,
            container_name=container_name,
            blob_name=blob_name,
        )
        head = blob_client.download_blob(offset=0, length=_PROBE_BYTES).readall()
    except Exception:
        return None

    return detect_data_offset(head)


def _build_index(
    blobs: List[Tuple[str, int]],
    data_offset: int,
) -> Dict[str, Any]:
    """
    Assemble the audience index.

    Parameters
    ----------
    blobs : List[Tuple[str, int]]
        The blobs to index, in order.
    data_offset : int
        The number of bytes preceding the first record in each blob.

    Returns
    -------
    Dict[str, Any]
        The index, with a ``total`` record count and per blob details.
    """
    entries: List[Dict[str, Any]] = []
    total = 0

    for name, size in blobs:
        payload = size - data_offset
        if payload < 0 or payload % MAID_RECORD_BYTES != 0:
            raise ValueError(
                "Audience blob {name} is {size} bytes, which is not a whole "
                "number of {record} byte records after a {offset} byte "
                "header. The audience is not in the expected "
                "layout.".format(
                    name=name,
                    size=size,
                    record=MAID_RECORD_BYTES,
                    offset=data_offset,
                )
            )

        record_count = payload // MAID_RECORD_BYTES
        entries.append(
            {
                "name": name,
                "size": size,
                "data_offset": data_offset,
                "record_count": record_count,
            }
        )
        total += record_count

    return {"blobs": entries, "total": total, "record_bytes": MAID_RECORD_BYTES}


def build_audience_index(
    conn_str: str,
    container_name: str,
    blob_prefix: str,
) -> Dict[str, Any]:
    """
    Build an index of the finalized audience blobs.

    The index is built from blob metadata only (name and size), so the memory
    footprint stays proportional to the number of blobs in the folder and not
    to the size of the audience.

    Parameters
    ----------
    conn_str : str
        Connection string, or the name of the environment variable holding it.
    container_name : str
        The container holding the audience blobs.
    blob_prefix : str
        Prefix to index, e.g. ``audiences/{audience_id}/{iso_date}``.

    Returns
    -------
    Dict[str, Any]
        The index, with a ``total`` record count and per blob details.
    """
    found = _list_blob_sizes(conn_str, container_name, blob_prefix)
    if not found:
        return {"blobs": [], "total": 0, "record_bytes": MAID_RECORD_BYTES}

    blobs = _select_blobs(found)
    if not blobs:
        return {"blobs": [], "total": 0, "record_bytes": MAID_RECORD_BYTES}

    probed = _probe_data_offset(conn_str, container_name, blobs[0][0])
    data_offset = choose_data_offset([size for _, size in blobs], probed)

    if data_offset is None:
        raise ValueError(
            "Audience blobs under {prefix} do not share a consistent record "
            "grid; expected every blob to hold whole {record} byte "
            "records.".format(prefix=blob_prefix, record=MAID_RECORD_BYTES)
        )

    return _build_index(blobs, data_offset)


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesUtils_indexAudienceBlobs(
    ingress: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Index the finalized audience blobs so they can be paged without a database.

    ingress:
        {
            "conn_str": "AzureWebJobsStorage",
            "container_name": "general",
            "blob_prefix": "audiences/{audience_id}/{iso_date}",
        }
    """
    return build_audience_index(
        conn_str=ingress["conn_str"],
        container_name=ingress["container_name"],
        blob_prefix=ingress["blob_prefix"],
    )
