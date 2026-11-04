"""
Blob-backed reader for audience user pages.

Kept separate from :mod:`userPage` so that the pure paging helpers stay
dependency free (stdlib only) while this module may use the Azure SDK.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.userPage import (
    parse_records,
)
from libs.utils.azure_storage import init_blob_client


def fetch_users_page(
    ingress: Dict[str, Any],
) -> Tuple[Optional[List[str]], Optional[Dict[str, Any]], int, int]:
    """
    Fetch up to `batch_size` MAIDs from the audience blobs.

    The bytes are read straight out of Blob Storage with a range request, so
    only the requested page is ever resident in memory. Peak usage is
    O(batch_size) regardless of how large the audience is.

    ingress:
        {
            "destination": {"conn_str": ..., "container_name": ...},
            "batch": {"batch_seq": 1, ...},
            "batch_size": 10000,
            "ranges": [
                {"blob_name": ..., "offset": ..., "length": ..., "records": ...},
            ],
        }

    Returns: (users or None, error or None, offset, limit)
    """
    batch = ingress["batch"]
    offset = max(int(batch["batch_seq"]) - 1, 0) * int(ingress["batch_size"])
    limit = int(ingress["batch_size"])

    ranges = ingress.get("ranges") or []
    if not ranges:
        return [], None, offset, limit

    destination = ingress["destination"]
    users: List[str] = []
    users_append = users.append

    try:
        for entry in ranges:
            blob_client = init_blob_client(
                conn_str=destination["conn_str"],
                container_name=destination["container_name"],
                blob_name=entry["blob_name"],
            )
            payload = blob_client.download_blob(
                offset=int(entry["offset"]),
                length=int(entry["length"]),
            ).readall()

            # Decode one range at a time so only a single page is resident.
            for record in parse_records(payload, int(entry["records"])):
                users_append(record)

            del payload
    except Exception as e:
        return None, {"error": {"source": "blob", "message": str(e)}}, offset, limit

    # Ensure we never exceed `limit` even if the caller asked for more.
    if len(users) > limit:
        del users[limit:]

    return users, None, offset, limit
