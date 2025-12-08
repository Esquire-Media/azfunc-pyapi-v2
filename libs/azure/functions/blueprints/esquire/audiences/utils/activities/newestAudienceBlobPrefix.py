from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, Dict, Any

from azure.durable_functions import Blueprint
from azure.storage.blob import ContainerClient

bp: Blueprint = Blueprint()


def _parse_iso_dir_name(dir_name: str) -> Optional[datetime]:
    """
    Try to parse a directory name as an ISO-8601 date/datetime.

    Accepts:
    - Date-only (e.g. '2025-01-31')
    - Datetime (e.g. '2025-01-31T12:34:56')
    - Datetime with 'Z' (e.g. '2025-01-31T12:34:56Z')

    Returns a datetime if parsing succeeds, otherwise None.
    """
    candidate = dir_name

    # Handle trailing 'Z' used to indicate UTC in ISO-8601
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesUtils_newestAudienceBlobPrefix(
    ingress: Dict[str, Any]
) -> Optional[str]:
    """
    Find the newest ISO-date directory prefix for the given audience.

    ingress = {
        "conn_str": "ESQUIRE_AUDIENCE_CONN_STR" or raw connection string,
        "container_name": "general",
        "audience_id": "<audience-id>",
    }

    Expected blob layout:
      audiences/{audience_id}/{iso_date_or_datetime}/...

    Only directory names that are valid ISO dates/datetimes are considered.
    """
    container_client = ContainerClient.from_connection_string(
        conn_str=os.environ.get(ingress["conn_str"], ingress["conn_str"]),
        container_name=ingress["container_name"],
    )

    audience_prefix = f"audiences/{ingress['audience_id']}/"

    most_recent_blob_prefix: Optional[str] = None
    most_recent_dt: Optional[datetime] = None

    # Iterate lazily over blobs with the given audience prefix
    for blob in container_client.list_blobs(name_starts_with=audience_prefix):
        parts = blob.name.split("/")
        # Expect at least: ["audiences", "{audience_id}", "{date_dir}", ...]
        if len(parts) < 3:
            continue

        date_dir = parts[2]
        parsed_dt = _parse_iso_dir_name(date_dir)
        # Skip any directory that is not a valid ISO date/datetime
        if parsed_dt is None:
            continue

        if most_recent_dt is None or parsed_dt > most_recent_dt:
            most_recent_dt = parsed_dt
            # audiences/{audience_id}/{date_dir}
            most_recent_blob_prefix = "/".join(parts[:3])

    return most_recent_blob_prefix
