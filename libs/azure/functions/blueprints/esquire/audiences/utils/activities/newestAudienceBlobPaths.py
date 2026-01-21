from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from azure.durable_functions import Blueprint
from azure.storage.blob import ContainerClient

bp: Blueprint = Blueprint()


def _get_required_str(ingress: Dict[str, Any], key: str) -> str:
    value = ingress.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"ingress[{key!r}] must be a non-empty string")
    return value


def _resolve_conn_str(conn_str_or_env: str) -> str:
    # If ingress["conn_str"] is an env var name, use it; otherwise treat it as the raw conn str.
    return os.environ.get(conn_str_or_env, conn_str_or_env)


def _normalize_to_utc(dt: datetime) -> datetime:
    """
    Ensure dt is timezone-aware and normalized to UTC to allow safe comparisons.
    If dt is naive, assume UTC.
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_iso_dir_name(dir_name: str) -> Optional[datetime]:
    """
    Try to parse a directory name as an ISO-8601 date/datetime.

    Accepts:
    - Date-only (e.g. '2025-01-31')
    - Datetime (e.g. '2025-01-31T12:34:56')
    - Datetime with 'Z' (e.g. '2025-01-31T12:34:56Z')

    Returns a UTC-aware datetime if parsing succeeds, otherwise None.
    """
    candidate = dir_name

    # Handle trailing 'Z' used to indicate UTC in ISO-8601
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None

    return _normalize_to_utc(parsed)


def _is_directory_marker_blob(blob_name: str) -> bool:
    """
    Some tools create zero-length blobs that act like "folders" and end with '/'.
    We must not return those.
    """
    return blob_name.endswith("/")


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesUtils_newestAudienceBlobPaths(
    ingress: Dict[str, Any],
) -> List[str]:
    """
    Return blob paths under the newest ISO-date/datetime directory for the given audience,
    excluding any directory marker blobs (names ending in '/').

    ingress = {
        "conn_str": "ESQUIRE_AUDIENCE_CONN_STR" or raw connection string,
        "container_name": "general",
        "audience_id": "<audience-id>",
    }

    Expected blob layout:
      audiences/{audience_id}/{iso_date_or_datetime}/...

    Only directory names that are valid ISO dates/datetimes are considered.
    """
    conn_str_raw = _get_required_str(ingress, "conn_str")
    container_name = _get_required_str(ingress, "container_name")
    audience_id = _get_required_str(ingress, "audience_id")

    audience_prefix = f"audiences/{audience_id}/"

    most_recent_dt: Optional[datetime] = None
    most_recent_date_dir: Optional[str] = None

    # Pass 1: find newest ISO directory (considering even directory-marker blobs if they exist)
    with ContainerClient.from_connection_string(
        conn_str=_resolve_conn_str(conn_str_raw),
        container_name=container_name,
    ) as container_client:
        for blob in container_client.list_blobs(name_starts_with=audience_prefix):
            parts = blob.name.split("/")
            # Expect at least: ["audiences", "{audience_id}", "{date_dir}", ...]
            if len(parts) < 3:
                continue

            date_dir = parts[2]
            parsed_dt = _parse_iso_dir_name(date_dir)
            if parsed_dt is None:
                continue

            if most_recent_dt is None or parsed_dt > most_recent_dt:
                most_recent_dt = parsed_dt
                most_recent_date_dir = date_dir

        if most_recent_date_dir is None:
            return []

        # Pass 2: list blobs under newest directory, excluding directory-marker blobs
        newest_prefix = f"{audience_prefix}{most_recent_date_dir}/"
        results: List[str] = []
        for blob in container_client.list_blobs(name_starts_with=newest_prefix):
            if _is_directory_marker_blob(blob.name):
                continue
            results.append(blob.name)

        return results
