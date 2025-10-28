from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List

from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()


def _normalized_ext(fmt: str) -> str:
    """Map a logical format to a blob file extension deterministically."""
    fmt_upper = (fmt or "").upper()
    match fmt_upper:
        case "CSV":
            return "csv"
        case _:
            # Keep the original but normalized lowercased as a fallback.
            return fmt.lower() if fmt else "dat"


def _stable_chunk_blob_name(
    instance_id: str,
    blob_prefix: str,
    fmt: str,
    query: str,
    offset: int,
    limit: int,
) -> str:
    """
    Produce a deterministic blob name for a given chunk.

    We combine:
      - Orchestration instance_id (stable for the life of the orchestrator)
      - A short hash of the query text (protects against extremely long names and changes)
      - Paging parameters (offset/limit) to make each chunk unique & stable

    This makes activity retries idempotent and ensures the same blob name per chunk.
    """
    # Short, deterministic hash of the query for uniqueness without verbosity
    qhash = hashlib.sha1(query.encode("utf-8")).hexdigest()[:12]
    ext = _normalized_ext(fmt)
    # Zero-pad offset for lexicographic order when listing blobs
    return f"{blob_prefix}/df-{instance_id}-{qhash}-o{offset:012d}-l{limit}.{ext}"


@bp.orchestration_trigger(context_name="context")
def orchestrator_azurePostgres_queryToBlob(context: DurableOrchestrationContext):
    """
    Orchestrates paged export of a Postgres query to Azure Blob Storage, deterministically.

    Expected ingress:
    {
        "source": {
            "bind": "BIND_HANDLE",
            "query": "SELECT * FROM table"
        },
        "destination": {
            "conn_str": "YOUR_AZURE_CONNECTION_STRING_ENV_VARIABLE",
            "container_name": "your-azure-blob-container",
            "blob_prefix": "blob/prefix",
            "format": "CSV"
        },
        "limit": 1000  // optional, default 1000
    }
    """
    ingress: Dict[str, Any] = context.get_input() or {}

    if not context.is_replaying:
        logging.info("[orchestrator] Starting export to blob (deterministic).")

    # Validate minimal required inputs (pure/deterministic checks)
    src = ingress.get("source") or {}
    dst = ingress.get("destination") or {}
    query = src.get("query")
    if not query or not isinstance(query, str):
        raise ValueError("source.query is required and must be a string.")
    if not src.get("bind"):
        raise ValueError("source.bind is required.")
    if not dst.get("conn_str") or not dst.get("container_name") or not dst.get("blob_prefix"):
        raise ValueError("destination.conn_str, destination.container_name, and destination.blob_prefix are required.")

    fmt = (dst.get("format") or "CSV").upper()

    # 1) Get total record count (pure activity)
    if not context.is_replaying:
        logging.info("[orchestrator] Getting record count...")
    count: int = yield context.call_activity("activity_azurePostgres_getRecordCount", src)

    # 2) Create deterministic chunk payloads
    limit: int = int(ingress.get("limit", 1000))
    if limit <= 0:
        raise ValueError("limit must be a positive integer.")

    offsets = list(range(0, count, limit))

    if not context.is_replaying:
        logging.info(
            f"[orchestrator] Preparing {len(offsets)} chunk(s) with limit={limit} for count={count}."
        )

    # Build stable blob names for each chunk so retries are idempotent
    instance_id = context.instance_id
    chunk_calls = []
    for i in offsets:
        blob_name = _stable_chunk_blob_name(
            instance_id=instance_id,
            blob_prefix=dst["blob_prefix"],
            fmt=fmt,
            query=query,
            offset=i,
            limit=limit,
        )
        # Pass down a fully specified activity ingress with deterministic blob_name
        activity_ingress = {
            "source": src,
            "destination": {
                **dst,
                "format": fmt,
            },
            "limit": limit,
            "offset": i,
            "blob_name": blob_name,  # <- deterministic name used by the activity
        }
        chunk_calls.append(
            context.call_activity("activity_azurePostgres_resultToBlob", activity_ingress)
        )

    # 3) Fan-out/fan-in deterministically; task_all preserves order of the list we created.
    if not context.is_replaying:
        logging.info("[orchestrator] Dispatching chunk activities...")
    urls: List[str] = yield context.task_all(chunk_calls)

    if not context.is_replaying:
        logging.info("[orchestrator] Export complete.")

    return urls
