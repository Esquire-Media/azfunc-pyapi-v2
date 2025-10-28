from __future__ import annotations

import copy
import logging
from typing import Any, Dict, List, Optional, TypedDict

from azure.durable_functions import (
    Blueprint,
    DurableOrchestrationContext,
    RetryOptions,
)

bp = Blueprint()


# ========= Typed structures with explicit REQUIRED keys =========


class Advertiser(TypedDict, total=False):
    meta: str
    xandr: str


class DataSource(TypedDict, total=False):
    id: str
    dataType: Dict[str, Any]


# Audience requires dataSource for this orchestrator
class AudienceRequired(TypedDict):
    dataSource: DataSource


class Audience(AudienceRequired, total=False):
    id: str
    advertiser: Advertiser
    status: bool
    rebuildSchedule: str
    TTL_Length: int
    TTL_Unit: str
    dataFilter: str
    processing: Dict[str, Any]


# Storage target MUST have these three keys
class StorageTargetRequired(TypedDict):
    conn_str: str
    container_name: str
    blob_prefix: str


class StorageTarget(StorageTargetRequired, total=False):
    data_source: Optional[str]


# Ingress requires working, destination, audience
class IngressRequired(TypedDict):
    working: StorageTarget
    destination: StorageTarget
    audience: Audience


class Ingress(IngressRequired, total=False):
    results: List[Any]
    query: Any
    instance_id: str


# ========= Helpers =========


def _validate_ingress(i: object) -> Ingress:
    """
    Deterministic validation (no I/O, time, or randomness).
    Raises ValueError with stable messages for invalid shapes.
    Accepts 'object' so callers can pass Any|None without type errors; we narrow here.
    """
    if not isinstance(i, dict):
        # Handles None or non-dict Any
        raise ValueError("Ingress must be a dict.")

    # Validate buckets
    for key in ("working", "destination"):
        if key not in i or not isinstance(i[key], dict):
            raise ValueError(f"Missing or invalid '{key}' in ingress.")

        bucket = i[key]
        for sub in ("conn_str", "container_name", "blob_prefix"):
            if (
                sub not in bucket
                or not isinstance(bucket[sub], str)
                or not bucket[sub].strip()
            ):
                raise ValueError(f"Missing or invalid '{key}.{sub}' in ingress.")

    # Validate audience and dataSource
    if "audience" not in i or not isinstance(i["audience"], dict):
        raise ValueError("Missing or invalid 'audience' in ingress.")

    audience = i["audience"]
    if "dataSource" not in audience or not isinstance(audience["dataSource"], dict):
        raise ValueError(
            "Primary data source is not set (audience.dataSource missing)."
        )

    # If we get here, the shape satisfies our required keys.
    # Cast to our Ingress TypedDict to satisfy type checkers.
    return i  # type: ignore[return-value]


def _prefix_once(prefix: str, path: str) -> str:
    """
    Idempotently ensures 'prefix/' is at the start of 'path'.
    No double-prefixing on replays or retries.
    """
    if not prefix:
        return path
    normalized_prefix = prefix.strip("/")

    if path == normalized_prefix or path.startswith(normalized_prefix + "/"):
        return path
    return f"{normalized_prefix}/{path.lstrip('/')}"


def _non_replay_log(
    context: DurableOrchestrationContext, level: int, message: str
) -> None:
    """Log only when not replaying to keep logs deterministic and tidy."""
    if not context.is_replaying:
        logging.log(level, message)


# ========= Orchestrator =========


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_builder(context: DurableOrchestrationContext):
    """
    Deterministic & idempotent-friendly orchestrator for building Esquire audiences.

    Steps:
      1) Validate & copy input; attach instance_id; idempotently prefix working path
      2) Sub-orchestrator: primary data
      3) Sub-orchestrator: processing steps
      4) Sub-orchestrator: finalize (ensure device IDs as the final data type)
      5) Return final ingress
    """
    # -------- 1) Validate & prepare state (deterministic) --------
    raw_ingress: object = (
        context.get_input()
    )  # Any | None -> object; we narrow in _validate_ingress
    ingress: Ingress = _validate_ingress(copy.deepcopy(raw_ingress))
    ingress["instance_id"] = context.instance_id  # set deterministically

    # Idempotently adjust working prefix to include instance id (no double prefix on replay)
    working_prefix: str = ingress["working"]["blob_prefix"]
    ingress["working"]["blob_prefix"] = _prefix_once(
        context.instance_id, working_prefix
    )

    _non_replay_log(
        context,
        logging.INFO,
        f"[builder] instance={context.instance_id} working_prefix={ingress['working']['blob_prefix']}",
    )

    # Deterministic child instance IDs to prevent duplicates on replay/restart
    primary_child_id = f"{context.instance_id}:primary"
    processing_child_id = f"{context.instance_id}:processing"
    finalize_child_id = f"{context.instance_id}:finalize"

    # -------- 2) Primary dataset generation --------
    ingress = yield context.call_sub_orchestrator(
        "orchestrator_esquireAudiences_primaryData",
        input_=ingress,
        instance_id=primary_child_id,
    )

    results = ingress.get("results") or []
    if not isinstance(results, list) or not results:
        raise Exception("No results from primary data query.")

    # Capture the query in a local deterministic variable if needed later
    query_snapshot = ingress.get("query", None)

    _non_replay_log(
        context,
        logging.INFO,
        f"[builder] primary completed: results={len(results)}",
    )

    # -------- 3) Processing steps --------
    ingress = yield context.call_sub_orchestrator(
        "orchestrator_esquireAudiences_processingSteps",
        input_=ingress,
        instance_id=processing_child_id,
    )

    results = ingress.get("results") or []
    if not isinstance(results, list) or not results:
        raise Exception("No results after processing steps completed.")

    _non_replay_log(
        context,
        logging.INFO,
        f"[builder] processing completed: results={len(results)}",
    )

    # -------- 4) Finalize (ensure Device IDs are final type) --------
    ingress = yield context.call_sub_orchestrator(
        "orchestrator_esquireAudiences_finalize",
        input_=ingress,
        instance_id=finalize_child_id,
    )

    results = ingress.get("results") or []
    if not isinstance(results, list) or not results:
        raise Exception("No final results.")

    # Restore the original query value deterministically if the finalize step overwrote it
    ingress["query"] = query_snapshot  # ok; query is optional

    _non_replay_log(
        context,
        logging.INFO,
        f"[builder] finalize completed: results={len(results)}",
    )

    # -------- 5) Return final ingress --------
    return ingress
