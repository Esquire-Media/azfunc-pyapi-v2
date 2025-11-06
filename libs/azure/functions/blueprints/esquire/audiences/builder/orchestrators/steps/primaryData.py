from __future__ import annotations

from typing import Dict, Any, List, Optional
import hashlib

from azure.durable_functions import Blueprint, DurableOrchestrationContext

from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    extract_tenant_id_from_datafilter,
    extract_fields_from_dataFilter,
    extract_daysback_from_dataFilter,
)

bp = Blueprint()


# ----------------------------
# Pure helpers (deterministic)
# ----------------------------
def _compute_storage_handle_from_conn_str(conn_str: str) -> str:
    """
    Deterministically derive a stable storage handle from a connection string (or an env-var key).

    - If the string contains 'AccountName=', extract it.
    - Otherwise, fall back to a stable hex digest of the provided string.

    This avoids reading environment variables or instantiating SDK clients in the orchestrator,
    which would violate Durable Functions determinism requirements.
    """
    account_name: Optional[str] = None
    marker = "AccountName="

    if marker in conn_str:
        # Typical format: "DefaultEndpointsProtocol=...;AccountName=foo;AccountKey=...;EndpointSuffix=core.windows.net"
        for part in conn_str.split(";"):
            p = part.strip()
            if p.startswith(marker):
                account_name = p[len(marker) :].strip()
                break

    if not account_name:
        # Use a stable deterministic digest of whatever was provided.
        digest = hashlib.sha256(conn_str.encode("utf-8")).hexdigest()[:16]
        account_name = digest

    return f"sa_{account_name}"


def _normalize_datatype_for_format(data_type: str) -> str:
    """
    Deterministically map dataType to CETAS/CSV format.
    """
    dt = (data_type or "").strip().lower()
    return "CSV_HEADER" if dt == "addresses" else "CSV"


def _build_synapse_query(ds_cfg: Dict[str, Any], where_clause: str) -> str:
    """
    Deterministically build a Synapse SELECT query string.
    """
    select_clause = ds_cfg.get("query", {}).get("select", "*")

    table_cfg = ds_cfg.get("table", {})
    schema = table_cfg.get("schema")
    schema_prefix = f"[{schema}]." if schema else ""
    table_name = f"[{table_cfg.get('name', '').strip()}]"

    parts = [
        "SELECT",
        select_clause,
        "FROM",
        f"{schema_prefix}{table_name}",
        "WHERE",
        where_clause,
    ]
    # Single-space join to avoid newline/whitespace differences across replays
    return " ".join(parts)


def _build_postgres_query(ds_cfg: Dict[str, Any], where_clause: str) -> str:
    """
    Deterministically build a Postgres SELECT query string for non-EAV sources.
    """
    table_cfg = ds_cfg.get("table", {})
    schema = table_cfg.get("schema")
    schema_prefix = f"\"{schema}\"." if schema else ""
    table_name = f"\"{table_cfg.get('name', '').strip()}\""
    return f"SELECT * FROM {schema_prefix}{table_name} WHERE {where_clause}"


def _stable_sorted_urls(urls: List[str]) -> List[str]:
    """
    Deterministically sort a list of URLs to ensure stable fan-out ordering.
    """
    # Sorting ensures that task_all always fans out in the same order.
    # This prevents subtle non-determinism if the upstream activity returns URLs in arbitrary order.
    return sorted(urls)


# ---------------------------------------
# Orchestrator (deterministic & idempotent)
# ---------------------------------------
@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_primaryData(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the generation of primary data sets for Esquire audiences.

    Determinism safeguards:
      - Uses only data from input/context for branching.
      - No environment variable or SDK calls inside orchestrator.
      - No real-time clock access except context.current_utc_datetime (replay-safe).
      - No dict/set iteration order dependence.
      - Stable string building and whitespace.
      - Stable fan-out ordering (sorted URLs).

    Expected format for context.get_input():
    {
        "instance_id": str,
        "source": {"conn_str": str, "container_name": str, "blob_prefix": str},
        "working": {"conn_str": str, "container_name": str, "blob_prefix": str, "data_source": Optional[str]},
        "destination": {"conn_str": str, "container_name": str, "blob_prefix": str},
        "audience": {
            "id": str,
            "dataSource": {"id": str, "dataType": str},
            "dataFilter": str,
            "processing": dict,
            "TTL_Length": Optional[int],
            "TTL_Unit": Optional[str]
        }
    }
    """
    # 1) All inputs must come from context (replay-safe)
    ingress: Dict[str, Any] = context.get_input() or {}

    audience: Dict[str, Any] = ingress.get("audience", {}) or {}
    data_source: Optional[Dict[str, Any]] = audience.get("dataSource")
    if not data_source:
        # Nothing to do; return ingress unchanged to remain idempotent.
        return ingress

    ds_id: str = data_source.get("id", "")
    ds_cfg: Dict[str, Any] = MAPPING_DATASOURCE.get(ds_id, {}) or {}

    db_type: str = (ds_cfg.get("dbType") or "").strip().lower()
    data_type: str = (data_source.get("dataType") or "").strip().lower()

    # Defensive: ensure required blocks exist to avoid KeyErrors during replay.
    working: Dict[str, Any] = ingress.get("working", {}) or {}
    audience_filter: str = audience.get("dataFilter", "") or ""

    # Pre-compute a deterministic handle for any downstream storage usage.
    # If a handle is explicitly given in ingress["working"]["data_source"], use that; else compute from the conn_str.
    storage_handle: str = working.get(
        "data_source",
        _compute_storage_handle_from_conn_str(working.get("conn_str", "")),
    )

    # Initialize fields we may enrich (deterministically)
    ingress_query: Optional[str] = None
    ingress_results: Optional[List[str]] = None

    # 2) Deterministic branching based only on input/config
    if db_type == "synapse":
        # 2a) Build query purely from inputs/config
        query = _build_synapse_query(ds_cfg, audience_filter)

        # Optional TTL-based filter function — must be a pure function of provided args.
        filter_fn: str = ds_cfg.get("query", {}).get("filter")
        if callable(filter_fn):
            ttl_len = audience.get("TTL_Length")
            ttl_unit = audience.get("TTL_Unit")
            # The function must be pure/deterministic wrt inputs; we don't call any non-deterministic API here.
            query += filter_fn(ttl_len, ttl_unit)

        ingress_query = query

        # 2b) Call activity (all I/O happens outside orchestrator)
        format_str = _normalize_datatype_for_format(data_type)
        ingress_results = yield context.call_activity(
            "synapse_activity_cetas",
            {
                # IDs and config are all deterministic inputs.
                "instance_id": ingress.get("instance_id"),
                **ds_cfg,
                "destination": {
                    "conn_str": working.get("conn_str"),
                    "container_name": working.get("container_name"),
                    "blob_prefix": f"{working.get('blob_prefix')}/-1",
                    "handle": storage_handle,
                    "format": format_str,
                },
                "query": ingress_query,
                "return_urls": True,
            },
        )

    elif db_type == "postgres":
        is_eav: bool = bool(ds_cfg.get("isEAV", False))

        if is_eav:
            # Generate EAV sales query inside an activity — pass only deterministic inputs.
            ingress_query = yield context.call_activity(
                "activity_esquireAudienceBuilder_generateSalesAudiencePrimaryQuery",
                {
                    **ingress,  # safe: this is the original deterministic input payload
                    **ds_cfg,
                    "tenant_id": extract_tenant_id_from_datafilter(audience_filter),
                    "fields": extract_fields_from_dataFilter(audience_filter),
                    # Replay-safe time via context (Durable Functions logs/replays guarantee determinism).
                    "utc_now": str(context.current_utc_datetime),
                    "days_back": extract_daysback_from_dataFilter(audience_filter),
                },
            )
        else:
            ingress_query = _build_postgres_query(ds_cfg, audience_filter)

        # Execute the query to blob once (no duplicate calls)
        ingress_results = yield context.call_sub_orchestrator(
            "orchestrator_azurePostgres_queryToBlob",
            {
                "source": {
                    "bind": ds_cfg.get("bind"),
                    "query": ingress_query,
                },
                "destination": {
                    "conn_str": working.get("conn_str"),
                    "container_name": working.get("container_name"),
                    "blob_prefix": f"{working.get('blob_prefix')}/-1",
                    "format": "CSV",
                },
            },
        )

        # Optionally post-process polygons in a deterministic, stable order.
        if data_type == "polygons" and ingress_results:
            stable_urls = _stable_sorted_urls(list(ingress_results))
            # Fan-out in a stable order to avoid non-deterministic scheduling differences.
            tasks = [
                context.call_activity(
                    "activity_esquireAudienceBuilder_formatPolygons",
                    {
                        "source": url,
                        "destination": working,
                    },
                )
                for url in stable_urls
            ]
            # task_all will preserve the order of provided tasks; because we sorted, this is deterministic.
            ingress_results = yield context.task_all(tasks)

    # 3) Write back enriched fields in a single place (dict order preserved since 3.7+)
    if ingress_query is not None:
        ingress["query"] = ingress_query
    if ingress_results is not None:
        ingress["results"] = ingress_results

    # 4) Return the updated ingress — still idempotent. Replaying will reproduce the same decisions & calls.
    return ingress
