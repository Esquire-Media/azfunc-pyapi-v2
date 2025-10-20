# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/primaryData.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    extract_tenant_id_from_datafilter,
    extract_fields_from_dataFilter,
    extract_daysback_from_dataFilter,
)
import hashlib

# import logging
bp = Blueprint()


def _compute_storage_handle_from_conn_str(conn_str: str) -> str:
    """
    Deterministically derive a stable storage handle from a connection string (or an env-var key).

    - If the string contains 'AccountName=', extract it.
    - Otherwise, fall back to a stable hex digest of the provided string.

    This avoids reading environment variables or instantiating SDK clients in the orchestrator,
    which would violate Durable Functions determinism requirements.
    """
    account_name = None
    marker = "AccountName="
    if marker in conn_str:
        # Typical format: "DefaultEndpointsProtocol=...;AccountName=foo;AccountKey=...;EndpointSuffix=core.windows.net"
        # Split on ';' to find the AccountName component.
        for part in conn_str.split(";"):
            part = part.strip()
            if part.startswith(marker):
                account_name = part[len(marker) :].strip()
                break

    if not account_name:
        # Use a stable deterministic digest of whatever was provided.
        digest = hashlib.sha256(conn_str.encode("utf-8")).hexdigest()[:16]
        account_name = digest

    return f"sa_{account_name}"


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_primaryData(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the generation of primary data sets for Esquire audiences.

    This orchestrator processes the data source specified for an audience, executes the necessary queries, and stores the results in the specified storage location.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    dict: The updated ingress data with the results of the data processing.

    Expected format for context.get_input():
    {
        "source": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "working": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "destination": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "audience": {
            "id": str,
            "dataSource": {
                "id": str,
                "dataType": str
            },
            "dataFilter": str,
            "processing": dict
        }
    }
    """

    # Retrieve the input data for the orchestration
    ingress = context.get_input()

    # logging.warning(f"[LOG] ingress before PrimaryData: {ingress}")

    # Check if the audience has a data source
    if ingress["audience"].get("dataSource"):
        ds_id = ingress["audience"]["dataSource"]["id"]
        ds_cfg = MAPPING_DATASOURCE[ds_id]

        match ds_cfg["dbType"]:
            case "synapse":
                # Build the query deterministically
                select_clause = ds_cfg["query"].get("select", "*")
                schema_prefix = ""
                table_cfg = ds_cfg["table"]
                if table_cfg.get("schema"):
                    schema_prefix = f"[{table_cfg['schema']}]."
                table_name = f"[{table_cfg['name']}]"
                where_clause = ingress["audience"]["dataFilter"]

                ingress["query"] = " ".join(
                    [
                        "SELECT",
                        select_clause,
                        "FROM",
                        f"{schema_prefix}{table_name}",
                        "WHERE",
                        where_clause,
                    ]
                )

                # Optional TTL-based filter from config (pure function)
                if filter_fn := ds_cfg["query"].get("filter"):
                    ingress["query"] += filter_fn(
                        ingress["audience"]["TTL_Length"],
                        ingress["audience"]["TTL_Unit"],
                    )

                # Compute a deterministic storage handle without env or SDK calls
                handle = ingress["working"].get(
                    "data_source",
                    _compute_storage_handle_from_conn_str(
                        ingress["working"]["conn_str"]
                    ),
                )

                # Ship to Synapse activity (I/O occurs in activity, not orchestrator)
                ingress["results"] = yield context.call_activity(
                    "synapse_activity_cetas",
                    {
                        "instance_id": ingress["instance_id"],
                        **ds_cfg,
                        "destination": {
                            "conn_str": ingress["working"]["conn_str"],
                            "container_name": ingress["working"]["container_name"],
                            "blob_prefix": "{}/-1".format(
                                ingress["working"]["blob_prefix"]
                            ),
                            "handle": handle,
                            "format": (
                                "CSV_HEADER"
                                if ingress["audience"]["dataSource"]["dataType"]
                                == "addresses"
                                else "CSV"
                            ),
                        },
                        "query": ingress["query"], 
                        "return_urls": True,
                    },
                )

            case "postgres":
                # Build query deterministically (no SDK/env access here)
                if ds_cfg.get("isEAV", False):
                    # Generate EAV sales query in an activity (safe: passes only deterministic inputs)
                    ingress["query"] = yield context.call_activity(
                        "activity_esquireAudienceBuilder_generateSalesAudiencePrimaryQuery",
                        {
                            **ingress,
                            **ds_cfg,
                            "tenant_id": extract_tenant_id_from_datafilter(
                                ingress["audience"]["dataFilter"]
                            ),
                            "fields": extract_fields_from_dataFilter(
                                ingress["audience"]["dataFilter"]
                            ),
                            # Use orchestration-provided time (deterministic)
                            "utc_now": str(context.current_utc_datetime),
                            "days_back": extract_daysback_from_dataFilter(
                                ingress["audience"]["dataFilter"]
                            ),
                        },
                    )
                else:
                    schema_prefix = ""
                    table_cfg = ds_cfg["table"]
                    if table_cfg.get("schema"):
                        schema_prefix = f"\"{table_cfg['schema']}\"."
                    table_name = f"\"{table_cfg['name']}\""
                    where_clause = ingress["audience"]["dataFilter"]
                    ingress["query"] = (
                        f"SELECT * FROM {schema_prefix}{table_name} WHERE {where_clause}"
                    )

                # Execute the query to blob once (fix duplicated call)
                ingress["results"] = yield context.call_sub_orchestrator(
                    "orchestrator_azurePostgres_queryToBlob",
                    {
                        "source": {
                            "bind": ds_cfg["bind"],
                            "query": ingress["query"],
                        },
                        "destination": {
                            "conn_str": ingress["working"]["conn_str"],
                            "container_name": ingress["working"]["container_name"],
                            "blob_prefix": "{}/-1".format(
                                ingress["working"]["blob_prefix"]
                            ),
                            "format": "CSV",
                        },
                    },
                )

                # If polygons, format them via activities deterministically
                match ingress["audience"]["dataSource"]["dataType"]:
                    case "polygons":
                        ingress["results"] = yield context.task_all(
                            [
                                context.call_activity(
                                    "activity_esquireAudienceBuilder_formatPolygons",
                                    {
                                        "source": source_url,
                                        "destination": ingress["working"],
                                    },
                                )
                                for source_url in ingress["results"]
                            ]
                        )

    # Return the updated ingress data with the results
    return ingress
