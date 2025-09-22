# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/primaryData.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from azure.storage.blob import BlobServiceClient
from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import extract_tenant_id_from_datafilter, extract_fields_from_dataFilter
import os
# import logging
bp = Blueprint()


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
        # Generate a primary data set based on the data source type
        match MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]]["dbType"]:
            case "synapse":
                blob_storage = BlobServiceClient.from_connection_string(
                    os.environ.get(
                        ingress["working"]["conn_str"],
                        ingress["working"]["conn_str"],
                    )
                )
                ingress["query"] = "SELECT {} FROM {}{} WHERE {}".format(
                    MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                        "query"
                    ].get("select", "*"),
                    (
                        "[{}].".format(
                            MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                                "table"
                            ]["schema"]
                        )
                        if MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                            "table"
                        ].get("schema", None)
                        else ""
                    ),
                    "["
                    + MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                        "table"
                    ]["name"]
                    + "]",
                    ingress["audience"]["dataFilter"],
                )
                if filter_fn := MAPPING_DATASOURCE[
                    ingress["audience"]["dataSource"]["id"]
                ]["query"].get("filter"):
                    ingress["query"] += filter_fn(
                        ingress["audience"]["TTL_Length"],
                        ingress["audience"]["TTL_Unit"],
                    )
                ingress["results"] = yield context.call_activity(
                    "synapse_activity_cetas",
                    {
                        "instance_id": ingress["instance_id"],
                        **MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]],
                        "destination": {
                            "conn_str": ingress["working"]["conn_str"],
                            "container_name": ingress["working"]["container_name"],
                            "blob_prefix": "{}/-1".format(
                                ingress["working"]["blob_prefix"],
                            ),
                            "handle": ingress["working"].get(
                                "data_source",
                                "sa_{}".format(blob_storage.account_name),
                            ),
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
                # logging.warning("[LOG] Taking postgres branch")
                # get query to handle anything hooking into the sales data (because of EAV setup)
                if MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]].get("isEAV", False):
                    # logging.warning("[LOG] Taking EAV sales branch")
                    # logging.warning("[LOG] Calling generateSalesAudiencePrimaryQuery activity")
                    ingress["query"] = yield context.call_activity(
                        "activity_esquireAudienceBuilder_generateSalesAudiencePrimaryQuery",
                        {
                            **ingress,
                            **MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]],
                            "tenant_id": extract_tenant_id_from_datafilter(ingress["audience"]["dataFilter"]),
                            "fields": extract_fields_from_dataFilter(ingress["audience"]["dataFilter"]),
                            "utc_now": str(context.current_utc_datetime)
                            }
                    )
                    # logging.warning(f"generateSalesAudiencePrimaryQuery returned query: {ingress['query']!r}")
                    # logging.warning(f"[LOG] Getting ready to send to blob.\nConn_str: {ingress['working']['conn_str']}\nContainer name':{ingress['working']['container_name']}")

                    ingress["results"] = yield context.call_sub_orchestrator(
                        "orchestrator_azurePostgres_queryToBlob",
                        {
                            "source": {
                                "bind": MAPPING_DATASOURCE[
                                    ingress["audience"]["dataSource"]["id"]
                                ]["bind"],
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

                else:
                    ingress["query"] = "SELECT * FROM {}{} WHERE {}".format(
                        (
                            '"{}".'.format(
                                MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                                    "table"
                                ]["schema"]
                            )
                            if MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                                "table"
                            ].get("schema", None)
                            else ""
                        ),
                        '"'
                        + MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]][
                            "table"
                        ]["name"]
                        + '"',
                        ingress["audience"]["dataFilter"],
                    )
                ingress["results"] = yield context.call_sub_orchestrator(
                    "orchestrator_azurePostgres_queryToBlob",
                    {
                        "source": {
                            "bind": MAPPING_DATASOURCE[
                                ingress["audience"]["dataSource"]["id"]
                            ]["bind"],
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
                # Check the data type and format polygons if necessary
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
