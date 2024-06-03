# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/primaryData.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)

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
            "dataFilter": str
        }
    }
    """

    # Retrieve the input data for the orchestration
    ingress = context.get_input()

    # Check if the audience has a data source
    if ingress["audience"].get("dataSource"):
        # Generate a primary data set based on the data source type
        match MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]]["dbType"]:
            case "synapse":
                ingress["results"] = yield context.call_activity(
                    "synapse_activity_cetas",
                    {
                        "instance_id": ingress["instance_id"],
                        **MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]],
                        "destination": {
                            "conn_str": ingress["working"]["conn_str"],
                            "container_name": ingress["working"]["container_name"],
                            "blob_prefix": "{}/{}/{}/-1".format(
                                ingress["working"]["blob_prefix"],
                                ingress["instance_id"],
                                ingress["audience"]["id"],
                            ),
                            "handle": "sa_esqdevdurablefunctions",  # will need to change at some point
                            "format": "CSV",
                        },
                        "query": """
                    SELECT * FROM {}{}
                    WHERE {}
                """.format(
                            (
                                "[{}].".format(
                                    MAPPING_DATASOURCE[
                                        ingress["audience"]["dataSource"]["id"]
                                    ]["table"]["schema"]
                                )
                                if MAPPING_DATASOURCE[
                                    ingress["audience"]["dataSource"]["id"]
                                ]["table"].get("schema", None)
                                else ""
                            ),
                            "["
                            + MAPPING_DATASOURCE[
                                ingress["audience"]["dataSource"]["id"]
                            ]["table"]["name"]
                            + "]",
                            ingress["audience"]["dataFilter"],
                        ),
                    },
                )
            case "postgres":
                ingress["results"] = yield context.call_sub_orchestrator(
                    "orchestrator_azurePostgres_queryToBlob",
                    {
                        "source": {
                            "bind": MAPPING_DATASOURCE[
                                ingress["audience"]["dataSource"]["id"]
                            ]["bind"],
                            "query": "SELECT * FROM {}{} WHERE {}".format(
                                (
                                    '"{}".'.format(
                                        MAPPING_DATASOURCE[
                                            ingress["audience"]["dataSource"]["id"]
                                        ]["table"]["schema"]
                                    )
                                    if MAPPING_DATASOURCE[
                                        ingress["audience"]["dataSource"]["id"]
                                    ]["table"].get("schema", None)
                                    else ""
                                ),
                                '"'
                                + MAPPING_DATASOURCE[
                                    ingress["audience"]["dataSource"]["id"]
                                ]["table"]["name"]
                                + '"',
                                ingress["audience"]["dataFilter"],
                            ),
                        },
                        "destination": {
                            "conn_str": ingress["working"]["conn_str"],
                            "container_name": ingress["working"]["container_name"],
                            "blob_prefix": "{}/{}/{}/-1".format(
                                ingress["working"]["blob_prefix"],
                                ingress["instance_id"],
                                ingress["audience"]["id"],
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
