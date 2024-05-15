# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/primaryData.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_primaryData(
    context: DurableOrchestrationContext,
):
    # ingress ={
    #         "source": {
    #             "conn_str": "ONSPOT_CONN_STR",
    #             "container_name": "general",
    #             "blob_prefix": "audiences",
    #         },
    #         "working": {
    #             "conn_str": "ONSPOT_CONN_STR",
    #             "container_name": "general",
    #             "blob_prefix": "raw",
    #         },
    #         "destination": {
    #             "conn_str": "ONSPOT_CONN_STR",
    #             "container_name": "general",
    #             "blob_prefix": "audiences",
    #         },
    #         "audience": {
    #             "id": id,
    #         },
    #     },

    ingress = context.get_input()
    
    if ingress["audience"].get("dataSource"):
        # Generate a primary data set
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
                match ingress["audience"]["dataSource"]["dataType"]:
                    case "polygons":
                        ingress["results"] = yield context.task_all([
                            context.call_activity(
                                "activity_esquireAudienceBuilder_formatPolygons",
                                {
                                    "source": source_url,
                                    "destination": ingress["working"]
                                }
                            )
                            for source_url in ingress["results"]
                        ])
    return ingress
