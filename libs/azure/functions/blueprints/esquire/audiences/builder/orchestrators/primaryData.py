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
                # Activity that queries the DB and streams the results to a blob(s).
                pass

    return ingress
