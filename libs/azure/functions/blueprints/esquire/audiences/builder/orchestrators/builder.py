# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/builder.py

from libs.azure.functions.blueprints.esquire.audiences.builder.config import (
    MAPPING_DATASOURCE,
)
from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_builder(
    context: DurableOrchestrationContext,
):
    ingress = context.get_input()
    ingress["audience"] = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudience", ingress["audience"]
    )

    if ingress["audience"].get("dataSource"):
        yield context.call_activity(
            "synapse_activity_cetas",
            {
                "instance_id": context.instance_id,
                **MAPPING_DATASOURCE[ingress["audience"]["dataSource"]["id"]],
                "destination": {
                    "conn_str": ingress["working"]["conn_str"],
                    "container_name": ingress["working"]["container_name"],
                    "blob_prefix": "{}/{}/{}/source".format(
                        ingress["working"]["blob_prefix"],
                        context.instance_id,
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
                ),
            },
        )

    if processes := ingress["audience"].get("processes"):
        for step, process in enumerate(processes):
            process["inputType"] = (
                processes[step - 1]["outputType"]
                if step
                else ingress["audience"]["dataSource"]["dataType"]
            )

    return ingress
