# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/builder.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_builder(
    context: DurableOrchestrationContext,
):
    # ingress={
    #         "source": {
    #             "conn_str": "ONSPOT_CONN_STR",
    #             "container_name": "general",
    #             "blob_prefix": "audiences",
    #         },
    #         "working": {
    #             "conn_str": "ONSPOT_CONN_STR",
    #             "container_name": "general",
    #             "blob_prefix": "raw",
    #         },
    #         "destination": {
    #             "conn_str": "ONSPOT_CONN_STR",
    #             "container_name": "general",
    #             "blob_prefix": "audiences",
    #         },
    #         "audience": {
    #             "id": id,
    #         },
    #     },
    
    ingress = context.get_input()
    ingress["instance_id"] = context.instance_id

    # Get full details for audience
    ingress["audience"] = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudience",
        ingress["audience"],
    )
    
    if ingress["audience"].get("dataSource"):
        # Generate a primary data set
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_primaryData", ingress
        )

        # Run Processing Steps
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_processingSteps", ingress
        )

        # Ensure Device IDs are the final data type
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_finalize", ingress
        )

    return ingress
