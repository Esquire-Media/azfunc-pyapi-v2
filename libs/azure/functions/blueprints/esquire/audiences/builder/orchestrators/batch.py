# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/batch.py

from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_batch(
    context: DurableOrchestrationContext,
):
    # client_input={
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
    #     },
    
    audience_ids = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudienceIds"
    )

    results = yield context.task_all(
        [
            context.call_sub_orchestrator(
                "orchestrator_esquireAudiences_builder",
                {
                    **context.get_input(),
                    "audience": {
                        "id": id,
                    },
                },
            )
            for id in audience_ids
            if id == "clulpbe4r001s12jigokcm2i7"
        ]
    )

    return results
