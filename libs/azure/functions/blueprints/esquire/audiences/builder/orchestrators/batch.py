# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/batch.py

from azure.durable_functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_batch(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates batch processing of Esquire audiences.

    This orchestrator retrieves a list of audience IDs and triggers the `orchestrator_esquireAudiences_builder` for each audience ID. The results of all orchestrations are collected and returned.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    list: The results of each audience processing orchestration.

    Expected format for context.get_input():
    {
        "working": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "destination": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        }
    }
    """

    # Retrieve the list of audience IDs to process
    audience_ids = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudienceIds"
    )

    # Trigger the builder orchestrator for each audience ID and collect the results
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
        ]
    )

    # Purge history related to this instance
    yield context.call_sub_orchestrator(
        "purge_instance_history",
        {"instance_id": context.instance_id},
    )

    # Return the results of the batch processing
    return results
