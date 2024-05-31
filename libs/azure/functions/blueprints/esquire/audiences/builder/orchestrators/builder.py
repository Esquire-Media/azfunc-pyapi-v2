# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/builder.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_builder(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the building of Esquire audiences.

    This orchestrator performs several steps to process audience data, including fetching audience details, generating primary datasets, running processing steps, finalizing data, and pushing the data to configured DSPs.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    dict: The final state of the ingress data after processing.

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
        },
    }
    """

    # Retrieve the input data for the orchestration
    ingress = context.get_input()
    ingress["instance_id"] = context.instance_id

    # Fetch the full details for the audience
    ingress["audience"] = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudience",
        ingress["audience"],
    )

    # Check if the audience has a data source
    if ingress["audience"].get("dataSource"):
        # Generate a primary data set
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_primaryData", ingress
        )

        # Run processing steps
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_processingSteps", ingress
        )

        # Ensure Device IDs are the final data type
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_finalize", ingress
        )

        # Push the newly generated audiences to the DSPs that are configured
        tasks = []
        if ingress["advertiser"]["meta"]:
            tasks.append(
                context.call_sub_orchestrator(
                    "meta_customaudience_orchestrator", ingress["audience"]["id"]
                )
            )
        if ingress["advertiser"]["oneview"]:
            tasks.append(
                context.call_sub_orchestrator(
                    "oneview_customaudience_orchestrator", ingress["audience"]["id"]
                )
            )
        if ingress["advertiser"]["xandr"]:
            tasks.append(
                context.call_sub_orchestrator(
                    "xandr_customaudience_orchestrator", ingress["audience"]["id"]
                )
            )

        # Wait for all tasks to complete
        yield context.task_all(tasks)

    # Return the final state of the ingress data
    return ingress
