# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/uploader.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiences_uploader(
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
        "destination": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "audience": {
            "id": str,
            "advertiser": {
                "meta": str,
                "xandr": str,
            }
        },
    }
    """

    # Retrieve the input data for the orchestration
    ingress = context.get_input()

    # Push the most recently generated audiences to the DSPs that are configured
    tasks = []
    if ingress["audience"]["advertiser"]["meta"]:
        tasks.append(
            context.call_sub_orchestrator(
                "meta_customaudience_orchestrator",
                ingress,
            )
        )
    if ingress["audience"]["advertiser"]["xandr"]:
        tasks.append(
            context.call_sub_orchestrator(
                "xandr_segment_orchestrator",
                ingress,
            )
        )

    # Wait for all tasks to complete
    yield context.task_all(tasks)
