
from azure.durable_functions import Blueprint, DurableOrchestrationContext
import uuid

bp = Blueprint()

@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudiencesSteps_deviceidsDemoFiltered(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the filtering of device IDs by demographics for Esquire audiences.

    This orchestrator filters demographics and returns the URLs of the processed device ID data.

    Parameters:
    context (DurableOrchestrationContext): The context object provided by Azure Durable Functions, used to manage and track the orchestration.

    Returns:
    list: The URLs of the processed data results.

    Expected format for context.get_input():
    {
        "working": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "source_urls": [str]
    }
    """

    ingress = context.get_input()

    demo_urls = context.task_all(
        context.call_activity(
            "orchestrator_esquireAudiencesSteps_deviceids2Demographics",
            **ingress,
        )
    )

    

    return