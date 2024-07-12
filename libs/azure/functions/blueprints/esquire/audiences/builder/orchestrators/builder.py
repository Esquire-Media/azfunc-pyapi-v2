# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/builder.py

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions import Blueprint

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
        "working": {
            "conn_str": str,
            "container_name": str,
            "blob_prefix": str,
        },
        "destination": {
            "conn_str": str,
            "container_name": str,
            "data_source": str,
            "blob_prefix": str,
        },
        "audience": {
            "id": str,
            "advertiser": {
                "meta": str,
                "xandr": str,
            },
            "status": bool,
            "rebuildSchedule": str,
            "TTL_Length": int,
            "TTL_Unit": str,
            "dataSource": {
                "id": str,
                "dataType": dict,
            },
            "dataFilter": str,
            "processes": [
                {
                    "id": str,
                    "sort": str,
                    "outputType": str,
                    "filterBy": str,
                    "customCoding": str,
                }
            ]
        },
    }
    """
    # Retrieve the input data for the orchestration
    ingress = context.get_input()
    ingress["instance_id"] = context.instance_id

    # Prepend the instance id to the working path for easy cleanup
    ingress["working"]["blob_prefix"] = "{}/{}".format(
        context.instance_id, ingress["working"]["blob_prefix"]
    )

    # Check if the audience has a data source
    if ingress["audience"].get("dataSource"):
        # Generate a primary data set
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_primaryData", ingress
        )
        if not ingress["results"] or not len(ingress["results"]):
            raise Exception("No results from primary data query.")
        
        # Run processing steps
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_processingSteps", ingress
        )
        if not ingress["results"] or not len(ingress["results"]):
            raise Exception("No results after processing steps completed.")
        
        # Ensure Device IDs are the final data type
        ingress = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_finalize", ingress
        )
        if not ingress["results"] or not len(ingress["results"]):
            raise Exception("No final results.")
    else:
        raise Exception("Primary data source is not set.")

    # Return the final state of the ingress data
    return ingress
