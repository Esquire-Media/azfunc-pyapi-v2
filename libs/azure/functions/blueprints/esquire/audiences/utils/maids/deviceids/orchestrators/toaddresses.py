# File: libs/azure/functions/blueprints/esquire/audiences/maids/deviceids/orchestrators/toaddresses.py

from azure.durable_functions import DurableOrchestrationContext, RetryOptions
from azure.storage.blob import BlobClient
from azure.durable_functions import Blueprint
import orjson as json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceMaidsDeviceIds_toaddresses(
    context: DurableOrchestrationContext,
):
    """
    Suborchestrator function for processing device IDs for Esquire Audiences.

    This function coordinates tasks for processing device IDs. It calls the OnSpot
    suborchestrator to process device IDs and then merges the resulting address files.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with the Durable Functions runtime.
        - working : dict
            Information about the working blob storage, including blob prefix and other details.
        - source : str
            The URL to the source data in Blob Storage which needs to be processed.
        - destination : dict
            The details of the destination blob storage where the processed data will be saved.
            This includes 'conn_str', 'container_name', and 'blob_name'.
    """
    
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # Call the OnSpot Orchestrator with the request loaded from the Blob Storage
    onspot = yield context.task_all(
        [
            context.call_sub_orchestrator_with_retry(
                "onspot_orchestrator",
                retry,
                {
                    **ingress["working"],
                    "endpoint": "/save/files/household",
                    "request": json.loads(
                        BlobClient.from_blob_url(ingress["source"]).download_blob().content_as_bytes()
                    ),
                },
            )
        ]
    )

    # Check for success in all callbacks from OnSpot Orchestrator
    if not all([c["success"] for r in onspot for c in r["callbacks"]]):
        # Raise an exception if any callback failed
        raise Exception([c for r in onspot for c in r["callbacks"] if not c["success"]])

    # merge all of the device files into one file
    blob_uri = yield context.call_activity_with_retry(
        "activity_onSpot_mergeDevices",
        retry,
        {
            "source": ingress["working"],
            "destination": ingress["destination"],
        },
    )

    return blob_uri
