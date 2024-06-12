# File: libs/azure/functions/blueprints/esquire/audiences/maids/addresses/orchestrators/standard.py

from azure.durable_functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext, RetryOptions
import uuid

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquireAudienceMaidsAddresses_standard(
    context: DurableOrchestrationContext,
):
    """
    Suborchestrator function for processing Friends and Family audiences.

    This function is responsible for coordinating the tasks involved in processing
    Friends and Family audience data. It includes calling a suborchestrator for OnSpot
    processing, checking callbacks for success, and merging device files.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the orchestration, containing methods for interacting with the Durable Functions runtime.
        - working : dict
            Information about the working blob storage, including blob prefix and other details.
        - source : str
            The source URL for the data processing.
    """
    ingress = context.get_input()
    retry = RetryOptions(15000, 1)

    # Call the OnSpot Orchestrator with the necessary parameters
    onspot = yield context.call_sub_orchestrator_with_retry(
        "onspot_orchestrator",
        retry,
        {
            **ingress["working"],
            "endpoint": "/save/addresses/all/devices",
            "request": {
                "hash": False,
                "name": uuid.uuid4().hex,
                "fileName": uuid.uuid4().hex,
                "fileFormat": {
                    "delimiter": ",",
                    "quoteEncapsulate": True,
                },
                "mappings": {
                    "street": ["delivery_line_1"],
                    "city": ["city_name"],
                    "state": ["state_abbreviation"],
                    "zip": ["zipcode"],
                    "zip4": ["plus4_code"],
                },
                "matchAcceptanceThreshold": 29.9,
                "sources": [ingress["source"].replace("https://", "az://")],
            },
        },
    )

    # Check for success in all callbacks from the OnSpot Orchestrator
    if not all([c["success"] for c in onspot["callbacks"]]):
        # Raise an exception if any callback failed
        raise Exception([c for c in onspot["callbacks"] if not c["success"]])

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
