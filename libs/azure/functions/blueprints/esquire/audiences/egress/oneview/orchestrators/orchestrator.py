# File path: libs/azure/functions/blueprints/esquire/audiences/oneview/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import os

# Initialize a Blueprint object to define and manage functions
bp = Blueprint()


# Define the orchestration trigger function for managing Meta custom audiences
@bp.orchestration_trigger(context_name="context")
def oneview_customaudience_orchestrator(
    context: DurableOrchestrationContext,
):
    """
    Orchestrates the process of managing and updating OneView segments.

    This function handles the creation of new OneView segments, fetching existing audience information,
    updating audience data, and adding users to the audience.

    Args:
        context (DurableOrchestrationContext): The orchestration context.
    """
    # reach out to audience definition DB - get information pertaining to the xandr audience (segment)
    audience = yield context.call_activity(
        "activity_esquireAudienceOneview_fetchAudience",
        {"id": context.get_input()},
    )

    # TODO: figure out how to check for and create a segment if it doesn't exist
    
    
    blob_names = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPaths",
        {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
            "audience_id": audience["id"],
        },
    )

    blob_url = yield context.call_activity(
        "activity_esquireAudienceOneview_collateAudience",
        {
            "audience": audience,
            "sources": {
                "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
                "container_name": os.environ["ESQUIRE_AUDIENCE_CONTAINER_NAME"],
                "blob_names": blob_names,
            },
            "destination": {
                "conn_str": "AzureWebJobsStorage",
                "container_name": os.environ["TASK_HUB_NAME"] + "-largemessages",
                "blob_prefix": context.instance_id,
            },
        },
    )

    yield context.call_activity(
        "blob_to_s3",
        {
            "source": blob_url,
            "target": {
                "access_key": os.environ["ONEVIEW_SEGMENTS_AWS_ACCESS_KEY"],
                "secret_key": os.environ["ONEVIEW_SEGMENTS_AWS_SECRET_KEY"],
                "bucket": os.environ["ONEVIEW_SEGMENTS_S3_BUCKET"],
                "object_key": "{}/{}.csv".format(
                    os.environ["ONEVIEW_SEGMENTS_S3_PREFIX"],
                    audience["segment"],
                ),
            },
        },
    )

    return {}
