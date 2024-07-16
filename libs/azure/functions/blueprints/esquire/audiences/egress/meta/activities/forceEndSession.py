# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/forceEndSession.py

from azure.durable_functions import Blueprint
from facebook_business.adobjects.customaudience import CustomAudience
from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)
import uuid

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudienceSession_forceEnd(ingress: dict):
    """
    Replaces users in a Facebook Custom Audience using the provided ingress details.

    Args:
        ingress (dict): A dictionary containing the following keys:
            - "audience" (dict): Contains interal audience meta data.
                - "id" (str): The internal audience ID
                - "audience" (str): The Meta audience ID
            - "batch" (dict): Contains batching details:
                - "sequence" (int): The current batch sequence number.
                - "size" (int): The number of records to fetch per batch.
                - "total" (int): The total number of records to process.
                - "session_id" (optional, int): The session ID for the batch process. If not provided, a random session ID will be generated.
            - "access_token" (optional, str): The access token for Facebook API. If not provided, it will be fetched from environment variables.
            - "app_id" (optional, str): The app ID for Facebook API. If not provided, it will be fetched from environment variables.
            - "app_secret" (optional, str): The app secret for Facebook API. If not provided, it will be fetched from environment variables.

    Returns:
        None
    """
    return (
        CustomAudience(
            fbid=ingress["audience"]["audience"],
            api=initialize_facebook_api(ingress),
        )
        .create_users_replace(
            params={
                "payload": {
                    "schema": CustomAudience.Schema.mobile_advertiser_id,
                    "data": [str(uuid.uuid4())],
                },
                "session": {
                    "session_id": ingress["batch"]["session_id"],
                    "estimated_num_total": ingress["batch"]["total"],
                    "batch_seq": ingress["batch"]["sequence"] + 1,
                    "last_batch_flag": True,
                },
            },
        )
        .export_all_data()
    )
