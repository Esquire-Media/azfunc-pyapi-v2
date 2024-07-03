# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/getCustomAudienceSession.py

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)
from azure.durable_functions import Blueprint
from facebook_business.api import Cursor
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.adobjects.customaudiencesession import CustomAudienceSession

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudienceSessions_get(ingress: dict):
    """
    Replaces users in a Facebook Custom Audience using the provided ingress details.

    Args:
        ingress (dict): A dictionary containing the following keys:
            - "audience" (dict): Contains interal audience meta data.
                - "id" (str): The internal audience ID
                - "audience" (str): The Meta audience ID

    Returns:
        None
    """
    return [
        s.export_all_data()
        for s in CustomAudience(
            fbid=ingress["audience"]["audience"],
            api=initialize_facebook_api(ingress),
        ).get_sessions(
            fields=[
                CustomAudienceSession.Field.session_id,
                CustomAudienceSession.Field.stage,
                CustomAudienceSession.Field.num_received,
            ]
        )
    ]
