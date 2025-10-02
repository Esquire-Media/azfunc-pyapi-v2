# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/getCustomAudienceSessions.py

import logging
from azure.durable_functions import Blueprint
from facebook_business.adobjects.customaudience import CustomAudience
from facebook_business.adobjects.customaudiencesession import CustomAudienceSession

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudienceSessions_get(ingress: dict):
    """
    Lists sessions for a Facebook Custom Audience (deterministic, read-only).
    """
    result = CustomAudience(
        fbid=ingress["audience"]["audience"],
        api=initialize_facebook_api(ingress),
    ).get_sessions(
        fields=[
            CustomAudienceSession.Field.session_id,
            CustomAudienceSession.Field.stage,
            CustomAudienceSession.Field.num_received,
        ]
    )
    logging.warning(result)
    return [
        s.export_all_data()
        for s in result
    ]
