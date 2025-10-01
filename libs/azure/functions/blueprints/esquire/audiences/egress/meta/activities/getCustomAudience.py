# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/getCustomAudience.py

from azure.durable_functions import Blueprint
from facebook_business.adobjects.customaudience import CustomAudience

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_get(ingress: dict):
    """
    Retrieves a Facebook Custom Audience by id.
    Returns the audience with name/description/operation_status/id for deterministic orchestration.
    """
    return (
        CustomAudience(
            fbid=ingress["audience"]["audience"],
            api=initialize_facebook_api(ingress),
        )
        .api_get(
            fields=[
                CustomAudience.Field.id,
                CustomAudience.Field.name,
                CustomAudience.Field.description,
                CustomAudience.Field.operation_status,
            ]
        )
        .export_all_data()
    )
