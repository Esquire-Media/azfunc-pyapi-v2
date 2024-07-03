# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/getCustomAudience.py

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import initialize_facebook_api
from azure.durable_functions import Blueprint
from facebook_business.adobjects.customaudience import CustomAudience

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_get(ingress: dict):
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
    return (
        CustomAudience(
            fbid=ingress["audience"]["audience"],
            api=initialize_facebook_api(ingress),
        )
        .api_get(fields=[
            CustomAudience.Field.name,
            CustomAudience.Field.description,
            CustomAudience.Field.operation_status
        ])
        .export_all_data()
    )
