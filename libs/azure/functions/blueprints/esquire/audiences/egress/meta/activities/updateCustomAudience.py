# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/updateCustomAudience.py

from azure.durable_functions import Blueprint
from facebook_business.adobjects.customaudience import CustomAudience
from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import initialize_facebook_api

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_update(ingress: dict):
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
        .api_update(
            fields=[
                CustomAudience.Field.name,
                CustomAudience.Field.description,
                CustomAudience.Field.operation_status,
            ],
            params={
                "name": ingress["audience"]["name"],
                "description": ingress["audience"]["id"],
            },
        )
        .export_all_data()
    )
