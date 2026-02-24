# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/updateCustomAudience.py

from azure.durable_functions import Blueprint
from facebook_business.adobjects.customaudience import CustomAudience

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_update(ingress: dict):
    """
    Updates name/description for a Facebook Custom Audience.

    Determinism:
      * Pure 'api_update' with deterministic params; DF handles replay.
    """
    try:
        res = (
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
        return res
    except Exception as e:
        return {
            'error': f"updateCustomAudience Error {e.http_status()} : {e.api_error_message()}"
        }
