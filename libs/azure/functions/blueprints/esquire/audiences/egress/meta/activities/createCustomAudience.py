# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/createCustomAudience.py

from azure.durable_functions import Blueprint
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.customaudience import CustomAudience

from libs.azure.functions.blueprints.esquire.audiences.egress.meta.utils import (
    initialize_facebook_api,
)

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_customAudience_create(ingress: dict):
    """
    Creates a Facebook Custom Audience.

    Determinism/Idempotency:
      * DF ensures activities won't be re-executed on replay.
      * We return full data including 'id' for downstream orchestration.
    """
    return (
        AdAccount(
            fbid=ingress["audience"]["adAccount"],
            api=initialize_facebook_api(ingress),
        )
        .create_custom_audience(
            fields=[
                CustomAudience.Field.id,
                CustomAudience.Field.name,
                CustomAudience.Field.description,
                CustomAudience.Field.operation_status,
            ],
            params={
                "name": ingress["audience"]["name"],
                "description": ingress["audience"]["id"],
                "customer_file_source": CustomAudience.CustomerFileSource.user_provided_only,
                "subtype": CustomAudience.Subtype.custom,
            },
        )
        .export_all_data()
    )
