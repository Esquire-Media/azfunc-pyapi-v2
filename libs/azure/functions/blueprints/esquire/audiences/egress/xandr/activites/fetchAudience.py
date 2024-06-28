# File: /libs/azure/functions/blueprints/esquire/audiences/egress/xandr/activities/fetchAudience.py

from datetime import timedelta
from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_fetchAudience(ingress: dict):
    """
    Fetches audience metadata from the database using the given audience ID.

    This activity retrieves the audience metadata, including related advertiser information and audience tags, from the database.

    Parameters:
    ingress (dict):
        id (str): The ID of the audience to fetch.

    Returns:
    dict: A dictionary containing the ad account metadata, audience metadata, and tags.

    Raises:
    Exception: If no results are found for the given audience ID.
    """
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]
    advertiser = provider.models["public"]["Advertiser"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .join(advertiser)
        .where(
            audience.id == ingress["id"],  # esq audience
            audience.status == True,
            audience.xandr != None,
            advertiser.xandr != None,
        )
    )

    result = session.execute(query).one_or_none()

    if len(result):
        return {
            **ingress,
            "advertiser": result.Audience.related_Advertiser.xandr,
            "segment": result.Audience.xandr,
            "tags": [
                related_tag.related_Tag.title
                for related_tag in sorted(
                    result.Audience.collection_AudienceTag, key=lambda x: x.order
                )
            ],
            "expiration": timedelta(
                **{result.Audience.rebuildUnit: result.Audience.rebuild}
            ).total_seconds()
            // 60,
        }

    raise Exception(
        f"There were no Xandr Advertiser results for the given ESQ audience ({ingress})."
    )
