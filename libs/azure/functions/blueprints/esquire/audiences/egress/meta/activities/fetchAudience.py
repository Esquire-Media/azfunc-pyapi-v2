# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_fetchAudience(ingress: str):
    """
    Fetches audience metadata from the database using the given audience ID.

    This activity retrieves the audience metadata, including related advertiser information and audience tags, from the database.

    Parameters:
    ingress (str): The ID of the audience to fetch.

    Returns:
    dict: A dictionary containing the ad account metadata, audience metadata, and tags.

    Raises:
    Exception: If no results are found for the given audience ID.
    """
    provider = from_bind("keystone")
    audience = provider.models["keystone"]["Audience"]
    advertiser = provider.models["keystone"]["Advertiser"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .join(advertiser)
        .where(
            audience.id == ingress,  # esq audience
            audience.status == True,
            audience.meta != None,
            advertiser.meta != None,
        )
    )

    result = session.execute(query).one_or_none()

    if result and len(result):
        return {
            "adAccount": result.Audience.related_Advertiser.meta,
            "audience": result.Audience.meta,
            "tags": [
                related_tag.related_Tag.title
                for related_tag in sorted(
                    result.Audience.collection_AudienceTag, key=lambda x: x.order
                )
            ],
        }

    raise Exception(
        f"There were no Meta AdAccount results for the given ESQ audience ({ingress})."
    )
