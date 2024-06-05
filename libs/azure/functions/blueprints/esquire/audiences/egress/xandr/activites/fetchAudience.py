# File: /libs/azure/functions/blueprints/esquire/audiences/xandr/activities/fetchAudience.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_fetchAudience(ingress: str):
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
    audience = provider.models["public"]["Audience"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .options(
            lazyload(audience.related_Advertiser),
        )
        .where(
            audience.id == ingress,  # esq audience
            audience.status == True,
            audience.related_Advertiser.xandr != None,
        )
    )
    result = session.execute(query).one_or_none()

    if result:
        return {
            "adAccount": result.Audience.related_Advertiser.xandr, 
            "audience": result.Audience.xandr,
            "tags": [
                related_tag.related_Tag.title
                for related_tag in sorted(
                    result.Audience.collection_AudienceTag, key=lambda x: x.order
                )
            ],
        }
        
    raise Exception(f"There were no Xandr Advertiser results for the given ESQ audience ({ingress}) while using the binding {provider.handle}.")
