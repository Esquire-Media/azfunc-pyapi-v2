# File: /libs/azure/functions/blueprints/esquire/audiences/egress/xandr/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from dateutil.relativedelta import relativedelta
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session

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
    audience = provider.models["keystone"]["Audience"]
    advertiser = provider.models["keystone"]["Advertiser"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .join(advertiser)
        .where(
            audience.id == ingress,  # esq audience
            audience.status == True,
            audience.xandr != None,
            advertiser.xandr != None,
        )
    )

    result = session.execute(query).one_or_none()

    if result and len(result):
        return {
            "advertiser": result.Audience.related_Advertiser.xandr,
            "segment": result.Audience.xandr,
            "tags": [
                related_tag.related_Tag.title
                for related_tag in sorted(
                    result.Audience.collection_AudienceTag, key=lambda x: x.order
                )
            ],
            "expiration": get_minutes_from_relativedelta(
                relativedelta(**{result.Audience.TTL_Unit: result.Audience.TTL_Length})
            ),
        }

    raise Exception(
        f"There were no Xandr Advertiser results for the given ESQ audience ({ingress})."
    )


def get_minutes_from_relativedelta(delta: relativedelta):
    # Conversion factors
    minutes_in_year = 365 * 24 * 60
    minutes_in_month = 30 * 24 * 60  # Assuming 30 days in a month for simplicity
    minutes_in_day = 24 * 60
    minutes_in_hour = 60

    # Calculate total minutes
    total_minutes = (
        delta.years * minutes_in_year
        + delta.months * minutes_in_month
        + delta.days * minutes_in_day
        + delta.hours * minutes_in_hour
        + delta.minutes
    )

    return total_minutes
