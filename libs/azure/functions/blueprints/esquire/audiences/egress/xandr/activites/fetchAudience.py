# File: /libs/azure/functions/blueprints/esquire/audiences/egress/xandr/activities/fetchAudience.py

from __future__ import annotations

from azure.durable_functions import Blueprint
from dateutil.relativedelta import relativedelta
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload
from typing import Optional, Any

from libs.data.structured.sqlalchemy.utils import _find_relationship_key

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceXandr_fetchAudience(ingress: str):
    """
    Fetches audience metadata for Xandr using the given audience ID.

    This dynamically discovers relationship attribute names so it works with both
    the old automap names and the new FK-aware names.
    """
    provider = from_bind("keystone")

    Audience = provider.models["keystone"]["Audience"]
    Advertiser = provider.models["keystone"]["Advertiser"]
    AudienceTag = provider.models["keystone"]["AudienceTag"]
    Tag = provider.models["keystone"]["Tag"]

    # Resolve relationship attribute names dynamically (robust to renames)
    # Audience (child) -> Advertiser (parent), scalar (many-to-one)
    rel_Audience__Advertiser = _find_relationship_key(Audience, Advertiser, uselist=False)

    # Audience (parent) <- AudienceTag (child), collection backref on Audience
    rel_Audience__AudienceTag_collection = _find_relationship_key(Audience, AudienceTag, uselist=True)

    # AudienceTag (child) -> Tag (parent), scalar (many-to-one)
    rel_AudienceTag__Tag = _find_relationship_key(AudienceTag, Tag, uselist=False)

    session: Session = provider.connect()

    # Eager-load the relationships we will access
    query = (
        select(Audience)
        .options(
            joinedload(getattr(Audience, rel_Audience__Advertiser)),
            joinedload(getattr(Audience, rel_Audience__AudienceTag_collection)).joinedload(
                getattr(AudienceTag, rel_AudienceTag__Tag)
            ),
        )
        .where(
            Audience.id == ingress,                 # ESQ audience
            Audience.status.is_(True),
            getattr(Audience, "xandr").isnot(None),
            # require non-null advertiser xandr through the relationship
            getattr(Audience, rel_Audience__Advertiser).has(
                getattr(Advertiser, "xandr").isnot(None)
            ),
        )
    )

    # Pull the single Audience row or None
    audience_obj: Optional[Any] = session.execute(query).unique().scalars().one_or_none()

    if not audience_obj:
        raise Exception(
            f"There were no Xandr Advertiser results for the given ESQ audience ({ingress})."
        )

    # Resolve related objects via discovered relationship names
    advertiser_obj = getattr(audience_obj, rel_Audience__Advertiser)
    audience_tags = getattr(audience_obj, rel_Audience__AudienceTag_collection) or []

    # Sort tags by the association's 'order' column (if present)
    audience_tags_sorted = sorted(audience_tags, key=lambda x: getattr(x, "order", 0))

    tags_titles = [
        getattr(getattr(rel, rel_AudienceTag__Tag), "title")
        for rel in audience_tags_sorted
        if getattr(rel, rel_AudienceTag__Tag) is not None
    ]

    # TTL to minutes (handles both the legacy and new schemas)
    ttl_unit = getattr(audience_obj, "TTL_Unit", None)
    ttl_length = getattr(audience_obj, "TTL_Length", None)
    expiration = (
        get_minutes_from_relativedelta(relativedelta(**{ttl_unit: ttl_length}))
        if ttl_unit and ttl_length
        else None
    )

    return {
        "advertiser": getattr(advertiser_obj, "xandr"),
        "segment": getattr(audience_obj, "xandr"),
        "tags": tags_titles,
        "expiration": expiration,
    }


def get_minutes_from_relativedelta(delta: relativedelta) -> int:
    # Conversion factors
    minutes_in_year = 365 * 24 * 60
    minutes_in_month = 30 * 24 * 60  # Assuming 30 days in a month for simplicity
    minutes_in_day = 24 * 60
    minutes_in_hour = 60

    # Calculate total minutes
    total_minutes = (
        (getattr(delta, "years", 0) or 0) * minutes_in_year
        + (getattr(delta, "months", 0) or 0) * minutes_in_month
        + (getattr(delta, "days", 0) or 0) * minutes_in_day
        + (getattr(delta, "hours", 0) or 0) * minutes_in_hour
        + (getattr(delta, "minutes", 0) or 0)
    )

    return int(total_minutes)
