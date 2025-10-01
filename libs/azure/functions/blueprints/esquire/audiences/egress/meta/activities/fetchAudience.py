# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload
from typing import Optional, Any

from libs.data.structured.sqlalchemy.utils import _find_relationship_key

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_fetchAudience(ingress: str):
    """
    Fetches audience metadata from the database using the given audience ID.

    Returns a dictionary containing:
      - adAccount: Advertiser.meta
      - audience:  Audience.meta
      - tags:      The list of Tag titles associated to the Audience (ordered by AudienceTag.order)
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
            Audience.id == ingress,                 # esq audience
            Audience.status.is_(True),
            Audience.meta.isnot(None),
            # require non-null advertiser meta through the relationship
            getattr(Audience, rel_Audience__Advertiser).has(
                getattr(Advertiser, "meta").isnot(None)
            ),
        )
    )

    # Pull the single Audience row or None
    audience_obj: Optional[Any] = session.execute(query).unique().scalars().one_or_none()

    if not audience_obj:
        raise Exception(
            f"There were no Meta AdAccount results for the given ESQ audience ({ingress})."
        )

    # Resolve fields using the discovered relationship names
    advertiser_obj = getattr(audience_obj, rel_Audience__Advertiser)
    audience_tags = getattr(audience_obj, rel_Audience__AudienceTag_collection) or []

    # Sort by the association column 'order' (present in your original code)
    audience_tags_sorted = sorted(audience_tags, key=lambda x: getattr(x, "order", 0))

    tags_titles = [
        getattr(getattr(rel, rel_AudienceTag__Tag), "title")
        for rel in audience_tags_sorted
        if getattr(rel, rel_AudienceTag__Tag) is not None
    ]

    return {
        "adAccount": getattr(advertiser_obj, "meta"),
        "audience": getattr(audience_obj, "meta"),
        "tags": tags_titles,
    }