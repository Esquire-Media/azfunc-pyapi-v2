from __future__ import annotations

from typing import Any, Dict, Optional

from azure.durable_functions import Blueprint
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from libs.data import from_bind
from libs.data.structured.sqlalchemy.utils import _find_relationship_key

bp = Blueprint()

@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_fetchAudience(ingress: str) -> Dict[str, Any]:
    """
    Fetches audience metadata for Freewheel/Buyer Cloud using the given ESQ audience ID.

    ingress:
        "<Audience.id>"
    """
    audience_id = str(ingress)
    provider = from_bind("keystone")

    Audience = provider.models["keystone"]["Audience"] # type: ignore
    Advertiser = provider.models["keystone"]["Advertiser"] # type: ignore

    # Resolve relationship attribute name dynamically (robust to renames)
    rel_Audience__Advertiser = _find_relationship_key(
        Audience,
        Advertiser,
        uselist=False,
    )

    # Build the query; execute within a context-managed session for cleanliness.
    with provider.connect() as session:  # type: ignore # type: Session
        query = (
            select(Audience)
            .options(
                joinedload(getattr(Audience, rel_Audience__Advertiser)),
            )
            .where(
                Audience.id == audience_id,
                Audience.status.is_(True),
                getattr(Audience, "freewheel").isnot(None),
                getattr(Audience, rel_Audience__Advertiser).has(
                    getattr(Advertiser, "freewheel").isnot(None)
                ),
            )
        )

        audience_obj: Optional[Any] = (
            session.execute(query).unique().scalars().one_or_none()
        )

    if audience_obj is None:
        raise Exception(
            f"There were no Freewheel Advertiser results for the given ESQ audience ({audience_id})."
        )

    advertiser_obj = getattr(audience_obj, rel_Audience__Advertiser)

    return {
        "advertiser": getattr(advertiser_obj, "freewheel"),
        "segment": getattr(audience_obj, "freewheel"),
    }
