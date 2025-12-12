from __future__ import annotations

from typing import Any, Dict, Optional

from azure.durable_functions import Blueprint
from dateutil.relativedelta import relativedelta
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from libs.data import from_bind
from libs.data.structured.sqlalchemy.utils import _find_relationship_key

bp = Blueprint()


def _compute_expiration_minutes(audience_obj: Any) -> Optional[int]:
    """
    Derive expiration (in minutes) from an Audience-like object that exposes
    TTL_Unit and TTL_Length attributes. Returns None if either is missing.
    """
    ttl_unit = getattr(audience_obj, "TTL_Unit", None)
    ttl_length = getattr(aience_obj, "TTL_Length", None)

    if not ttl_unit or not ttl_length:
        return None

    # Example: ttl_unit="days", ttl_length=3 -> relativedelta(days=3)
    delta = relativedelta(**{str(ttl_unit): ttl_length})
    return get_minutes_from_relativedelta(delta)


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceFreewheel_fetchAudience(ingress: str) -> Dict[str, Any]:
    """
    Fetches audience metadata for Freewheel/Buyer Cloud using the given ESQ audience ID.

    ingress:
        "<Audience.id>"
    """
    audience_id = ingress
    provider = from_bind("keystone")

    Audience = provider.models["keystone"]["Audience"]
    Advertiser = provider.models["keystone"]["Advertiser"]

    # Resolve relationship attribute name dynamically (robust to renames)
    rel_Audience__Advertiser = _find_relationship_key(
        Audience,
        Advertiser,
        uselist=False,
    )

    # Build the query; execute within a context-managed session for cleanliness.
    with provider.connect() as session:  # type: Session
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

    # TTL is derived from the Audience row only; no extra queries required.
    expiration = _compute_expiration_minutes(audience_obj)

    # NOTE: `segment` here is whatever you store in Audience.freewheel.
    # For Buyer Cloud, this should be the segment_key, e.g. "stinger-123".
    return {
        "advertiser": getattr(advertiser_obj, "freewheel"),
        "segment": getattr(audience_obj, "freewheel"),
        "expiration": expiration,
    }


def get_minutes_from_relativedelta(delta: relativedelta) -> int:
    """
    Approximate the number of minutes represented by a relativedelta, using:
      - 365 days per year
      - 30 days per month
    """
    minutes_in_year = 365 * 24 * 60
    minutes_in_month = 30 * 24 * 60
    minutes_in_day = 24 * 60
    minutes_in_hour = 60

    total_minutes = (
        (getattr(delta, "years", 0) or 0) * minutes_in_year
        + (getattr(delta, "months", 0) or 0) * minutes_in_month
        + (getattr(delta, "days", 0) or 0) * minutes_in_day
        + (getattr(delta, "hours", 0) or 0) * minutes_in_hour
        + (getattr(delta, "minutes", 0) or 0)
    )

    return int(total_minutes)
