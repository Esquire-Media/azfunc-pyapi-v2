# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/fetchAudience.py

from __future__ import annotations

from typing import Optional, Any, Dict, List
from copy import deepcopy

from azure.durable_functions import Blueprint
from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload

from libs.data import from_bind
from libs.data.structured.sqlalchemy.utils import _find_relationship_key

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_fetchAudience(ingress: str) -> Dict[str, Any]:
    """
    Idempotent activity: fetch audience metadata for a given ESQ Audience ID.

    Why this is idempotent (safe for at-least-once execution):
      - Pure read-only: no writes, mutations, or external side effects.
      - Deterministic output schema and ordering for stable orchestration replays.
        * Tags are ordered by AudienceTag.order (NULL as 0), then Tag.title (case-insensitive).
      - Defensive validation: raises on missing data so the orchestrator can terminate deterministically.

    Returns a dictionary with:
        {
            "adAccount": <Advertiser.meta>,   # required, non-null JSON-like (deep-copied)
            "audience":  <Audience.meta>,     # required, non-null JSON-like (deep-copied)
            "tags":      List[str],           # titles ordered deterministically
        }

    Raises:
        Exception: when the audience is not found, inactive, or required meta is missing.
                   The orchestrator catches this and ends deterministically with {}.
    """
    # Validate input deterministically.
    if not ingress or not isinstance(ingress, str):
        raise Exception("A non-empty Audience ID (str) is required.")

    provider = from_bind("keystone")

    Audience = provider.models["keystone"]["Audience"]
    Advertiser = provider.models["keystone"]["Advertiser"]
    AudienceTag = provider.models["keystone"]["AudienceTag"]
    Tag = provider.models["keystone"]["Tag"]

    rel_Audience__Advertiser = _find_relationship_key(Audience, Advertiser, uselist=False)
    rel_Audience__AudienceTag_collection = _find_relationship_key(Audience, AudienceTag, uselist=True)
    rel_AudienceTag__Tag = _find_relationship_key(AudienceTag, Tag, uselist=False)

    session: Session = provider.connect()
    try:
        query = (
            select(Audience)
            .options(
                joinedload(getattr(Audience, rel_Audience__Advertiser)),
                joinedload(getattr(Audience, rel_Audience__AudienceTag_collection)).joinedload(
                    getattr(AudienceTag, rel_AudienceTag__Tag)
                ),
            )
            .where(
                Audience.id == ingress,
                Audience.status.is_(True),
                Audience.meta.isnot(None),
                getattr(Audience, rel_Audience__Advertiser).has(
                    getattr(Advertiser, "meta").isnot(None)
                ),
            )
        )

        audience_obj: Optional[Any] = session.execute(query).unique().scalars().one_or_none()
        if not audience_obj:
            raise Exception(
                f"There were no Meta AdAccount results for the given ESQ audience ({(ingress)})."
            )

        advertiser_obj = getattr(audience_obj, rel_Audience__Advertiser)
        audience_tags: List[Any] = getattr(audience_obj, rel_Audience__AudienceTag_collection) or []

        def _order_key(at: Any):
            order_val = getattr(at, "order", 0) or 0
            tag_obj = getattr(at, rel_AudienceTag__Tag)
            tag_title = getattr(tag_obj, "title", "") if tag_obj is not None else ""
            return (int(order_val), str(tag_title).lower())

        audience_tags_sorted = sorted(audience_tags, key=_order_key)

        tags_titles: List[str] = []
        for rel in audience_tags_sorted:
            tag_obj = getattr(rel, rel_AudienceTag__Tag, None)
            if tag_obj is None:
                continue
            title = getattr(tag_obj, "title", None)
            if title is None:
                continue
            tags_titles.append(str(title))

        ad_account_meta = getattr(advertiser_obj, "meta")
        audience_meta = getattr(audience_obj, "meta")

        if ad_account_meta is None or audience_meta is None:
            raise Exception(
                f"Audience ({ingress}) or Advertiser has null meta; cannot sync Meta audience."
            )

        return {
            "adAccount": deepcopy(ad_account_meta),
            "audience": deepcopy(audience_meta),
            "tags": tags_titles,
        }

    finally:
        try:
            session.close()
        except Exception:
            pass
