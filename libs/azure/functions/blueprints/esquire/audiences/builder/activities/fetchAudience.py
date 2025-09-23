# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload
import logging
from typing import Any, Dict, List, Union, Optional

bp = Blueprint()


def _canonicalize_jsonlogic(node: Any) -> Any:
    """
    Deterministically canonicalize a JsonLogic-like structure:
      - For dicts: sort keys lexicographically, recurse on values.
      - For lists/tuples: preserve element order, recurse on items.
      - For scalars: return as-is.

    This keeps semantics intact (array order preserved) while removing
    nondeterminism from arbitrary dict key order that could affect SQL generation.
    """
    if isinstance(node, dict):
        # sort keys for deterministic traversal
        return {k: _canonicalize_jsonlogic(node[k]) for k in sorted(node.keys())}
    if isinstance(node, (list, tuple)):
        return [ _canonicalize_jsonlogic(v) for v in node ]
    return node


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudience(ingress: dict):
    """
    Fetches audience data from the database using the given audience ID.

    Returns:
        dict: A dictionary containing the audience data along with the initial ingress data.
              The shape and ordering of derived fields are deterministic.
    """
    provider = from_bind("keystone")
    audience = provider.models["keystone"]["Audience"]

    session: Session = provider.connect()
    try:
        query = (
            select(audience)
            .options(
                # Keep relationship loading explicit to avoid incidental loads later.
                lazyload(audience.related_Advertiser),
                lazyload(audience.related_TargetingDataSource),
            )
            .where(audience.id == ingress["id"])
        )

        result = session.execute(query).one_or_none()
        if result:
            aud = result.Audience

            # Defensive extraction of related objects
            adv = getattr(aud, "related_Advertiser", None)
            tds = getattr(aud, "related_TargetingDataSource", None)

            # Canonicalize dataFilter before converting to SQL to prevent order-induced differences.
            data_filter_raw: Optional[Union[Dict[str, Any], List[Any]]] = getattr(aud, "dataFilter", None)
            try:
                data_filter_sql = (
                    jsonlogic_to_sql(_canonicalize_jsonlogic(data_filter_raw))
                    if data_filter_raw is not None
                    else None
                )
            except Exception as e:
                # If translation fails, log and propagate the raw filter deterministically
                logging.warning(
                    "[audience-builder] jsonlogic_to_sql failed for audience %s: %s",
                    ingress.get("id"),
                    e,
                )
                data_filter_sql = None

            return {
                **ingress,
                "advertiser": {
                    "meta": getattr(adv, "meta", None),
                    "xandr": getattr(adv, "xandr", None),
                },
                "status": getattr(aud, "status", None),
                "rebuildSchedule": getattr(aud, "rebuildSchedule", None),
                "TTL_Length": getattr(aud, "TTL_Length", None),
                "TTL_Unit": getattr(aud, "TTL_Unit", None),
                "dataSource": {
                    "id": getattr(tds, "id", None),
                    "dataType": getattr(tds, "dataType", None),
                },
                "dataFilter": data_filter_sql,
                "processing": getattr(aud, "processing", None),
            }

        logging.warning(f"[LOG] ingress after fetch audience: {ingress}")
        return ingress
    finally:
        try:
            session.close()
        except Exception:
            # Best-effort close; avoid raising from cleanup.
            pass
