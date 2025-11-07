# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudience.py

from __future__ import annotations

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload
import logging
import os
from typing import Any, Dict, List, Union, Optional

from libs.data.structured.sqlalchemy.utils import _find_relationship_key
from libs.data import register_binding, from_bind
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import enforce_bindings

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
        return [_canonicalize_jsonlogic(v) for v in node]
    return node


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudience(ingress: dict):
    """
    Fetches audience data from the database using the given audience ID.

    Returns:
        dict: A dictionary containing the audience data along with the initial ingress data.
              The shape and ordering of derived fields are deterministic.
    """
    if not from_bind("keystone"):
        register_binding(
            "keystone",
            "Structured",
            "sql",
            url=os.environ["DATABIND_SQL_KEYSTONE"],
            schemas=["keystone"],
            pool_size=1000,
            max_overflow=100,
        )
    provider = from_bind("keystone")

    Audience = provider.models["keystone"]["Audience"]
    Advertiser = provider.models["keystone"]["Advertiser"]
    TargetingDataSource = provider.models["keystone"]["TargetingDataSource"]

    # Discover relationship attribute names dynamically (works with old & new naming)
    rel_Audience__Advertiser = _find_relationship_key(Audience, Advertiser, uselist=False)
    rel_Audience__TargetingDataSource = _find_relationship_key(
        Audience, TargetingDataSource, uselist=False
    )

    session: Session = provider.connect()
    try:
        query = (
            select(Audience)
            .options(
                # Keep relationship loading explicit to avoid incidental loads later.
                lazyload(getattr(Audience, rel_Audience__Advertiser)),
                lazyload(getattr(Audience, rel_Audience__TargetingDataSource)),
            )
            .where(Audience.id == ingress["id"])
        )

        result = session.execute(query).one_or_none()
        if result:
            aud = result.Audience

            # Defensive extraction of related objects (using discovered attribute names)
            adv = getattr(aud, rel_Audience__Advertiser, None)
            tds = getattr(aud, rel_Audience__TargetingDataSource, None)

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
                "dataFilterRaw": data_filter_raw,
                "processing": getattr(aud, "processing", None),
            }
        # logging.warning(f"[LOG] ingress after fetch audience: {ingress}")
    finally:
        try:
            session.close()
        except Exception:
            # Best-effort close; avoid raising from cleanup.
            pass
    return ingress
