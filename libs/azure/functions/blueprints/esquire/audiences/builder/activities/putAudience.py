from __future__ import annotations

import logging
from typing import Any, Dict, Optional, TypedDict

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select, update
from sqlalchemy.orm import Session

bp = Blueprint()


class AudiencePayload(TypedDict):
    id: str
    count: int


class Ingress(TypedDict):
    audience: AudiencePayload


def _parse_ingress(ingress: Dict[str, Any]) -> Ingress:
    """
    Validate and normalize the incoming payload.

    Raises a ValueError with a deterministic message if the payload
    is malformed. Durable orchestrators may replay activities,
    so raising deterministic errors helps avoid non-determinism.
    """
    if not isinstance(ingress, dict):
        raise ValueError("Ingress must be a dict with an 'audience' field.")

    audience = ingress.get("audience")
    if not isinstance(audience, dict):
        raise ValueError("Ingress must include an 'audience' object.")

    audience_id = audience.get("id")
    if not isinstance(audience_id, str) or not audience_id:
        raise ValueError("audience.id must be a non-empty string.")

    count = audience.get("count")
    if not isinstance(count, int):
        # Be strict and deterministic about types; do not coerce.
        raise ValueError("audience.count must be an integer.")

    return {"audience": {"id": audience_id, "count": count}}


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesBuilder_putAudience(ingress: Dict[str, Any]) -> Dict[str, Any]:
    """
    Idempotently update the audience's lastDeviceCount.

    This activity is written to be *idempotent* and *deterministic*:
    - If the audience does not exist, **no-op** and return a stable result.
    - If the value is already equal to the requested count, **no-op** and return `changed=False`.
    - If the value is different, perform a single conditional UPDATE and return `changed=True`.

    Returns a stable, serializable result so replays/duplicates from Durable Functions
    produce the same observable output for the same input and DB state.

    Parameters
    ----------
    ingress : dict
        {
          "audience": {
            "id": str,
            "count": int
          }
        }

    Returns
    -------
    dict
        {
          "audienceId": str,
          "previousCount": Optional[int],
          "newCount": Optional[int],
          "changed": bool,
          "status": "updated" | "unchanged" | "not_found"
        }
    """
    payload: Ingress = _parse_ingress(ingress)

    provider = from_bind("keystone")
    Audience = provider.models["keystone"]["Audience"]  # SQLAlchemy mapped class

    session: Session = provider.connect()
    try:
        with session.begin():
            # Read current value deterministically
            current: Optional[int] = session.execute(
                select(Audience.lastDeviceCount).where(Audience.id == payload["audience"]["id"])
            ).scalar_one_or_none()

            if current is None:
                # Audience record not found; no side-effect => idempotent no-op
                result = {
                    "audienceId": payload["audience"]["id"],
                    "previousCount": None,
                    "newCount": None,
                    "changed": False,
                    "status": "not_found",
                }
                # Deterministic log message (no timestamps/randomness)
                logging.info(
                    "Audience lastDeviceCount update skipped: audience not found | audienceId=%s",
                    payload["audience"]["id"],
                )
                return result

            new_count = payload["audience"]["count"]

            if current == new_count:
                # Already at desired state; no-op => idempotent
                result = {
                    "audienceId": payload["audience"]["id"],
                    "previousCount": current,
                    "newCount": current,
                    "changed": False,
                    "status": "unchanged",
                }
                logging.info(
                    "Audience lastDeviceCount unchanged | audienceId=%s | count=%s",
                    payload["audience"]["id"],
                    current,
                )
                return result

            # Apply deterministic state transition: set to the provided value
            stmt = (
                update(Audience)
                .where(Audience.id == payload["audience"]["id"])
                .values(lastDeviceCount=new_count)
            )
            session.execute(stmt)

            result = {
                "audienceId": payload["audience"]["id"],
                "previousCount": current,
                "newCount": new_count,
                "changed": True,
                "status": "updated",
            }
            logging.info(
                "Audience lastDeviceCount updated | audienceId=%s | previousCount=%s | newCount=%s",
                payload["audience"]["id"],
                current,
                new_count,
            )
            return result
    finally:
        # Ensure resources are cleaned up deterministically on all paths
        session.close()
