# File: /libs/azure/functions/blueprints/esquire/audiences/egress/meta/activities/putAudience.py

from azure.durable_functions import Blueprint
from sqlalchemy import update
from sqlalchemy.orm import Session

from libs.data import from_bind

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_putAudience(ingress: dict):
    """
    Idempotent write: record the Meta audience id back to ESQ.

    Re-applying the same value is safe; transaction commits atomically.
    """
    provider = from_bind("keystone")
    Audience = provider.models["keystone"]["Audience"]

    session: Session = provider.connect()
    try:
        session.execute(
            update(Audience)
            .where(Audience.id == ingress["audience"])
            .values(meta=ingress["metaAudienceId"])
        )
        session.commit()
        return None
    finally:
        try:
            session.close()
        except Exception:
            pass
