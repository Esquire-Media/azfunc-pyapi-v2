# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/putAudience.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import update
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_putAudience(ingress: str):
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]

    session: Session = provider.connect()
    
    session.execute(
        update(audience)
        .where(audience.id == ingress['audience'])
        .values(meta=ingress['metaAudienceId'])
    )
    result = session.commit()
    
    return result
