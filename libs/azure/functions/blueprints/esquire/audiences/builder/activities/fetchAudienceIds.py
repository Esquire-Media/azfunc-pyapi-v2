# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudienceIds.py.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudienceIds(ingress: dict):
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]
    session: Session = provider.connect()

    query = select(audience.id).where(audience.status == True)
    results = session.execute(query).all()

    return list(map(lambda row: row.id, results))
