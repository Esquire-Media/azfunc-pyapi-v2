# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/fetchAudience.py

from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_fetchAudience(ingress: str):
    handle = "keystone"
    provider = from_bind(handle)
    audience = provider.models["public"]["Audience"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .options(
            lazyload(audience.related_Advertiser),
        )
        .where(
            audience.id == ingress,  # esq audience
            audience.status == True,
            audience.related_Advertiser.meta != None,
        )
    )
    result = session.execute(query).one_or_none()

    if result:
        return {
            "adAccount": result.Audience.related_Advertiser.meta, 
            "audience": None, # once the DB is updated, this will need to change
        }
        
    raise Exception(f"There were no Meta AdAccount results for the given ESQ audience ({ingress}) while using the binding {handle}.")
