# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/fetchAudience.py

from libs.azure.functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceMeta_fetchAudience(ingress: str):
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]
    advertiser = provider.models["public"]["Advertiser"]
    tag = provider.models["public"]["Tag"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .options(
            lazyload(audience.related_Advertiser),
            lazyload(audience.collection_AudienceTag),
        )
        .where(
            audience.id == ingress,  # esq audience
            audience.status == True,
            advertiser.meta != None,
            
        )
    )
    result = session.execute(query).one_or_none()
    if result:
        return {
            "adAccount": result.Audience.related_Advertiser.meta,
            "audience": result.Audience.meta,
            "tags": [
                related_tag.related_Tag.title
                for related_tag in sorted(result.Audience.collection_AudienceTag, key=lambda x: x.order)
            ],
        }

    raise Exception(
        f"There were no Meta AdAccount results for the given ESQ audience ({ingress}) while using the binding {handle}."
    )
