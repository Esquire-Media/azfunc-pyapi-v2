# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudience.py

from azure.durable_functions import Blueprint
from libs.azure.functions.blueprints.esquire.audiences.builder.utils import (
    jsonlogic_to_sql,
)
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session, lazyload

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudience(ingress: dict):
    """
    Fetches audience data from the database using the given audience ID.

    This activity retrieves the audience data, including related advertiser information, targeting data source, and audience processes, from the database.

    Parameters:
    ingress (dict): A dictionary containing the audience ID.
        {
            "id": str
        }

    Returns:
    dict: A dictionary containing the audience data along with the initial ingress data.

    Raises:
    Exception: If no results are found for the given audience ID.
    """
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]

    session: Session = provider.connect()
    query = (
        select(audience)
        .options(
            lazyload(audience.related_Advertiser),
            lazyload(audience.related_TargetingDataSource),
            lazyload(audience.collection_AudienceProcess),
        )
        .where(audience.id == ingress["id"])
    )

    result = session.execute(query).one_or_none()
    if result:
        return {
            **ingress,
            "advertiser": {
                "meta": result.Audience.related_Advertiser.meta,
                "xandr": result.Audience.related_Advertiser.xandr,
            },
            "status": result.Audience.status,
            "rebuild": result.Audience.rebuild,
            "rebuildUnit": result.Audience.rebuildUnit,
            "TTL_Length": result.Audience.TTL_Length,
            "TTL_Unit": result.Audience.TTL_Unit,
            "dataSource": {
                "id": result.Audience.related_TargetingDataSource.id,
                "dataType": result.Audience.related_TargetingDataSource.dataType,
            },
            "dataFilter": jsonlogic_to_sql(result.Audience.dataFilter),
            "processes": list(
                map(
                    lambda row: {
                        "id": row.id,
                        "sort": row.sort,
                        "outputType": row.outputType,
                        "customCoding": row.customCoding,
                    },
                    result.Audience.collection_AudienceProcess,
                )
            ),
        }

    return ingress
