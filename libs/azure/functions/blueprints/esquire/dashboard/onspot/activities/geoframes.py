# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/activities/geoframes.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from libs.data.structured.sqlalchemy import SQLAlchemyStructuredProvider
from sqlalchemy import or_

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def esquire_dashboard_onspot_activity_geoframes(ingress: dict):
    provider: SQLAlchemyStructuredProvider = from_bind("keystone")
    tables = provider.models["keystone"]
    session = provider.connect()

    markets = (
        (
            session.query(tables["_Market_owned"].B.label("id"))
            .select_from(tables["Market"])
            .join(
                tables["_Market_owned"],
                tables["_Market_owned"].A == tables["Market"].id,
            )
            .filter(
                tables["Market"].status == True,
            )
        )
        .union(
            session.query(tables["_Market_competitors"].B.label("id"))
            .select_from(tables["Market"])
            .join(
                tables["_Market_competitors"],
                tables["_Market_competitors"].A == tables["Market"].id,
            )
            .filter(
                tables["Market"].status == True,
            )
        )
        .distinct()
        .subquery()
    )

    return [
        (
            row.id,
            row.polygon["features"][0],
        )
        for row in session.query(
            tables["TargetingGeoFrame"].id,
            tables["TargetingGeoFrame"].polygon,
        )
        .join(markets, markets.c.id == tables["TargetingGeoFrame"].id)
        .all()
    ]
