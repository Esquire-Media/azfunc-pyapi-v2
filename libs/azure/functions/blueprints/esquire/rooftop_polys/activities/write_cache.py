# File: libs/azure/functions/blueprints/esquire/audiences/daily_audience_generation/activities/write_cache.py

from azure.durable_functions import Blueprint
from typing import List
from sqlalchemy.orm import Session
from libs.data import from_bind
from datetime import datetime as dt

bp: Blueprint = Blueprint()


# activity to validate the addresses
@bp.activity_trigger(input_name="polys")
def activity_rooftopPolys_writeCache(polys: List[dict]):
    # set the provider
    provider = from_bind("universal")
    rooftop = provider.models["dbo"]["GoogleRooftopCache"]
    session: Session = provider.connect()

    session.add_all(
        [
            rooftop(
                Query=p["query"],
                Boundary=p["geojson"],
                LastUpdated=dt.utcnow(),
            )
            for p in polys
        ]
    )

    session.commit()

    return {}
