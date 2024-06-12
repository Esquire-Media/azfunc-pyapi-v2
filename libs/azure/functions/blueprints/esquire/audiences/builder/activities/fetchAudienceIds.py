# File: /libs/azure/functions/blueprints/esquire/audiences/builder/activities/fetchAudienceIds.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudienceBuilder_fetchAudienceIds(ingress: dict):
    """
    Fetches the IDs of all active audiences from the database.

    This activity retrieves the IDs of all audiences that have their status set to True.

    Parameters:
    ingress (dict): A dictionary containing any necessary input data (not used in this function).

    Returns:
    list: A list of audience IDs.
    """
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]
    session: Session = provider.connect()

    query = select(audience.id).where(audience.status == True)
    results = session.execute(query).all()

    return list(map(lambda row: row.id, results))
