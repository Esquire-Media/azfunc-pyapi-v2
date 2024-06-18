# File: /libs/azure/functions/blueprints/esquire/audiences/meta/activities/putAudience.py

from azure.durable_functions import Blueprint
from libs.data import from_bind
from sqlalchemy import update
from sqlalchemy.orm import Session

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_esquireAudiencesBuilder_putAudience(ingress: dict):
    """
    Updates audience metadata in the database.

    This activity updates the 'meta' field of the specified audience in the database with the provided metadata.

    Parameters:
    ingress (dict): A dictionary containing the audience ID and the new metadata.
        {
            "audience": {
                "id": str,
                "count": int,
            }
        }

    Returns:
    None: Returns None if the update is successful.

    Raises:
    Exception: If an error occurs during the database operation.
    """
    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]

    session: Session = provider.connect()

    session.execute(
        update(audience)
        .where(audience.id == ingress["audience"]["id"])
        .values(lastDeviceCount=ingress["audience"]["count"])
    )
    session.commit()

    return None
