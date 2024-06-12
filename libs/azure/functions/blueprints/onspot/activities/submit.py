# File: libs/azure/functions/blueprints/onspot/activities/submit.py

from azure.durable_functions import Blueprint
from libs.openapi.clients.onspot import OnSpot

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def onspot_activity_submit(ingress: dict):
    """
    Submits a request to the OnSpotAPI and returns the response.

    This function creates a request for a specific endpoint and HTTP method
    (POST), sends the request, and returns the response.

    Parameters
    ----------
    ingress : dict
        The input for the activity function, including the endpoint and request.

    Returns
    -------
    dict
        The response from the OnSpotAPI as a JSON object.
    """
    data = OnSpot[(ingress["endpoint"], "post")](ingress["request"])

    return (
        [d.model_dump() for d in data] if isinstance(data, list) else data.model_dump()
    )
