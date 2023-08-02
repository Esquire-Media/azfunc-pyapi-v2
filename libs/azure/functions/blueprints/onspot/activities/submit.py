# File: libs/azure/functions/blueprints/onspot/activities/submit.py

from libs.azure.functions import Blueprint
from libs.openapi.clients.onspot import OnSpotAPI

OSA = OnSpotAPI()
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
    factory = OSA.createRequest((ingress["endpoint"], "post"))
    _, _, response = await factory.request(ingress["request"])

    return response.json()
