from libs.azure.functions import Blueprint
from libs.openapi.clients.onspot import OnSpotAPI

OSA = OnSpotAPI()
bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
async def onspot_activity_submit(ingress: dict):
    factory = OSA.createRequest((ingress["endpoint"], "post"))
    _, _, response = await factory.request(ingress["request"])

    return response.json()
