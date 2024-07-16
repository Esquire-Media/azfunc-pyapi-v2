# File: libs/azure/functions/blueprints/onspot/triggers/status.py

from azure.durable_functions import Blueprint
from azure.functions import AuthLevel, HttpRequest, HttpResponse
from libs.openapi.clients import OnSpotAPI

bp = Blueprint()


@bp.route(
    route="onspot/status",
    methods=["get"],
    auth_level=AuthLevel.FUNCTION,
)
async def onspot_status(req: HttpRequest) -> HttpResponse:
    OSA = OnSpotAPI(production=True)
    stat = OSA.createRequest(("/status/queue", "get"))
    _, _, resp = await stat.request()
    return HttpResponse(resp.content, mimetype="application/json")
