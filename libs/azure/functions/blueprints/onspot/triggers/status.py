# File: libs/azure/functions/blueprints/onspot/triggers/status.py

from azure.functions import AuthLevel
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.openapi.clients import OnSpotAPI

bp = Blueprint()


@bp.route(
    route="onspot/status",
    methods=["get"],
    auth_level=AuthLevel.FUNCTION,
)
async def http_onspot_status(req: HttpRequest) -> HttpResponse:
    OSA = OnSpotAPI(production=True)
    stat = OSA.createRequest(("/status/queue", "get"))
    _, _, resp = await stat.request()
    return HttpResponse(resp.content, mimetype="application/json")
