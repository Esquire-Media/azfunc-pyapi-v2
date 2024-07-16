# File: libs/azure/functions/blueprints/meta/endpoints/creative_preview.py

from azure.durable_functions import Blueprint
from azure.functions import AuthLevel, HttpRequest, HttpResponse
from libs.openapi.clients.meta import Meta

bp = Blueprint()


@bp.route(
    route="meta_creative_preview",
    methods=["GET"],
    auth_level=AuthLevel.FUNCTION,
)
async def meta_endpoint_creativePreview(req: HttpRequest):
    previews = Meta["AdCreative.Get.Previews"](
        parameters={k: v for k, v in req.params.items() if k != "code"}
    )
    return HttpResponse("".join([p["body"] for p in previews.root.data]))
