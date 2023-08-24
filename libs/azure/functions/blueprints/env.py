from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
import os, json

bp = Blueprint()


@bp.route(route="env", methods=["POST"])
async def env(req: HttpRequest):
    return HttpResponse(json.dumps({k: v for k, v in os.environ.items()}, indent=2))
