from azure.durable_functions import Blueprint
from azure.functions import HttpRequest, HttpResponse
import os
import orjson as json

bp = Blueprint()


@bp.route(route="env", methods=["POST"])
async def env(req: HttpRequest):
    return HttpResponse(json.dumps({k: v for k, v in os.environ.items()}))
