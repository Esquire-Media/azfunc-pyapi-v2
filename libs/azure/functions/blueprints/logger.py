from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse

import logging

bp = Blueprint()


@bp.route(route="logger", methods=["POST"])
async def logger(req: HttpRequest):
    data = req.get_body()
    logging.warn(data)
    return HttpResponse("OK")
