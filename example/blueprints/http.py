from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
import logging

bp = Blueprint()


@bp.route(route="example_http", methods=["GET"])
async def example_http(req: HttpRequest):
    logging.warning(req.get_body())
    return HttpResponse("OK")
