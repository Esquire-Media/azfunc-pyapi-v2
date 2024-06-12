from azure.durable_functions import Blueprint
from azure.functions import HttpRequest, HttpResponse
from libs.utils.oauth2.tokens import ValidateWellKnown
import os, logging

bp = Blueprint()


@bp.route(route="example_http", methods=["GET"])
@bp.function_name("_".join(os.path.relpath(__file__.replace(".py", "")).split("\\")))
async def example_http(req: HttpRequest):
    logging.warning(getattr(req, "oauth2", None))
    return HttpResponse("OK")
