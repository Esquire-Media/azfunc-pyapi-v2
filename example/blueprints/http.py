from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.utils.oauth2.tokens import ValidateWellKnown
import os, logging

bp = Blueprint()


@bp.oauth2(
    [
        ValidateWellKnown(
            openid_config_url=os.environ["OAUTH2_CONFIG_URL"],
            audience=os.environ["OAUTH2_AUDIENCE"],
        )
    ]
)
@bp.route(route="example_http", methods=["GET"])
@bp.function_name("_".join(os.path.relpath(__file__.replace(".py", "")).split("\\")))
async def example_http(req: HttpRequest):
    logging.warning(getattr(req, "oauth2", None))
    return HttpResponse("OK")
