from azure.durable_functions import Blueprint
from azure.functions import HttpRequest, HttpResponse
from libs.openapi.clients import specifications
import orjson as json, yaml

# Create a Blueprint instance
bp = Blueprint()


@bp.route(route="docs/yaml/{spec}", methods=["GET"])
async def documentation_yaml(req: HttpRequest):
    if req.route_params.get("spec") in specifications.keys():
        return HttpResponse(
            yaml.dump(specifications[req.route_params["spec"]]()),
            headers={"Content-Type": "text/vnd.yaml"},
        )
    return HttpResponse(status_code=404)


@bp.route(route="docs/json/{spec}", methods=["GET"])
async def documentation_json(req: HttpRequest):
    if req.route_params.get("spec") in specifications.keys():
        return HttpResponse(
            json.dumps(specifications[req.route_params["spec"]]()),
            headers={"Content-Type": "application/json"},
        )
    return HttpResponse(status_code=404)
