# File: libs/azure/functions/blueprints/onspot/starters/http.py

from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


@bp.route(route="onspot/{*endpoint}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def onspot_starter_http(
    req: HttpRequest, client: DurableOrchestrationClient
):
    endpoint = "/" + req.route_params["endpoint"]
    instance_id = await client.start_new(
        "onspot_orchestrator",
        None,
        {
            "endpoint": endpoint,
            "request": req.get_json(),
        },
    )
    return client.create_check_status_response(request=req, instance_id=instance_id)
