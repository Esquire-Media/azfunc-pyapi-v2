from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.onspot.helpers import onspot_initializer
from libs.azure.functions.http import HttpRequest
import os
bp = Blueprint()


# An HTTP-Triggered Function with a Durable Functions Client binding
@bp.route(route="onspot/{*endpoint}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def onspot_starter_http(req: HttpRequest, client: DurableOrchestrationClient):
    # Generate an instance ID and start the OnSpot durable function flow
    instanceId = await onspot_initializer(
        endpoint=req.route_params["endpoint"],
        request=req.get_json(),
        client=client,
        context={key: value for key, value in req.params.items()},
        response_url=os.environ.get("REVERSE_PROXY", req.url)
    )
    return client.create_check_status_response(request=req, instance_id=instanceId)
