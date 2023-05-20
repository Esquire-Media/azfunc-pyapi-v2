from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.onspot.helpers import onspot_initializer
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


# An HTTP-Triggered Function with a Durable Functions Client binding
@bp.route(route="onspot/{*endpoint}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def onspot_starter(req: HttpRequest, client: DurableOrchestrationClient):
    # Generate an instance ID
    instanceId = await onspot_initializer(req=req, client=client)
    return client.create_check_status_response(request=req, instance_id=instanceId)
