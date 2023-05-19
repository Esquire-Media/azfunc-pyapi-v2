from azure.durable_functions import DurableOrchestrationClient, EntityId
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from libs.onspot.endpoints import REGISTRY, EndpointToStorage

try:
    import simplejson as json
except:
    import json
import uuid

bp = Blueprint()


# An HTTP-Triggered Function with a Durable Functions Client binding
@bp.route(route="onspot/{*endpoint}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def onspot_starter(req: HttpRequest, client: DurableOrchestrationClient):
    # Parse JSON payload or fail
    try:
        data = req.get_json()
    except:
        try:
            data = json.load(req.get_body())
        except:
            return HttpResponse("MALFORMED", status_code=400)

    # Generate an instance ID
    instanceId = str(uuid.uuid4())

    # Use the instance ID to create a settings entity
    await client.signal_entity(
        entityId=EntityId(name="entity_generic", key=f"{instanceId}_settings"),
        operation_name="init",
        operation_input={
            "endpoint": req.route_params["endpoint"],
            "context": {key: value for key, value in req.params.items()},
            "target": (
                "blobs"
                if isinstance(REGISTRY[req.route_params["endpoint"]], EndpointToStorage)
                else "callback"
            ),
            "response_links": client.get_client_response_links(
                request=req, instance_id=instanceId
            ),
        },
    )

    # Use the instance ID to create a data entity
    await client.signal_entity(
        entityId=EntityId(name="entity_generic", key=f"{instanceId}_data"),
        operation_name="init",
        operation_input=data,
    )

    # Start orchestrator
    await client.start_new(
        orchestration_function_name="onspot_orchestrator", instance_id=instanceId
    )

    response = client.create_check_status_response(request=req, instance_id=instanceId)
    return response
