from azure.durable_functions import DurableOrchestrationClient, EntityId
from libs.azure.functions.http import HttpRequest
from libs.onspot.endpoints import REGISTRY, EndpointToStorage
import uuid

async def onspot_initializer(req: HttpRequest, client: DurableOrchestrationClient):
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
                if issubclass(REGISTRY[req.route_params["endpoint"]], EndpointToStorage)
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
        operation_input=req.get_json(),
    )

    # Start orchestrator
    await client.start_new(
        orchestration_function_name="onspot_orchestrator", instance_id=instanceId
    )
    
    return instanceId