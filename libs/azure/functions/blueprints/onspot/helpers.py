from azure.durable_functions import DurableOrchestrationClient, EntityId
from libs.onspot.endpoints import REGISTRY, EndpointToStorage
import uuid


async def onspot_initializer(
    endpoint: str,
    request: dict,
    client: DurableOrchestrationClient,
    context: dict = None,
    response_url: str = None,
):
    # Generate an instance ID
    instanceId = str(uuid.uuid4())

    # Use the instance ID to create a settings entity
    await client.signal_entity(
        entityId=EntityId(name="entity_generic", key=f"{instanceId}_settings"),
        operation_name="init",
        operation_input={
            "endpoint": endpoint,
            "context": context,
            "links": {
                k: client._replace_url_origin(response_url, v)
                if response_url
                else v.replace(
                    client._orchestration_bindings.management_urls["id"], instanceId
                )
                for k, v in client._orchestration_bindings.management_urls.copy().items()
            },
        },
    )

    # Use the instance ID to create a data entity
    await client.signal_entity(
        entityId=EntityId(name="entity_generic", key=f"{instanceId}_data"),
        operation_name="init",
        operation_input=request,
    )

    # Start orchestrator
    await client.start_new(
        orchestration_function_name="onspot_orchestrator", instance_id=instanceId
    )

    return instanceId
