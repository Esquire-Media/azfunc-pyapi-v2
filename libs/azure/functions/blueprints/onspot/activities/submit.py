from azure.durable_functions import DurableOrchestrationClient, EntityId
from libs.azure.functions import Blueprint
from libs.onspot import OnSpotAPI
import os
import logging


OSA = OnSpotAPI(
    access_key=os.environ["ONSPOT_ACCESS_KEY"],
    secret_key=os.environ["ONSPOT_SECRET_KEY"],
    api_key=os.environ["ONSPOT_API_KEY"],
)
bp = Blueprint()


@bp.activity_trigger(input_name="instanceId")
@bp.durable_client_input(client_name="client")
async def onspot_activity_submit(instanceId: str, client: DurableOrchestrationClient):
    # Use the calling orchestrator's instance ID to get its request entity
    ## This is an async call so it MUST be awaited
    request = (
        await client.read_entity_state(
            entityId=EntityId(
                name="entity_generic",
                key=f"{instanceId}_request",
            )
        )
    ).entity_state
    
    # Use the calling orchestrator's instance ID to create a response entity
    ## This is an async call so it MUST be awaited
    await client.signal_entity(
        entityId=EntityId(
            name="entity_generic",
            key=f"{instanceId}_response",
        ),
        operation_name="update",
        operation_input={
            "endpoint": request["endpoint"],
            "data": (response := OSA.submit(**request)).json(),
            "code": response.status_code
        },
    )
    
    return ""
