from azure.durable_functions import DurableOrchestrationClient, EntityId
from libs.azure.functions import Blueprint
from libs.onspot.endpoints import REGISTRY
import logging

bp = Blueprint()


@bp.activity_trigger(input_name="instanceId")
@bp.durable_client_input(client_name="client")
async def onspot_activity_result(instanceId: str, client: DurableOrchestrationClient):
    # Use the calling orchestrator's instance ID to get its settings entity
    ## This is an async call so it MUST be awaited
    settings = (
        await client.read_entity_state(
            entityId=EntityId("entity_generic", f"{instanceId}_settings")
        )
    ).entity_state

    callback_schema = REGISTRY[settings["endpoint"]].callback()

    # Use the calling orchestrator's instance ID to get its external event history
    match settings["target"]:
        case "blobs":
            pass
        case "callback":
            results = [
                data
                for event in (
                    await client.get_status(instance_id=instanceId, show_history=True)
                ).historyEvents
                if event["EventType"] == "EventRaised"
                if (data := callback_schema.loads(event["Input"]))
                if data.pop("cbInfo")
            ]

    return results
