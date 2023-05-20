from azure.durable_functions import DurableOrchestrationClient, EntityId
from azure.storage.blob import BlobClient
from libs.azure.functions import Blueprint
from libs.onspot.endpoints import REGISTRY
from libs.onspot.schemas.responses import ResponseWithSaveSchema
import logging

bp = Blueprint()


@bp.activity_trigger(input_name="instanceId")
@bp.durable_client_input(client_name="client")
async def onspot_activity_result(instanceId: str, client: DurableOrchestrationClient):
    # Use the calling orchestrator's instance ID to get its response entity
    ## This is an async call so it MUST be awaited
    responses = (
        await client.read_entity_state(
            entityId=EntityId("entity_generic", f"{instanceId}_response")
        )
    ).entity_state

    # Get schemas
    response_schema = REGISTRY[responses["endpoint"]].response()
    callback_schema = REGISTRY[responses["endpoint"]].callback()

    # Parse repsonses data
    responses = {
        r.pop("id"): r for r in response_schema.load(responses.pop("data"), many=True)
    }

    # Use the calling orchestrator's instance ID to get its external event history
    events = [
        e
        for event in (
            await client.get_status(instance_id=instanceId, show_history=True)
        ).historyEvents
        if event["EventType"] == "EventRaised"
        if (e := callback_schema.loads(event["Input"]))
    ]

    # Format results
    if isinstance(response_schema, ResponseWithSaveSchema):
        data = {
            responses[e["id"]]["name"]: {
                "url": url,
                "size": blob.get_blob_properties().size,
            }
            for e in events
            if e["success"]
            if (
                blob := BlobClient.from_blob_url(
                    url := responses[e["id"]]["location"].replace("az://", "https://")
                )
            )
        }
        errors = [e for e in events if not e["success"]]
    else:
        data = {
            name: e
            for e in events
            if e["cbInfo"]["success"]
            if e.pop("cbInfo")
            if (name := e.pop("name"))
        }
        errors = [e["cbInfo"] for e in events if not e["cbInfo"]["success"]]

    return {"data": data, "errors": errors}
