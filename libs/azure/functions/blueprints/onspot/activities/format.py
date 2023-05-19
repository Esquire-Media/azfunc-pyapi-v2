from azure.durable_functions import DurableOrchestrationClient, EntityId
from azure.storage.blob import ContainerClient
from libs.azure.functions import Blueprint
from libs.onspot.endpoints import REGISTRY
from libs.utils.helpers import find_key
import os

bp = Blueprint()


@bp.activity_trigger(input_name="instanceId")
@bp.durable_client_input(client_name="client")
async def onspot_activity_format(
    instanceId: str, client: DurableOrchestrationClient
) -> dict:
    # Generate an ID for this request
    # requestId = str(uuid.uuid4())

    # Use the calling orchestrator's instance ID to get its settings entity
    ## This is an async call so it MUST be awaited
    settings = (
        await client.read_entity_state(
            entityId=EntityId("entity_generic", f"{instanceId}_settings")
        )
    ).entity_state

    # Use the calling orchestrator's instance ID to get its data entity
    ## This is an async call so it MUST be awaited
    data = (
        await client.read_entity_state(
            entityId=(entity_data := EntityId("entity_generic", f"{instanceId}_data"))
        )
    ).entity_state

    if settings["target"] == "blobs":
        # Make sure the intended Storage Container exists, if not create it
        conn_str = os.environ.get("ONSPOT_CONN_STR", os.environ["AzureWebJobsStorage"])
        container_name = os.environ.get(
            "ONSPOT_CONTAINER",
            f"{client._orchestration_bindings.task_hub_name}-largemessages",
        )
        container: ContainerClient = ContainerClient.from_connection_string(
            conn_str=conn_str, container_name=container_name
        )
        if not container.exists():
            container.create_container()

    # Autofill and validate the original data and replace the data entity with the results
    request_schema = REGISTRY[settings["endpoint"]].request(
        context={
            "hash": False,
            "callback": lambda self_, data_: (
                settings["response_links"]["sendEventPostUri"].replace(
                    "{eventName}", 
                    # f"{requestId}_{data_.get('name', '')}",
                    data_.get('name', ''),
                )
            ),
            **(
                {
                    "outputAzConnStr": conn_str,
                    "outputLocation": f"{container_name}/{instanceId}/results",
                }
                if settings["target"] == "blobs"
                else {}
            ),
            **settings["context"],
        }
    )
    request_serialized = request_schema.dumps(request := request_schema.load(data))

    # Use the orchestrator instance ID and request ID to create a request entity
    await client.signal_entity(
        entityId=EntityId(
            name="entity_generic", 
            # key=f"{instanceId}_request_{requestId}",
            key=f"{instanceId}_request",
        ),
        operation_name="update",
        operation_input={
            "endpoint": settings["endpoint"],
            "request": request_serialized,
        },
    )
    
    # Return all the event IDs
    return find_key(request, "name")
    
    await client.signal_entity(
        entityId=EntityId(name="entity_generic", key=f"{instanceId}_requests"),
        operation_name="update",
        operation_input={requestId: find_key(request, "name")},
    )

    return {"requestId": requestId, "eventIds": find_key(request, "name")}
