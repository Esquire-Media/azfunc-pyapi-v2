from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationClient,
    # DurableOrchestrationContext,
    EntityId,
    # DurableEntityContext,
)
from libs.azure.functions.http import HttpRequest, HttpResponse
import uuid

bp:Blueprint = Blueprint()

@bp.route(route="audiences/devtest")
@bp.durable_client_input(client_name="client")
async def daily_audience_generation_starter(req: HttpRequest, client:DurableOrchestrationClient):
    
    # Generate a unique instance ID for the orchestration
    # instance_id = str(uuid.uuid4())

    # # Signal a Durable Entity to instantiate and set its state
    # await client.signal_entity(
    #     entityId=EntityId(name="example_entity", key=f"{instance_id}"),
    # )

    # Start a new instance of the orchestrator function
    # instance_id = await client.start_new(
    #     orchestration_function_name="daily_audience_generation_orchestrator",
    #     # instance_id=instance_id,
    # )

    return HttpResponse("Success!", status_code=200)

def daily_audience_generation_orchestrator():
    pass