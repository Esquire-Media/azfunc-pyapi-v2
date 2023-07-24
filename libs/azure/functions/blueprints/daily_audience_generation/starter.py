from libs.azure.functions import Blueprint
import pandas as pd
import os
import json
from io import BytesIO
from azure.durable_functions import (
    DurableOrchestrationClient,
    DurableOrchestrationContext,
    EntityId,
    RetryOptions
)
from azure.storage.blob import BlobServiceClient
from libs.azure.functions.http import HttpRequest, HttpResponse
from sqlalchemy.orm import Session
from libs.data import from_bind
import geojson
import uuid

bp:Blueprint = Blueprint()

# daily trigger
# @bp.timer_trigger(arg_name="timer", schedule="0 0 0 * * *")

@bp.route(route="audiences/devtest")
@bp.durable_client_input(client_name="client")
async def starter_daily_audience_generation(req: HttpRequest, client:DurableOrchestrationClient):
    
    # Generate a unique instance ID for the orchestration
    # instance_id = str(uuid.uuid4())

    # # Signal a Durable Entity to instantiate and set its state
    # await client.signal_entity(
    #     entityId=EntityId(name="example_entity", key=f"{instance_id}"),
    # )

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_daily_audience_generation",
        # instance_id=instance_id,
    )

    return HttpResponse("Success!", status_code=200)