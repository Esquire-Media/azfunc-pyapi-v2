from azure.durable_functions import (
    Blueprint,
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
)
from azure.functions import TimerRequest
from libs.data import from_bind
from sqlalchemy import select
from sqlalchemy.orm import Session
import os

bp = Blueprint()


@bp.timer_trigger(arg_name="timer", schedule="0 0 0 * * *")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesBuilder_schedule_batch(
    timer: TimerRequest,
    client: DurableOrchestrationClient,
):
    settings = {
        "working": {
            "conn_str": "AzureWebJobsStorage",
            "container_name": "{}-largemessages".format(os.environ["TASK_HUB_NAME"]),
            "blob_prefix": "raw",
        },
        "destination": {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": "general",
            "data_source": os.environ["ESQUIRE_AUDIENCE_DATA_SOURCE"],
            "blob_prefix": "audiences",
        }
    }

    provider = from_bind("keystone")
    audience = provider.models["public"]["Audience"]
    session: Session = provider.connect()
    query = select(audience.id).where(audience.status == True)
    results = session.execute(query).all()

    async for row in results:
        status = await client.get_status(row.id)
        if not status.runtime_status:
            await client.start_new(
                orchestration_function_name="orchestrator_esquire_audience",
                client_input=settings,
                instance_id=row.id,
            )
