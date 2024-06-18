from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import TimerRequest
import os

bp = Blueprint()


@bp.timer_trigger(arg_name="timer", schedule="0 0 0 * * *")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesBuilder_schedule_batch(
    timer: TimerRequest,
    client: DurableOrchestrationClient,
):
    # Start a new instance of the orchestrator function
    await client.start_new(
        orchestration_function_name="orchestrator_esquireAudiences_batch",
        client_input={
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
            },
        },
    )
