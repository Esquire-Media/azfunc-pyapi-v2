# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/starter.py

from azure.durable_functions import DurableOrchestrationClient
from azure.durable_functions import Blueprint
from azure.functions import TimerRequest
import os
from azure.data.tables import TableClient

bp = Blueprint()

@bp.timer_trigger(arg_name="timer", schedule="0 0 8 * * *")
@bp.durable_client_input(client_name="client")
async def starter_pixelPush(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    """
    Timer-triggered function to start the Unmasked Pixel Push orchestrator.

    This function is triggered via a timer request. It starts a new instance of
    the Unmasked Pixel Push orchestrator function and logs the initiation with relevant details.

    Parameters
    ----------
    req : HttpRequest
        The incoming HTTP request that triggered this function.
    client : DurableOrchestrationClient
        Client object to interact with the Durable Functions runtime.

    Returns
    -------
    HttpResponse
        The response object with details to check the status of the started orchestrator instance.

    """    

    # Start a new instance of the orchestrator function
    await client.start_new(
        orchestration_function_name="orchestrator_pixelPush_root",
        client_input={
            "access_key": "DISTILLED_ACCESS_KEY",
            "secret_key": "DISTILLED_SECRET_KEY",
            "bucket": "DISTILLED_BUCKET",
            "region": "DISTILLED_REGION",
            "database": "DISTILLED_DATABASE",
            "workgroup": "DISTILLED_WORKGROUP",
            "runtime_container":{
                "conn_str": "AzureWebJobsStorage",
                "container_name": f"{os.environ['TASK_HUB_NAME']}-largemessages",
            }
        },
    )