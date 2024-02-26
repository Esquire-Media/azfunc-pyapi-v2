# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/starter.py

from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest
from azure.functions import TimerRequest
from libs.utils.logging import AzureTableHandler
import json, logging
import requests

bp = Blueprint()

# Initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("moversSync.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)

@bp.timer_trigger(arg_name="timer", schedule="0 0 8 * * *")
async def starter_moversSync(timer: TimerRequest, client: DurableOrchestrationClient):
    """
    Timer-triggered function to start the Movers Sync orchestrator.

    This function is triggered via a timer request (once per day at 3 AM). It starts a new instance of 
    the Movers Sync orchestrator function and logs the initiation with relevant details.

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
    logger = logging.getLogger("moversSync.logger")

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_moversSync_root",
        client_input={},
    )

    # Add instance info to the logging table for usage metrics
    logger.info(
        msg="started",
        extra={
            "context": {
                "PartitionKey": "moversSync",
                "RowKey": instance_id,
                # **{k: v if isinstance(v, str) else json.dumps(v) for k, v in payload.items()}
            }
        },
    )