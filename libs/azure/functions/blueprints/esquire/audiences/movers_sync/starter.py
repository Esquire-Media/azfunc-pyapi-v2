# File: libs/azure/functions/blueprints/esquire/audiences/mover_sync/starter.py

from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest
from libs.utils.logging import AzureTableHandler
import json, logging

bp = Blueprint()

# Initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("moversSync.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)

@bp.route(route="esquire/movers-sync/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_moversSync(req: HttpRequest, client: DurableOrchestrationClient):
    """
    HTTP-triggered function to start the Movers Sync orchestrator.

    This function is triggered via an HTTP POST request. It starts a new instance of 
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
    payload = {}  # Empty payload for now, can be populated as needed
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_moversSync_root",
        client_input=payload,
    )

    # Add instance info to the logging table for usage metrics
    logger.info(
        msg="started",
        extra={
            "context": {
                "PartitionKey": "moversSync",
                "RowKey": instance_id,
                **{k: v if isinstance(v, str) else json.dumps(v) for k, v in payload.items()}
            }
        },
    )

    # Return a response with the status query URLs for the orchestrator instance
    return client.create_check_status_response(req, instance_id)
