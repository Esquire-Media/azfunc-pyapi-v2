from libs.azure.functions import Blueprint
import logging
from libs.utils.logging import AzureTableHandler
import json
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import DurableOrchestrationClient
bp = Blueprint()

# initialize logging features
__handler = AzureTableHandler()
__logger = logging.getLogger("moversSync.logger")
if __handler not in __logger.handlers:
    __logger.addHandler(__handler)

@bp.route(route="esquire/movers-sync/starter", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def starter_moversSync(req: HttpRequest, client: DurableOrchestrationClient):
    logger = logging.getLogger("moversSync.logger")

    # Start a new instance of the orchestrator function
    payload = {}
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_moversSync_root",
        client_input=payload,
    )

    # add instance info to the logging table for usage metrics
    # ensure that each value is json serializable
    logger.info(
        msg="started",
        extra={"context": {"PartitionKey": "moversSync", "RowKey": instance_id, **{k:v if isinstance(v, str) else json.dumps(v) for k,v in payload.items()}}},
    )

    # Return a response that includes the status query URLs
    return client.create_check_status_response(req, instance_id)