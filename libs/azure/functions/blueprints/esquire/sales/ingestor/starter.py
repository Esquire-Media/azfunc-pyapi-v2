from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, AuthLevel
import json

bp: Blueprint = Blueprint()

@bp.route(route="esquire/sales/ingestor", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def sales_ingestion_starter(req: HttpRequest, client: DurableOrchestrationClient):
    body = json.loads(req.get_body())
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_ingestData",
        client_input=body,
    )
    return client.create_check_status_response(req, instance_id)