from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


@bp.route(route="audiences/builder/test")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesBuilder_test(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_esquireAudiences_batch",
        client_input={
            "source": {
                "conn_str": "ONSPOT_CONN_STR",
                "container_name": "general",
                "blob_prefix": "audiences",
            },
            "working": {
                "conn_str": "ONSPOT_CONN_STR",
                "container_name": "general",
                "blob_prefix": "raw",
            },
            "destination": {
                "conn_str": "ONSPOT_CONN_STR",
                "container_name": "general",
                "blob_prefix": "audiences",
            },
            "fetch": True
        },
    )
    
    return client.create_check_status_response(req, instance_id)