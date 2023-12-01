from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


@bp.route(route="audiences/maids/test/addresses")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesMaid_test(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # get audiences
    audiences = [
        {
            "id": "a0HPK000000e2r72AA",
            "type": "New Movers",
        },
    ]

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_esquireAudiencesMaids_addresses",
        client_input={
            "audiences": audiences[1:],
            "destination": {
                "conn_str": "ONSPOT_CONN_STR",
                "container_name": "general",
                "blob_prefix": "audiences",
            },
        },
    )

    return client.create_check_status_response(req, instance_id)