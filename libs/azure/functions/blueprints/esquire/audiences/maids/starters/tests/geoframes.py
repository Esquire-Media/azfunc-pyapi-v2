from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


@bp.route(route="audiences/maids/test/geoframes")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesMaidsTests_geoframes(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # get audiences
    audiences = [
        {
            "id": "a0H5A00000aZbI1UAK",
            "type": "InMarket Shoppers",
        },
    ]

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_esquireAudiencesMaids_geoframes",
        client_input={
            "audiences": audiences,
            "destination": {
                "conn_str": "ONSPOT_CONN_STR",
                "container_name": "general",
                "blob_prefix": "audiences",
            },
        },
    )

    return client.create_check_status_response(req, instance_id)
