from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions.http import HttpRequest

bp = Blueprint()


@bp.route(route="audiences/maids/tests/fetch")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesMaidsTest_fetch(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # get audiences
    audiences = [
        {
            "id": "a0H6e00000bNazEEAS_test",
            "type": "Friends Family",
        },
        {
            "id": "a0HPK000000e2r72AA",
            "type": "New Movers",
        },
        {
            "id": "a0H5A00000aZbI1UAK",
            "type": "InMarket Shoppers",
        },
    ]

    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_esquireAudiencesMaids_prepare",
        client_input={
            "audiences": audiences,
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
