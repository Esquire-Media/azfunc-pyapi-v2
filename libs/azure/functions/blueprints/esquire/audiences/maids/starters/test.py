from libs.azure.functions import Blueprint
from azure.durable_functions import (
    DurableOrchestrationClient,
)
from libs.azure.functions.http import HttpRequest, HttpResponse
import logging

bp: Blueprint = Blueprint()


@bp.route(route="audiences/maids/test")
@bp.durable_client_input(client_name="client")
async def starter_esquireAudiencesMaid_test(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    # get audiences
    test_friends_family = [
        {
            "id": "a0H6e00000bNazEEAS_test",
            "name": "FF_Test",
            "type": "Friends Family",
            "lookback": None,
        }
    ]
    
    # Start a new instance of the orchestrator function
    instance_id = await client.start_new(
        orchestration_function_name="orchestrator_esquireAudiencesMaid_fetch",
        client_input={
            "audiences": test_friends_family,
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
            }
        },
    )

    return client.create_check_status_response(req, instance_id)
