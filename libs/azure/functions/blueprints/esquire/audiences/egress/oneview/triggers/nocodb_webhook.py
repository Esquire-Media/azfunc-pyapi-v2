# File: libs/azure/functions/blueprints/esquire/audiences/oneview/triggers/nocodb_webhook.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import AuthLevel
from azure.durable_functions import Blueprint
from azure.functions import HttpRequest, HttpResponse

bp = Blueprint()


@bp.route(
    route="oneview/segment/nocodb",
    methods=["POST"],
    auth_level=AuthLevel.FUNCTION,
)
@bp.durable_client_input(client_name="client")
async def esquire_audiences_oneview_nocodb_webhook(
    req: HttpRequest, client: DurableOrchestrationClient
) -> HttpResponse:
    """
    Webhook trigger for NocoDB updates related to OneView segments.

    This webhook is triggered when there are updates in the NocoDB related to
    OneView segments. It initiates the `esquire_audiences_oneview_segment_updater`
    orchestrator for each record received in the webhook payload.

    Parameters
    ----------
    req : HttpRequest
        Incoming HTTP request object containing the webhook payload.
    client : DurableOrchestrationClient
        Azure Durable Functions client to interact with Durable Functions extension.

    Returns
    -------
    HttpResponse
        HTTP response object indicating the status of the webhook processing.
    """

    # Loop through the rows in the received data and initiate orchestrators
    for record in [None] + req.get_json()["data"]["rows"]:
        # Start the `esquire_audiences_oneview_segment_updater` orchestrator
        await client.start_new(
            "esquire_audiences_oneview_segment_updater",
            None,
            record,
        )

    # Return a HTTP response indicating successful processing of the webhook
    return HttpResponse("OK")
