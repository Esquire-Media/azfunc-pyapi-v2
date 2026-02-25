import os

from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse

bp = Blueprint()


@bp.route(route="{id?}")
@bp.durable_client_input(client_name="client")
async def starter_http_meta_audience(req: HttpRequest, client: DurableOrchestrationClient):
    """
    HTTP Trigger to start/restart the Meta Custom Audience orchestrator.

    The orchestrator fetches audience details and determines blob prefix internally.
    """
    if audience_id := req.route_params.get("id"):
        instance_id = audience_id
    else:
        return HttpResponse(status_code=400)

    if req.method == "POST":
        settings = {
            "audience": {"id": audience_id},
            "destination": {
                "conn_str": "AzureWebJobsStorage",
                "container_name": "general",
                "data_source": os.environ.get("ESQUIRE_AUDIENCE_DATA_SOURCE", "esquire-audiences"),
                "blob_prefix": "audiences",
            },
        }

        # Check if orchestration already exists
        existing_status = await client.get_status(instance_id)

        if existing_status:
            if existing_status.runtime_status == "Running":
                # Return status of running orchestration
                return client.create_check_status_response(req, instance_id)
            elif existing_status.runtime_status in ("Failed", "Terminated", "Canceled"):
                # Purge failed/terminated instances before starting new
                await client.purge_instance(instance_id)
            # For Completed, we can restart

        try:
            await client.start_new(
                orchestration_function_name="meta_customaudience_orchestrator",
                client_input=settings,
                instance_id=instance_id,
            )
        except Exception as ex:
            # Check if it's an orchestration already exists error
            if "OrchestrationAlreadyExistsException" in str(ex) or "already exists" in str(ex).lower():
                # Return status of running orchestration
                return client.create_check_status_response(req, instance_id)
            # Re-raise for other exceptions
            raise

    return client.create_check_status_response(req, instance_id)
