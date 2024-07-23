from azure.durable_functions import (
    Blueprint,
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
)
from azure.functions import HttpRequest, HttpResponse
import os

bp = Blueprint()


@bp.route(route="audiences/{id?}")
@bp.durable_client_input(client_name="client")
async def starter_http_esquire_audience(
    req: HttpRequest,
    client: DurableOrchestrationClient,
):
    """
    HTTP Trigger function to manage orchestrator instances for audience builds.

    - For DELETE requests, it terminates the orchestrator instance if it is running.
    - For POST requests, it starts or restarts the orchestrator instance for the given ID.

    Parameters:
    req (HttpRequest): The HTTP request object.
    client (DurableOrchestrationClient): The Durable Functions orchestrator client.

    Returns:
    HttpResponse: The HTTP response indicating the status of the operation.
    """
    audiences = []
    try:
        audience = req.get_json()
    except:
        pass
    if id := req.route_params.get("id"):
        audiences.append(id)

    if not len(audiences):
        return HttpResponse(status_code=400)

    settings = {
        "working": {
            "conn_str": "AzureWebJobsStorage",
            "container_name": "{}-largemessages".format(os.environ["TASK_HUB_NAME"]),
            "blob_prefix": "raw",
        },
        "destination": {
            "conn_str": "ESQUIRE_AUDIENCE_CONN_STR",
            "container_name": "general",
            "data_source": os.environ["ESQUIRE_AUDIENCE_DATA_SOURCE"],
            "blob_prefix": "audiences",
        },
        "forceRebuild": True if req.params.get("force", False) else False,
    }

    for audience in audiences:
        # Check the status of the orchestrator instance
        status = await client.get_status(audience, True)
        status.custom_status
        match req.method:
            case "POST":
                if (
                    status.runtime_status == OrchestrationRuntimeStatus.Running
                    and '"next_run"' in str(status.custom_status)
                ):
                    await client.raise_event(
                        audience,
                        "restart",
                        settings,
                    )
                else:
                    if status.runtime_status:
                        await client.purge_instance_history(audience)
                    await client.start_new(
                        orchestration_function_name="orchestrator_esquire_audience",
                        client_input=settings,
                        instance_id=audience,
                    )

    # Create and return a status response
    if len(audiences) == 1:
        return client.create_check_status_response(req, audiences[0])
    else:
        return [
            client.get_client_response_links(req, audience) for audience in audiences
        ]
