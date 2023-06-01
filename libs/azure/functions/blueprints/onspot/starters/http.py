from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.blueprints.onspot.helpers import onspot_initializer
from libs.azure.functions.http import HttpRequest
import os

bp = Blueprint()


@bp.route(route="onspot/{*endpoint}", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def onspot_starter_http(req: HttpRequest, client: DurableOrchestrationClient):
    """
    HTTP-triggered function for starting the OnSpot durable function flow.

    Parameters
    ----------
    req : HttpRequest
        The HTTP request object.
    client : DurableOrchestrationClient
        The Durable Orchestration Client for starting the durable function flow.

    Returns
    -------
    HttpResponse
        The HTTP response indicating the status of the durable function flow.

    Examples
    --------
    The following example demonstrates how to trigger the OnSpot starter function via an HTTP POST request:

    Request:
    POST /onspot/endpoint1
    {
        "data": "example"
    }

    Response:
    HTTP 202 Accepted
    {
        "statusQueryGetUri": "/runtime/webhooks/durabletask/instances/<instance_id>?taskHub=<task_hub_name>&connection=Storage&code=<code>",
        "sendEventPostUri": "/runtime/webhooks/durabletask/instances/<instance_id>/raiseEvent/{eventName}?taskHub=<task_hub_name>&connection=Storage&code=<code>",
        "terminatePostUri": "/runtime/webhooks/durabletask/instances/<instance_id>/terminate?reason={text}&taskHub=<task_hub_name>&connection=Storage&code=<code>",
        "rewindPostUri": "/runtime/webhooks/durabletask/instances/<instance_id>/rewind?reason={text}&taskHub=<task_hub_name>&connection=Storage&code=<code>"
    }
    """
    instanceId = await onspot_initializer(
        endpoint=req.route_params["endpoint"],
        request=req.get_json(),
        client=client,
        context={key: value for key, value in req.params.items()},
        response_url=os.environ.get("REVERSE_PROXY", req.url)
    )
    return client.create_check_status_response(request=req, instance_id=instanceId)
