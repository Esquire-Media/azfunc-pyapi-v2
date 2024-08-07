# File: libs/azure/functions/blueprints/oneview/tasks/endpoints/starter.py

from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import HttpRequest, HttpResponse
from libs.azure.functions.blueprints.oneview.tasks.helpers import request_initializer
from urllib.parse import urlparse
import orjson as json, os

bp = Blueprint()


@bp.logger()
@bp.easy_auth()
@bp.route(route="async/tasks", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def oneview_endpoint_starter(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Entry point for asynchronous tasks. It initializes the tasks and returns an
    instance ID along with a status query URI.

    Parameters
    ----------
    req : HttpRequest
        The incoming request, from which headers and JSON payload are extracted.
    client : DurableOrchestrationClient
        The client object for Azure Durable Functions, used to start orchestrator
        functions and communicate with them.

    Returns
    -------
    HttpResponse
        The response, containing a JSON object with the instance ID and the status
        query URI, or an error message if there was a problem.
    """
    # Extract JSON payload from request body
    try:
        payload = req.get_json()
    except:
        try:
            payload = json.loads(req.get_body())
        except:
            return HttpResponse(None, status_code=400)

    # Initialize request and get instance id
    instanceId = await request_initializer(
        request=payload,
        client=client,
        response_url=os.environ.get("REVERSE_PROXY", req.url),
    )

    # Build status query URI
    url = urlparse(req.url)
    return HttpResponse(
        json.dumps(
            {
                "instance_id": instanceId,
                "statusQueryGetUri": f"{url.scheme}://{url.netloc}{url.path}/{instanceId}",
            }
        ),
        headers={"Content-Type": "application/json"},
    )
