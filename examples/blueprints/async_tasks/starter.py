from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from urllib.parse import urlparse

try:
    import simplejson as json
except:
    import json

# Create a Blueprint instance
bp = Blueprint()

@bp.route(route="async/tasks", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def async_task_starter(req: HttpRequest, client: DurableOrchestrationClient):
    """
    Route handler for starting asynchronous tasks.

    Parameters
    ----------
    req : HttpRequest
        The HTTP request object.
    client : DurableOrchestrationClient
        The Durable Orchestration Client.

    Returns
    -------
    HttpResponse
        The HTTP response object.

    Notes
    -----
    This route handler processes the POST request to start asynchronous tasks.
    It receives the payload from the request and uses the Durable Orchestration Client to start a new orchestration.

    Steps:
    1. Try to parse the payload from the request body as JSON.
    2. If parsing fails, try to load the payload as JSON from the request body.
    3. If loading fails, return a 400 Bad Request response.
    4. Start a new orchestration with the "async_task_handler" function and the payload as the client input.
    5. Construct the status query URI based on the request URL.
    6. Return an HTTP response with the instance ID and the status query URI in JSON format.

    This route handler can be customized to handle additional validation, processing, or customization of the orchestration start process based on the application's requirements.
    """
    try:
        payload = req.get_json()
    except:
        try:
            payload = json.loads(req.get_body())
        except:
            return HttpResponse(None, status_code=400)

    instance_id = await client.start_new("async_task_handler", client_input=payload)
    url = urlparse(req.url)
    return HttpResponse(
        json.dumps(
            {
                "instance_id": instance_id,
                "statusQueryGetUri": f"{url.scheme}://{url.netloc}{url.path}/{instance_id}",
            }
        ),
        headers={"Content-Type": "application/json"},
    )
