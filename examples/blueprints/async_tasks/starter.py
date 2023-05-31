from azure.durable_functions import DurableOrchestrationClient
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from urllib.parse import urlparse

try:
    import simplejson as json
except:
    import json

bp = Blueprint()


# An HTTP-Triggered Function with a Durable Functions Client binding
@bp.route(route="async/tasks", methods=["POST"])
@bp.durable_client_input(client_name="client")
async def async_task_starter(req: HttpRequest, client: DurableOrchestrationClient):
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
