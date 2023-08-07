# File: libs/azure/functions/blueprints/oneview/tasks/endpoints/leasing/break.py

from azure.durable_functions import DurableOrchestrationClient
from datetime import datetime
from libs.azure.functions.blueprints.oneview.tasks.helpers import (
    state as OrchestratorState,
    process_state as OrchestartorStateOperation,
)
from libs.azure.functions.blueprints.oneview.tasks.schemas import (
    RequestSchema,
)
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse

bp = Blueprint()


@bp.logger()
@bp.easy_auth()
@bp.route(route="async/lease/break/{instance_id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def oneview_endpoint_lease_release(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Asynchronously handle lease release request and return status.

    This function checks the headers of the incoming request for client identity. If
    it is present, it then processes the request by sending a signal to release
    the lease of the specified instance if it exists.

    Parameters
    ----------
    req : HttpRequest
        The incoming HTTP request.
    client : DurableOrchestrationClient
        Client to communicate with Azure Durable Functions.

    Returns
    -------
    HttpResponse
        The HTTP response. It contains the status of the request.
    """

    # Create entity with the provided instance id from the route params
    state = RequestSchema().load(OrchestratorState(req.route_params["instance_id"]))

    # If entity exists, send a signal to release lease
    if state:
        if "lease" in state.keys():
            if (
                state["lease"]["expires"] > datetime.utcnow()
                and state["lease"]["provider"]
                == req.headers.get("x-ms-client-principal-idp")
                and state["lease"]["access_id"]
                == req.headers.get("x-ms-client-principal-id")
            ):
                state = OrchestartorStateOperation(
                    req.route_params["instance_id"],
                    "break",
                    {
                        "provider": req.headers.get("x-ms-client-principal-idp"),
                        "access_id": req.headers.get("x-ms-client-principal-id"),
                    },
                )

        # Return response after lease release signal
        return HttpResponse(
            headers={"Content-Type": "application/json"},
        )

    # If no suitable entity was found, return 404 error
    return HttpResponse(
        status_code=404,
        headers={"Content-Type": "application/json"},
    )
