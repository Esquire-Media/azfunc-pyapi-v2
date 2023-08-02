# File: libs/azure/functions/blueprints/async_tasks/endpoints/leasing/renew.py

from azure.durable_functions import DurableOrchestrationClient
from azure.durable_functions.models.DurableOrchestrationStatus import (
    DurableOrchestrationStatus,
)
from datetime import datetime
from libs.azure.functions.blueprints.oneview.helpers import (
    state as OrchestratorState,
    process_state as OrchestartorStateOperation,
)
from libs.azure.functions.blueprints.oneview.schemas import (
    RequestSchema,
    StatusSchema,
)
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse

bp = Blueprint()


@bp.easy_auth()
@bp.route(route="async/lease/renew/{instance_id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def oneview_endpoint_lease_renew(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Asynchronous method to handle lease renew requests.

    This method checks if a specific lease entity exists and renews it if it does.
    The entity is identified by an instance_id which is part of the route.

    Parameters
    ----------
    req : HttpRequest
        The http request object.
    client : DurableOrchestrationClient
        The Azure durable functions client.

    Returns
    -------
    HttpResponse
        The http response containing the lease status and information,
        a 403 status code if the headers are not set properly,
        or a 404 status code if the entity does not exist.
    """

    state = RequestSchema().load(OrchestratorState(req.route_params["instance_id"]))
    # Checking if the entity exists and renewing the lease if it does
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
                    "lease",
                    {
                        "provider": req.headers.get("x-ms-client-principal-idp"),
                        "access_id": req.headers.get("x-ms-client-principal-id"),
                    },
                )

                status: DurableOrchestrationStatus = await client.get_status(
                    req.route_params["instance_id"],
                    show_history=True,
                    show_history_output=True,
                )
                schema = StatusSchema()
                status = schema.load(
                    {
                        "started": status.created_time.isoformat(),
                        "updated": status.last_updated_time.isoformat(),
                        "message": status.custom_status,
                        "state": state,
                    }
                )
                # Return a HTTP response with the updated status of the lease
                return HttpResponse(
                    schema.dumps(status),
                    headers={"Content-Type": "application/json"},
                )
        return HttpResponse(
            status_code=403,
            headers={"Content-Type": "application/json"},
        )

    # Return a 404 status code if the entity does not exist
    return HttpResponse(
        status_code=404,
        headers={"Content-Type": "application/json"},
    )
