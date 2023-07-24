# File: libs/azure/functions/blueprints/async_tasks/endpoints/leasing/break.py

from azure.durable_functions import (
    DurableOrchestrationClient,
    EntityId,
)
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse

bp = Blueprint()


@bp.logger()
@bp.easy_auth()
@bp.route(route="async/lease/break/{instance_id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def roku_async_endpoint_lease_release(req: HttpRequest, client: DurableOrchestrationClient):
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
    entity = EntityId("roku_async_entity_request", req.route_params["instance_id"])
    state = await client.read_entity_state(entity)

    # If entity exists, send a signal to release lease
    if state.entity_exists:
        await client.signal_entity(
            entity,
            "release",
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
