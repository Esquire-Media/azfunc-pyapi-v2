# File: libs/azure/functions/blueprints/async_tasks/endpoints/leasing/next.py

from azure.durable_functions import (
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
    EntityId,
)
from azure.durable_functions.models.DurableOrchestrationStatus import (
    DurableOrchestrationStatus,
)
from libs.azure.functions.blueprints.roku_async.schemas import (
    RequestSchema,
    StatusSchema,
)
from datetime import datetime
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse

bp = Blueprint()


@bp.logger()
@bp.easy_auth()
@bp.route(route="async/lease/next", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def roku_async_endpoint_lease_next(
    req: HttpRequest, client: DurableOrchestrationClient
):
    """
    Asynchronously handle lease request and returns status.

    This function checks the headers of the incoming request for client identity. If
    it is present, it then processes the request by filtering the running instances
    and updating their lease if necessary.

    Parameters
    ----------
    req : HttpRequest
        The incoming HTTP request.
    client : DurableOrchestrationClient
        Client to communicate with Azure Durable Functions.

    Returns
    -------
    HttpResponse
        The HTTP response. It contains the status of the request and updated lease.
    """

    schema = RequestSchema()

    # Iterate over running instances
    for s in filter(
        lambda s: not s.instance_id.startswith("@"),
        await client.get_status_by(runtime_status=[OrchestrationRuntimeStatus.Running]),
    ):
        entity = EntityId("roku_async_entity_request", s.instance_id)
        state = await client.read_entity_state(entity)

        # If entity state is a string, load it as such, otherwise load as object
        if isinstance(state.entity_state, str):
            data = schema.loads(state.entity_state)
        else:
            data = schema.load(state.entity_state)

        # Check if lease expired or does not exist and update lease
        if "lease" not in data or data["lease"]["expires"] < datetime.utcnow():
            await client.signal_entity(
                entity,
                "lease",
                {
                    "provider": req.headers.get("x-ms-client-principal-idp"),
                    "access_id": req.headers.get("x-ms-client-principal-id"),
                },
            )

            state = await client.read_entity_state(entity)

            # Process the state the same way as above after update
            if isinstance(state.entity_state, str):
                data = schema.loads(state.entity_state)
            else:
                data = schema.load(state.entity_state)

            data = schema.dump(data)

            # Get status of the orchestration
            status: DurableOrchestrationStatus = await client.get_status(
                s.instance_id, show_history=True, show_history_output=True
            )

            schema = StatusSchema()

            # Load orchestration status into schema
            status = schema.load(
                {
                    "started": status.created_time.isoformat(),
                    "updated": status.last_updated_time.isoformat(),
                    "message": status.custom_status,
                    "state": data,
                }
            )

            # Return status
            return HttpResponse(
                schema.dumps(status),
                headers={"Content-Type": "application/json"},
            )

    # If no suitable instance was found, return 404 error
    return HttpResponse(
        status_code=404,
        headers={"Content-Type": "application/json"},
    )
