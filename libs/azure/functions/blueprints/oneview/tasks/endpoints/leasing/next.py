# File: libs/azure/functions/blueprints/oneview/tasks/endpoints/leasing/next.py

from azure.durable_functions import (
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
)
from azure.durable_functions.models.DurableOrchestrationStatus import (
    DurableOrchestrationStatus,
)
from libs.azure.functions.blueprints.oneview.tasks.schemas import (
    RequestSchema,
    StatusSchema,
)
from libs.azure.functions.blueprints.oneview.tasks.helpers import (
    state as OrchestratorState,
    process_state as OrchestartorStateOperation,
)
from datetime import datetime
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse

bp = Blueprint()


@bp.easy_auth()
@bp.route(route="async/lease/next", methods=["GET"])
@bp.durable_client_input(client_name="client")
async def oneview_endpoint_lease_next(
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

    # Iterate over running instances
    for s in filter(
        lambda s: not s.instance_id.startswith("@"),
        await client.get_status_by(runtime_status=[OrchestrationRuntimeStatus.Running]),
    ):
        state = RequestSchema().load(OrchestratorState(s.instance_id))

        # Check if lease expired or does not exist and update lease
        if "lease" not in state.keys() or state["lease"]["expires"] < datetime.utcnow():
            state = OrchestartorStateOperation(
                s.instance_id,
                "lease",
                {
                    "provider": req.headers.get("x-ms-client-principal-idp"),
                    "access_id": req.headers.get("x-ms-client-principal-id"),
                },
            )

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
                    "state": state,
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
