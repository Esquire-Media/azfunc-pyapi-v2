from azure.durable_functions import (
    DurableOrchestrationClient,
    OrchestrationRuntimeStatus,
)
from azure.durable_functions.models.DurableOrchestrationStatus import (
    DurableOrchestrationStatus,
)
from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from marshmallow import Schema, fields

bp = Blueprint()


# An HTTP-Triggered Function with a Durable Functions Client binding
@bp.route(route="async/tasks/{instance_id}", methods=["GET"])
@bp.durable_client_input(client_name="client")
@bp.logger()
async def async_task_status(req: HttpRequest, client: DurableOrchestrationClient):
    status: DurableOrchestrationStatus = await client.get_status(
        req.route_params["instance_id"], show_history=True, show_history_output=True
    )
    if not status:
        return HttpResponse(status_code=404)
    
    obj = {
        "started": status.created_time.isoformat(),
        "updated": status.last_updated_time.isoformat(),
    }
    if status.runtime_status == OrchestrationRuntimeStatus.Failed:
        obj["errors"] = [
            {"step": event["FunctionName"], "reason": event["Reason"]}
            for event in status.historyEvents
            if event["EventType"] == "TaskFailed"
        ]
    elif status.runtime_status == OrchestrationRuntimeStatus.Completed:
        obj["message"] = "Complete"
    elif status.runtime_status == OrchestrationRuntimeStatus.Canceled:
        obj["message"] = "Rejected"
    else:
        obj["message"] = status.custom_status
    s = StatusSchema()
    return HttpResponse(
        s.dumps(s.load(obj)), headers={"Content-Type": "application/json"}
    )


class ErrorSchema(Schema):
    step = fields.Str(require=True)
    reason = fields.Str(require=True)


class StatusSchema(Schema):
    started = fields.DateTime(required=True, format="iso")
    updated = fields.DateTime(required=False, format="iso")
    message = fields.Str(required=False)
    errors = fields.List(
        fields.Nested(ErrorSchema),
        required=False,
        allow_none=True,
        default=[],
    )
