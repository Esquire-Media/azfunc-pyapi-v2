from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from urllib.parse import urlparse

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def onspot_orchestrator(context: DurableOrchestrationContext):
    # Format the request
    request = yield context.call_activity(
        name="onspot_async_activity_format",
        input_={
            "instance_id": context.instance_id,
            "request": context.get_input()["request"],
        },
    )

    # Prepare for callbacks using external events
    events = [
        context.wait_for_external_event(
            urlparse(f["properties"]["callback"]).path.split("/")[-1]
        )
        for f in request["features"]
    ]

    # Submit request
    jobs = yield context.call_activity(
        name="onspot_async_activity_submit",
        input_={
            "endpoint": context.get_input()["endpoint"],
            "request": request,
        },
    )

    # Wait for all of the callbacks
    callbacks = yield context.task_all(events)

    return {"jobs": jobs, "callbacks": callbacks}
