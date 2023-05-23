from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from libs.utils.helpers import parse_exception
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def onspot_orchestrator(context: DurableOrchestrationContext):
    # Format the request
    try:
        eventIds = yield context.call_activity(
            name="onspot_activity_format", input_=context.instance_id
        )
    except Exception as e:
        return {"errors": parse_exception(e.args[0])}

    # Prepare for callbacks using external events
    events = [
        context.wait_for_external_event(eventId)
        for eventId in eventIds
    ]

    # Submit request
    yield context.call_activity(
        name="onspot_activity_submit",
        input_=context.instance_id,
    )

    # Wait for all of the callbacks
    yield context.task_all(events)
    
    # Process callbacks to get results
    results = yield context.call_activity(
        name="onspot_activity_result",
        input_=context.instance_id
    )

    return results
