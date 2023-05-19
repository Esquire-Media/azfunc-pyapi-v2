from libs.azure.functions import Blueprint
from azure.durable_functions import DurableOrchestrationContext
import logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def onspot_orchestrator(context: DurableOrchestrationContext):
    # Format the request
    eventIds = yield context.call_activity(
        name="onspot_activity_format", input_=context.instance_id
    )

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
