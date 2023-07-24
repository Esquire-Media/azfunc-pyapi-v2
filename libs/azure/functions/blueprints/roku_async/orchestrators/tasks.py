# File: blueprints/orchestrators/tasks.py

from azure.durable_functions import DurableOrchestrationContext, EntityId
from blueprints.roku_async.schemas import RequestSchema
from blueprints.roku_async.helpers import calculate_missing_campaigns, calculate_missing_flights
from libs.azure.functions import Blueprint

bp = Blueprint()


# Define the orchestrator function
@bp.logger()
@bp.orchestration_trigger(context_name="context")
def roku_async_orchestrator_tasks(context: DurableOrchestrationContext):
    """
    The orchestrator function for asynchronous tasks. This function manages the
    execution of tasks, including the validation of a creative asset, the
    registration of an advertiser, campaign, and creative, and the registration
    of flights. If an error occurs during the execution of these tasks, a
    notification is sent.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context object for the Azure Durable Functions orchestrator function,
        used to interact with the durable function runtime.

    Returns
    -------
    str
        A string representation of the current state of the instance, which includes
        the request details, links to the status and termination endpoints, and the
        current and existing states of the advertiser, campaign, creative, and flights.
        If an error occurs, an exception is raised and no value is returned.

    Raises
    ------
    Exception
        If an exception occurs during the execution of the tasks, the exception is
        raised after a notification is sent.
    """
    try:
        schema = RequestSchema()

        # Validate creative asset
        context.set_custom_status("Validating Creative Asset")
        yield context.call_activity(
            "roku_async_activity_validate_creative", context.instance_id
        )

        # Get the current state of the instance
        entity_id = EntityId("roku_async_entity_request", context.instance_id)
        state = yield context.call_entity(entity_id, "get")
        state = schema.loads(state)

        # For each of the tasks "advertiser", "creative", "campaign", if the task
        # does not exist in the current state, wait for an event signaling its
        # registration, and then update the state
        for task in ["advertiser", "creative"]:
            if not state["existing"][task]:
                context.set_custom_status(f"Awaiting: {task.capitalize()} Registration")
                event = yield context.wait_for_external_event(task)
                state = yield context.call_entity(entity_id, task, event)
                state = schema.loads(state)

        # If there are missing months, wait for flight registration
        while campaigns := calculate_missing_campaigns(state):
            context.set_custom_status(f"Awaiting: {len(campaigns)} Campaign Registrations")
            event = yield context.wait_for_external_event("campaign")
            state = yield context.call_entity(entity_id, "campaign", event)
            state = schema.loads(state)

        # If there are missing months, wait for flight registration
        while flights := calculate_missing_flights(state):
            context.set_custom_status(f"Awaiting: {len(flights)} Flight Registrations")
            event = yield context.wait_for_external_event("flight")
            state = yield context.call_entity(entity_id, "flight", event)
            state = schema.loads(state)

        return schema.dumps(state)

    except Exception as e:
        import traceback

        exc_dict = {
            "type": type(e).__name__,
            "message": str(e),
            "traceback": [
                {"file": s.filename, "line": s.lineno}
                for s in traceback.extract_stack()
            ],
        }
        state = yield context.call_entity(entity_id, "error", exc_dict)
        yield context.call_activity(
            "roku_async_activity_push_notification", schema.dump(schema.loads(state))
        )
        raise e
