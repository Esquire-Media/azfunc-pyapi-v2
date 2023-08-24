# File: libs/azure/functions/blueprints/orchestrators/tasks.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions.blueprints.oneview.tasks.helpers import (
    calculate_missing_campaigns,
    calculate_missing_flights,
    state as OrchestartorState,
    process_state as OrchestartorStateOperation,
)
from libs.azure.functions.blueprints.oneview.tasks.schemas import RequestSchema
from libs.azure.functions import Blueprint

bp = Blueprint()


# Define the orchestrator function
@bp.orchestration_trigger(context_name="context")
def oneview_orchestrator_tasks(context: DurableOrchestrationContext):
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
        # Validate creative asset
        # context.set_custom_status("Validating Creative Asset")
        # yield context.call_activity(
        #     "oneview_activity_validate_creative", context.instance_id
        # )

        # Get the current state of the instance
        schema = RequestSchema()
        state = schema.load(OrchestartorState(context.instance_id))

        # For each of the tasks "advertiser", "creative", "campaign", if the task
        # does not exist in the current state, wait for an event signaling its
        # registration, and then update the state
        for task in ["advertiser", "creative"]:
            while not state["existing"][task]:
                context.set_custom_status(f"Awaiting: {task.capitalize()} Registration")
                event = yield context.wait_for_external_event(task)
                state = schema.load(
                    OrchestartorStateOperation(context.instance_id, task, event)
                )

        # If there are missing months, wait for flight registration
        while campaigns := calculate_missing_campaigns(state):
            context.set_custom_status(
                f"Awaiting: {len(campaigns)} Campaign Registrations"
            )
            event = yield context.wait_for_external_event("campaign")
            state = schema.load(
                OrchestartorStateOperation(context.instance_id, "campaign", event)
            )

        # If there are missing months, wait for flight registration
        while flights := calculate_missing_flights(state):
            context.set_custom_status(f"Awaiting: {len(flights)} Flight Registrations")
            event = yield context.wait_for_external_event("flight")
            state = schema.load(
                OrchestartorStateOperation(context.instance_id, "flight", event)
            )

        return schema.dump(state)

    except Exception as e:
        import traceback

        exc_dict = {
            "type": type(e).__name__,
            "message": str(e),
            "traceback": [
                {"file": s.filename, "line": s.lineno}
                for s in traceback.extract_tb(e.__traceback__)
            ],
        }
        state = schema.load(
            OrchestartorStateOperation(context.instance_id, "error", exc_dict)
        )
        yield context.call_activity(
            "oneview_activity_push_notification", schema.dumps(state)
        )
        raise e
