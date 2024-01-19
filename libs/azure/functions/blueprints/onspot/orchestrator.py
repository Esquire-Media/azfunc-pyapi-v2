# File: libs/azure/functions/blueprints/onspot/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
from urllib.parse import urlparse

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_onspot(context: DurableOrchestrationContext):
    """
    Orchestrates the handling of a request in an Azure Durable Function.

    This function formats the request, prepares for callbacks using external
    events, submits the request, and then waits for all the callbacks.

    Parameters
    ----------
    context : DurableOrchestrationContext
        The context for the durable orchestration.

    Yields
    ------
    dict
        The result of calling the "activity_onspot_format" and
        "onspop_activity_submit" activities.

    Returns
    -------
    dict
        A dictionary with "jobs" that contains the result of the "activity_onspot_submit"
        call and "callbacks" that contains the result of waiting for all the callbacks.

    Example
    -------
    >>> await client.start_new(
    >>>     "orchestrator_onspot",
    >>>     None,
    >>>     {
    >>>         "endpoint": "/save/geoframe/all/devices",
    >>>         "request": req.get_json(),
    >>>     },
    >>> )
    OR as a sub-orchestrator
    >>> results = yield context.call_sub_orchestrator(
    >>>     "orchestrator_onspot",
    >>>     {
    >>>         "endpoint": "/save/geoframe/all/devices",
    >>>         "request": {...}
    >>>     }
    >>> )
    """

    # Format the request
    request = yield context.call_activity(
        name="activity_onspot_format",
        input_={
            "instance_id": context.instance_id,
            **context.get_input(),
        },
    )

    # Prepare for callbacks using external events
    if request.get("type", None) == "FeatureCollection":
        events = [
            context.wait_for_external_event(
                urlparse(f["properties"]["callback"]).path.split("/")[-1]
            )
            for f in request["features"]
        ]
    elif isinstance(request.get("sources"), list):
        events = [
            context.wait_for_external_event(
                urlparse(request["callback"]).path.split("/")[-1]
            )
        ]

    # Submit request
    jobs = yield context.call_activity(
        name="activity_onspot_submit",
        input_={
            "endpoint": context.get_input()["endpoint"],
            "request": request,
        },
    )

    # Wait for all of the callbacks
    callbacks = yield context.task_all(events)

    return {"jobs": jobs if isinstance(jobs, list) else [jobs], "callbacks": callbacks}
