# File: libs/azure/functions/blueprints/onspot/orchestrator.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from urllib.parse import urlparse

bp = Blueprint()

import logging

@bp.orchestration_trigger(context_name="context")
def onspot_orchestrator(context: DurableOrchestrationContext):
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
        The result of calling the "onspot_activity_format" and
        "onspop_activity_submit" activities.

    Returns
    -------
    dict
        A dictionary with "jobs" that contains the result of the "onspot_activity_submit"
        call and "callbacks" that contains the result of waiting for all the callbacks.

    Example
    -------
    >>> await client.start_new(
    >>>     "onspot_orchestrator",
    >>>     None,
    >>>     {
    >>>         "endpoint": "/save/geoframe/all/devices",
    >>>         "request": req.get_json(),
    >>>     },
    >>> )
    OR as a sub-orchestrator
    >>> results = yield context.call_sub_orchestrator(
    >>>     "onspot_orchestrator",
    >>>     {
    >>>         "endpoint": "/save/geoframe/all/devices",
    >>>         "request": {...}
    >>>     }
    >>> )
    """

    # Format the request
    logging.info("[LOG] Starting format Activity")
    request = yield context.call_activity(
        name="onspot_activity_format",
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
    logging.info("[LOG] Starting submit activity")
    jobs = yield context.call_activity(
        name="onspot_activity_submit",
        input_={
            "endpoint": context.get_input()["endpoint"],
            "request": request,
        },
    )

    # Wait for all of the callbacks
    logging.info("[LOG] Waiting for callbacks.")
    callbacks = yield context.task_all(events)
    logging.info("[LOG] All callbacks received")

    return {"jobs": jobs if isinstance(jobs, list) else [jobs], "callbacks": callbacks}
