# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/eternal.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from croniter import croniter
import datetime, orjson as json, logging

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquire_audience(context: DurableOrchestrationContext):
    """
    Orchestrator function to schedule and run audience builds based on a cron expression.

    - Fetches the audience details.
    - Determines the next run time based on the cron schedule.
    - Triggers the audience build sub-orchestrator if it's time to run.
    - Pushes the most recently generated audiences to the DSPs.
    - Sets a timer for the next scheduled run and continues as new.

    Parameters:
    context (DurableOrchestrationContext): The context for the orchestrator function.
    """
    # Define the configuration for working and destination connections
    ingress = context.get_input()

    # Fetch the full details for the audience
    ingress["audience"] = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudience",
        {"id": context.instance_id},
    )

    # Fetch the last run time from the most recent audience blob prefix
    audience_blob_prefix = yield context.call_activity(
        "activity_esquireAudiencesUtils_newestAudienceBlobPrefix",
        {
            "conn_str": ingress["destination"]["conn_str"],
            "container_name": ingress["destination"]["container_name"],
            "audience_id": ingress["audience"]["id"],
        },
    )
    last_run_time = (
        datetime.datetime.fromisoformat(audience_blob_prefix.split("/")[-1])
        if audience_blob_prefix
        else datetime.datetime.min
    )

    # Calculate the next run time based on the cron expression
    next_run = croniter(ingress["audience"]["rebuildSchedule"], last_run_time).get_next(
        datetime.datetime
    )

    # Check if it's time to run the audience build
    if (
        context.current_utc_datetime >= next_run and ingress["audience"]["status"]
    ) or ingress["forceRebuild"]:
        # Generate the audience data
        build = yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_builder", ingress
        )
        # Push the most recently generated audiences to the DSPs that are configured
        yield context.call_sub_orchestrator(
            "orchestrator_esquireAudiences_uploader",
            build,
        )

    # Set a timer for the next scheduled run
    next_timer: datetime.datetime = croniter(
        ingress["audience"]["rebuildSchedule"], context.current_utc_datetime
    ).get_next(datetime.datetime)
    context.set_custom_status("Next run: {}".format(next_timer.isoformat()))

    # Wait for either the timer or an external event
    timer_task = context.create_timer(next_timer)
    external_event_task = context.wait_for_external_event("restart")

    # Wait for the first of the tasks to complete
    winner = yield context.task_any([timer_task, external_event_task])
    if winner == external_event_task:
        timer_task.cancel()
        settings = json.loads(winner.result)
    else:
        settings: dict = context.get_input()
        settings.pop("forceRebuild")

    ## Purge sub-instances history
    yield context.call_sub_orchestrator(
        "purge_instance_history", {"instance_id": context.instance_id, "self": False}
    )

    if not context.is_replaying:
        context.continue_as_new(settings)
    return ""
