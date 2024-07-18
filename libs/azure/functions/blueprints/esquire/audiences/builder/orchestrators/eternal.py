# File: /libs/azure/functions/blueprints/esquire/audiences/builder/orchestrators/eternal.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from croniter import croniter
import datetime, orjson as json, pytz

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_esquire_audience(context: DurableOrchestrationContext):
    """
    Orchestrates the audience building and uploading process based on a cron schedule.

    - Fetches the audience details using the audience ID from the context instance ID.
    - Determines the last run time by retrieving the most recent audience blob prefix.
    - Calculates the next scheduled run time based on the provided cron expression.
    - Checks if it is time to rebuild the audience based on the current UTC time and audience status.
    - If it's time to rebuild or a force rebuild is requested:
        - Calls the sub-orchestrator to build the audience.
        - Calls the sub-orchestrator to upload the generated audience data to the configured DSPs.
    - Schedules the next run based on the cron expression.
    - Sets up a timer to wait for the next scheduled run or an external event to restart.
    - Handles the completion of either the timer or the external event, and continues as new with updated settings.
    - Purges the history of sub-instances related to this orchestrator instance.

    Args:
        context (DurableOrchestrationContext): The context for the orchestration, providing access
                                               to orchestration features and data.

    Returns:
        str: An empty string indicating the completion of the orchestration.
    """
    # Retrieve the initial configuration for working and destination connections
    ingress = context.get_input()

    # Fetch the full details for the audience using its ID (from context instance ID)
    ingress["audience"] = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudience",
        {"id": context.instance_id},
    )

    # Fetch the last run time by getting the most recent audience blob prefix from storage
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
        else datetime.datetime(year=1970, month=1, day=1)
    )
    # Ensure last_run_time is timezone-aware
    if last_run_time.tzinfo is None:
        last_run_time = last_run_time.replace(tzinfo=pytz.UTC)

    # Calculate the next scheduled run time based on the cron expression and last run time
    next_run = (
        croniter(ingress["audience"]["rebuildSchedule"], last_run_time)
        .get_next(datetime.datetime)
        .replace(tzinfo=pytz.UTC)
    )

    # Ensure context.current_utc_datetime is timezone-aware
    current_utc_datetime = context.current_utc_datetime
    if current_utc_datetime.tzinfo is None:
        current_utc_datetime = current_utc_datetime.replace(tzinfo=pytz.UTC)

    # Check if the current time is past the next scheduled run time and if the audience status is active,
    # or if a force rebuild has been requested
    if (
        context.current_utc_datetime >= next_run and ingress["audience"]["status"]
    ) or ingress.get("forceRebuild"):
        try:
            # Generate the audience data by calling the audience builder orchestrator
            build = yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiences_builder",
                ingress,
            )
            # Upload the newly generated audience data to the configured DSPs
            yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiences_uploader",
                build,
            )
        except Exception as e:
            # if any errors are caught, post an error card to teams tagging Ryan
            yield context.call_activity(
                "activity_microsoftGraph_postErrorCard",
                {
                    "instance_id": context.instance_id,
                    "error": f"{type(e).__name__} : {e}"[:1000],
                },
            )

    # Calculate the next timer for the next scheduled run
    context.set_custom_status(
        {
            "next_run": croniter(
                ingress["audience"]["rebuildSchedule"],
                context.current_utc_datetime,
            )
            .get_next(datetime.datetime)
            .isoformat(),
            "context": ingress
        }
    )

    # Purge the history of sub-instances related to this orchestrator
    yield context.call_sub_orchestrator(
        "purge_instance_history", {"instance_id": context.instance_id, "self": False}
    )

    # Schedule a trigger to rerun the orchestrator (>6 days)
    timer_task = context.create_timer(
        croniter("0 0 * * *", context.current_utc_datetime).get_next(datetime.datetime)
    )
    # Create an optional external event to manually restart
    external_event_task = context.wait_for_external_event("restart")

    # Wait for the first of the tasks to complete
    winner = yield context.task_any([timer_task, external_event_task])
    if winner == external_event_task:
        # If the external event is received, cancel the timer and get the settings from the event
        timer_task.cancel()
        settings = json.loads(winner.result)
    else:
        # If the timer completes first, use the current settings
        settings: dict = context.get_input()
        settings.pop("forceRebuild", None)

    # Restart the orchestrator
    context.continue_as_new(settings)
    return ""
