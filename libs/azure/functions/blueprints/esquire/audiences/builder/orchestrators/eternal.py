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
    ingress = context.get_input()

    # Fetch audience
    ingress["audience"] = yield context.call_activity(
        "activity_esquireAudienceBuilder_fetchAudience",
        {"id": context.instance_id},
    )

    # Last run prefix
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
    if last_run_time.tzinfo is None:
        last_run_time = last_run_time.replace(tzinfo=pytz.UTC)

    next_run = (
        croniter(ingress["audience"]["rebuildSchedule"], last_run_time)
        .get_next(datetime.datetime)
        .replace(tzinfo=pytz.UTC)
    )

    current_utc_datetime = context.current_utc_datetime
    if current_utc_datetime.tzinfo is None:
        current_utc_datetime = current_utc_datetime.replace(tzinfo=pytz.UTC)

    # Build + upload when due (or forced)
    if (
        context.current_utc_datetime >= next_run and ingress["audience"]["status"]
    ) or ingress.get("forceRebuild"):
        try:
            context.set_custom_status({"state": "Building audience..."})
            build = yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiences_builder",
                ingress,
            )
            context.set_custom_status({"state": "Uploading audience..."})
            # NOTE: Call the versioned, deterministic uploader
            yield context.call_sub_orchestrator(
                "orchestrator_esquireAudiences_uploader",
                build,
            )
        except Exception as e:
            # Optionally notify; left as-is from previous code
            yield context.call_activity(
                "activity_microsoftGraph_sendEmail",
                {
                    "from_id": "74891a5a-d0e9-43a4-a7c1-a9c04f6483c8",
                    "to_addresses": ["isaac@esquireadvertising.com"],
                    "subject": "esquire-auto-audience Failure",
                    "message": e,
                    "content_type": "html",
                },
            )
            raise e

    # Announce next run
    context.set_custom_status(
        {
            "next_run": croniter(
                ingress["audience"]["rebuildSchedule"],
                context.current_utc_datetime,
            )
            .get_next(datetime.datetime)
            .isoformat(),
            "context": ingress,
        }
    )

    # Sleep until tomorrow or manual restart
    timer_task = context.create_timer(
        croniter("0 0 * * *", context.current_utc_datetime).get_next(datetime.datetime)
    )
    external_event_task = context.wait_for_external_event("restart")
    winner = yield context.task_any([timer_task, external_event_task])
    if winner == external_event_task:
        timer_task.cancel()
        settings = json.loads(winner.result)
    else:
        settings: dict = context.get_input()
        settings.pop("forceRebuild", None)

    context.continue_as_new(settings)
    return ""