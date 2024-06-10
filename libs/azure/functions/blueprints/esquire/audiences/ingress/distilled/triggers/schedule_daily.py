# File: libs/azure/functions/blueprints/esquire/audiences/distilled/triggers/schedule_daily.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.timer_trigger(arg_name="timer", schedule="0 0 0 * * *")
@bp.durable_client_input(client_name="client")
async def esquire_audiences_distilled_schedule_daily(
    timer: TimerRequest, client: DurableOrchestrationClient
) -> None:
    """
    Scheduled function to start the OneView segment updater orchestrator daily.

    This function is triggered daily and starts an instance of the
    `esquire_audiences_oneview_segment_updater` orchestrator for each record
    fetched from the NocoDB.

    Parameters
    ----------
    timer : TimerRequest
        Azure timer request that triggered this function.
    client : DurableOrchestrationClient
        Azure Durable Functions client to start new orchestrator instances.

    Returns
    -------
    None

    Notes
    -----
    The function depends on the NocoDB client to fetch records and
    the Azure Durable Functions client to start orchestrators.
    """
    await client.start_new(
        "esquire_audiences_distilled_orchestrator_updater",
        None,
        {
            "source": {
                "access_key": "DISTILLED_ACCESS_KEY",
                "secret_key": "DISTILLED_SECRET_KEY",
                "bucket": "DISTILLED_BUCKET",
                "region": "DISTILLED_REGION",
                "database": "DISTILLED_DATABASE",
                "workgroup": "DISTILLED_WORKGROUP",
            },
            "destination": {
                "conn_str": "META_CONN_STR",
                "container_name": "general"
            }
        },
    )
