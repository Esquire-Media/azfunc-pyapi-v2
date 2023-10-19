# File: libs/azure/functions/blueprints/oneview/segments/triggers/schedule_daily.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from libs.azure.functions import Blueprint
from libs.openapi.clients.nocodb import NocoDB
import os

bp = Blueprint()


@bp.timer_trigger(arg_name="timer", schedule="0 0 0 * * *")
@bp.durable_client_input(client_name="client")
async def oneview_segments_schedule_daily(
    timer: TimerRequest, client: DurableOrchestrationClient
) -> None:
    """
    Scheduled function to start the OneView segment updater orchestrator daily.

    This function is triggered daily and starts an instance of the
    `oneview_orchestrator_segment_updater` orchestrator for each record
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

    # Initialize the NocoDB API client with necessary credentials
    api = NocoDB(
        host=os.getenv("NOCODB_HOST"),
        project_id="pifplkhdufgnyxl",
        api_token=os.getenv("NOCODB_API_KEY"),
    )

    # Start an orchestrator instance for each record
    for record in [None] + api.createRequest(("/OneView Segments", "get"))(
        parameters={"limit": 1000}
    ).list:
        # Start the `oneview_orchestrator_segment_updater` orchestrator
        # If the record is not None, dump its model data as input
        await client.start_new(
            "oneview_orchestrator_segment_updater",
            None,
            record.model_dump() if record != None else None,
        )
