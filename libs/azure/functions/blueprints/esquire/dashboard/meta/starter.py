# File: libs/azure/functions/blueprints/esquire/dashboard/meta/starter.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from azure.durable_functions import Blueprint

# Create a Blueprint instance
bp = Blueprint()


@bp.timer_trigger("timer", schedule="0 0 5 * * *")
@bp.durable_client_input("client")
async def daily_dashboard_meta_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    await client.start_new(
        "esquire_dashboard_meta_orchestrator_report_batch", None, None
    )
