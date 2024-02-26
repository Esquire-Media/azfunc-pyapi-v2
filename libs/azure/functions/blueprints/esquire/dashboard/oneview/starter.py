# File: libs/azure/functions/blueprints/esquire/dashboard/oneview/starter.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from libs.azure.functions import Blueprint
import os

# Create a Blueprint instance
bp = Blueprint()


@bp.timer_trigger("timer", schedule="0 0 6 * * *")
@bp.durable_client_input("client")
async def daily_dashboard_oneview_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    await client.start_new(
        "esquire_dashboard_oneview_orchestrator",
        client_input={
            "report_template_uid": os.environ["ONEVIEW_REPORT_TEMPLATE_UID"],
        },
    )
