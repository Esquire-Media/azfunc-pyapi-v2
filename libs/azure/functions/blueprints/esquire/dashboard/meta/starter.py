# File: libs/azure/functions/blueprints/esquire/dashboard/meta/starter.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from libs.azure.functions import Blueprint
import logging

# Create a Blueprint instance
bp = Blueprint()


@bp.timer_trigger("timer", schedule="0 0 0 * * *")
@bp.durable_client_input("client")
async def daily_dashboard_meta_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    instance_id = await client.start_new(
        "esquire_dashboard_meta_orchestrator_report_batch", None, None
    )
    logging.warn(client.create_http_management_payload(instance_id))
