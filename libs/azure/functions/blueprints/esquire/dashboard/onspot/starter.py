# File: libs/azure/functions/blueprints/esquire/dashboard/onspot/starter.py

from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import TimerRequest
import logging

# Create a Blueprint instance
bp = Blueprint()


@bp.timer_trigger("timer", schedule="0 0 8 * * *")
@bp.durable_client_input("client")
async def daily_dashboard_onspot_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    instance_id = await client.start_new("esquire_dashboard_onspot_orchestrator")
    logging.warn(client.create_http_management_payload(instance_id))
