# File: libs/azure/functions/blueprints/esquire/dashboard/unmasked/starter.py

from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.functions import TimerRequest

# Create a Blueprint instance
bp = Blueprint()


@bp.timer_trigger("timer", schedule="0 0 */4 * * *")
@bp.durable_client_input("client")
async def daily_dashboard_unmasked_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    await client.start_new(
        orchestration_function_name="esquire_dashboard_unmasked_orchestrator",
        client_input={
            "access_key": "DISTILLED_ACCESS_KEY",
            "secret_key": "DISTILLED_SECRET_KEY",
            "bucket": "DISTILLED_BUCKET",
            "region": "DISTILLED_REGION",
            "database": "DISTILLED_DATABASE",
            "workgroup": "DISTILLED_WORKGROUP",
            "runtime_container":{
                "conn_str": "AzureWebJobsStorage",
                "container_name": "general",
            }
        },
    )
