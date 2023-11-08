# File: libs/azure/functions/blueprints/aws/athena/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def aws_athena_orchestrator(context: DurableOrchestrationContext):
    settings = context.get_input()
    execution_id = yield context.call_activity(
        "aws_athena_activity_execute",
        settings
    )
    
    url = ""
    while url == "":
        url = yield context.call_activity(
            "aws_athena_activity_monitor",
            {
                **settings,
                "execution_id": execution_id
            }
        )
        if url == "":
            yield context.create_timer(datetime.utcnow() + timedelta(seconds=30))
        
    return url