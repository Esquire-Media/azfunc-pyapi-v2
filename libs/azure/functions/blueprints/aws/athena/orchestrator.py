# File: libs/azure/functions/blueprints/aws/athena/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_athena(context: DurableOrchestrationContext):
    ingress = context.get_input()
    execution_id = yield context.call_activity("activity_athena_execute", ingress)

    url = ""
    while url == "":
        url = yield context.call_activity(
            "activity_athena_monitor",
            {
                **ingress,
                "execution_id": execution_id,
            },
        )
        if url == "":
            yield context.create_timer(datetime.utcnow() + timedelta(seconds=5))

    if ingress.get("destination", False):
        url = yield context.call_activity(
            "activity_athena_download",
            {
                **ingress["destination"],
                "url": url,
            },
        )

    return url
