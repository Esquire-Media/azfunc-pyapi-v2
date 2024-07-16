# File: libs/azure/functions/blueprints/aws/athena/orchestrator.py

from azure.durable_functions import Blueprint, DurableOrchestrationContext
from datetime import datetime, timedelta

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def aws_athena_orchestrator(context: DurableOrchestrationContext):
    ingress = context.get_input()
    execution_id = yield context.call_activity("aws_athena_activity_execute", ingress)

    url = ""
    while url == "":
        url = yield context.call_activity(
            "aws_athena_activity_monitor",
            {
                **ingress,
                "execution_id": execution_id,
            },
        )
        if url == "":
            yield context.create_timer(datetime.utcnow() + timedelta(seconds=5))

    if ingress.get("destination", False):
        url = yield context.call_activity(
            "aws_athena_activity_download",
            {
                **ingress["destination"],
                "url": url,
            },
        )

    return url
