# File: libs/azure/functions/blueprints/oneview/reports/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint
import os

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def orchestrator_oneviewReports(
    context: DurableOrchestrationContext,
):
    # Expected keys:
    # report_template_uid
    ingress = context.get_input()

    # Submit report generation request
    yield context.call_activity(
        "activity_oneviewReports_run",
        ingress,
    )

    # Periodically check to see if it's done and get the download url when it is
    download_url = ""
    while not download_url:
        download_url = yield context.call_activity(
            "activity_oneviewReports_monitor",
            ingress,
        )
        yield context.create_timer(datetime.utcnow() + timedelta(minutes=5))

    return download_url
