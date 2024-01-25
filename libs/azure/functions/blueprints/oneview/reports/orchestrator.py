# File: libs/azure/functions/blueprints/oneview/reports/orchestrator.py

from azure.durable_functions import DurableOrchestrationContext
from datetime import datetime, timedelta
from libs.azure.functions import Blueprint

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def oneview_reports_orchestrator(
    context: DurableOrchestrationContext,
):
    # Expected keys:
    # report_template_uid
    ingress = context.get_input()

    # Submit report generation request
    yield context.call_activity(
        "oneview_reports_activity_run",
        ingress,
    )

    # Periodically check to see if it's done and get the download url when it is
    download_url = ""
    while not download_url:
        yield context.create_timer(datetime.utcnow() + timedelta(minutes=5))
        download_url = yield context.call_activity(
            "oneview_reports_activity_monitor",
            ingress,
        )

    return download_url
