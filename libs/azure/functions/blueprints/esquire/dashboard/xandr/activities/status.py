# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/activities/status.py

from libs.azure.functions import Blueprint
from libs.openapi.clients.xandr import XandrAPI
import json

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def esquire_dashboard_xandr_activity_status(ingress: dict):
    XA = XandrAPI(asynchronus=False)
    _, status, _ = XA.createRequest("GetReportStatus").request(
        parameters={"id": ingress["instance_id"]}
    )

    return {
        "status": status.response.execution_status,
        "report_type": (
            json.loads(status.response.report.json_request)["report"]["report_type"]
            if status.response.report
            else None
        ),
        "error": status.response.error if hasattr(status.response, "error") else None,
    }
