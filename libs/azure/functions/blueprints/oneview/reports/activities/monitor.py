# File: libs/azure/functions/blueprints/oneview/reports/activities/monitor.py

from libs.azure.functions import Blueprint
from libs.openapi.clients.oneview import OneView

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_reports_activity_monitor(ingress: dict):
    data = OneView["getReports"](
        parameters={
            "filter": '((status:"running") AND (report_template_uid:'
            + ingress["report_template_uid"]
            + "))"
        }
    )
    if not data.reports:
        data = OneView["getReports"](
            parameters={
                "filter": '((status:"done") AND (report_template_uid:'
                + ingress["report_template_uid"]
                + "))",
                "order": "-created_at",
                "limit": 1,
            }
        )
        if len(data.reports):
            return data.reports[0].download_url
    return ""
