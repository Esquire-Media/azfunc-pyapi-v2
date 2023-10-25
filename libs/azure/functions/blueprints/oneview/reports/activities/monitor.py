# File: libs/azure/functions/blueprints/oneview/reports/activities/monitor.py

from datetime import date, datetime
from libs.azure.functions import Blueprint
from libs.openapi.clients.oneview import OneView


bp = Blueprint()


@bp.activity_trigger(input_name="uid")
def oneview_reports_activity_monitor(uid: dict):
    _, data, _ = (
        OneView()
        ._["getReports"]
        .request(
            parameters={
                "filter": "report_template_uid:"+uid,
                "order": "-created_at",
                "limit": 1,
            }
        )
    )
    if len(data.reports):
        report = data.reports[0]
        t = date.today()
        if (
            report.end_at.replace(tzinfo=None)
            == datetime(t.year, t.month, t.day, 3, 59, 59)
            and report.status == "done"
        ):
            return report.download_url
    return ""
