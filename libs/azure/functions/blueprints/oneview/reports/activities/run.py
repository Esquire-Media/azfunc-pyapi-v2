# File: libs/azure/functions/blueprints/oneview/reports/activities/run.py

from libs.azure.functions import Blueprint
from libs.openapi.clients.oneview import OneView
import os


bp = Blueprint()


@bp.activity_trigger(input_name="uid")
def oneview_reports_activity_run(uid: str):
    OneView()._["postReportTemplateRun"].request(
        parameters={"report_template_uid": uid}
    )
    return ""
