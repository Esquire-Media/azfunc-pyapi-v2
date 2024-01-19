# File: libs/azure/functions/blueprints/oneview/reports/activities/run.py

from libs.azure.functions import Blueprint
from libs.openapi.clients.oneview import OneView


bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def activity_oneviewReports_run(ingress: str):
    OneView["postReportTemplateRun"](
        parameters={"report_template_uid": ingress["report_template_uid"]}
    )
    return ""
