# File: libs/azure/functions/blueprints/oneview/reports/activities/monitor.py

from libs.azure.functions import Blueprint
from libs.openapi.clients.oneview import OneView
from pydantic_core._pydantic_core import TzInfo
import datetime, pytz

bp = Blueprint()


@bp.activity_trigger(input_name="ingress")
def oneview_reports_activity_monitor(ingress: dict):
    data = OneView["getReports"](
        parameters={
            "filter": "report_template_uid:" + ingress["report_template_uid"],
            "order": "-created_at",
            "limit": 1,
        }
    )
    today = datetime.datetime(
        ingress["end_at"][0],
        ingress["end_at"][1],
        ingress["end_at"][2],
        4,
        59,
        59,
        tzinfo=TzInfo(0),
    )
    if len(data.reports):
        report = data.reports[0]
        if (
            report.end_at == (today + datetime.timedelta(hours=dst(today)))
            and report.status == "done"
        ):
            return report.download_url
    return ""


def dst(date_obj):
    """
    Check if a given date is during Daylight Saving Time in the U.S.

    :param date_obj: datetime.date object
    :return: -1 if the date is during DST, otherwise 0
    """
    # Assuming the date is for the United States, you'll need to specify the timezone.
    # For example, using New York for Eastern Time:
    timezone = pytz.timezone("America/New_York")

    # Convert the date object to a datetime object with the time as midnight
    datetime_obj = datetime.datetime.combine(date_obj, datetime.time.min)

    # Localize the datetime object to the timezone
    localized_datetime = timezone.localize(datetime_obj, is_dst=None)

    # Check if the date is DST
    if localized_datetime.dst() != datetime.timedelta(0):
        return -1
    else:
        return 0
