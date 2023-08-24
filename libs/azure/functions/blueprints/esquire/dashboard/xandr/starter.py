# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/starter.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from libs.azure.functions import Blueprint
from libs.openapi.clients.xandr import XandrAPI
import logging 
# Create a Blueprint instance
bp = Blueprint()


@bp.timer_trigger("timer", schedule="0 0 0 * * *")
@bp.durable_client_input("client")
async def daily_dashboard_xandr_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    XA = XandrAPI()
    generate_report = XA.createRequest("GenerateReport")
    for request in [
        {
            "report": {
                "timezone": "America/New_York",
                "report_type": "network_analytics",
                "report_interval": "last_30_days",
                "columns": [
                    "day",
                    "advertiser_id",
                    "advertiser_name",
                    "insertion_order_id",
                    "insertion_order_name",
                    "line_item_id",
                    "line_item_name",
                    "clicks",
                    "imps",
                    "cost",
                    "revenue",
                ],
                "format": "csv",
            }
        },
        {
            "report": {
                "timezone": "America/New_York",
                "report_type": "network_site_domain_performance",
                "report_interval": "last_30_days",
                "columns": [
                    "day",
                    "line_item_id",
                    "site_domain",
                    "mobile_application_id",
                    "mobile_application_name",
                    "supply_type",
                    "imps",
                    "clicks",
                ],
                "format": "csv",
            }
        },
        {
            "report": {
                "timezone": "America/New_York",
                "report_type": "buyer_approximate_unique_users_hourly",
                "report_interval": "last_30_days",
                "columns": [
                    "day",
                    "line_item_id",
                    "identified_imps",
                    "unidentified_imps",
                    "approx_users_count",
                    "estimated_people_reach",
                ],
                "format": "csv",
            }
        },
    ]:
        _, data, _ = await generate_report.request(data=request)
        instance_id = await client.start_new(
            "esquire_dashboard_xandr_orchestrator_reporting",
            data.response.report_id,
        )
        logging.warn(client.create_http_management_payload(instance_id))
