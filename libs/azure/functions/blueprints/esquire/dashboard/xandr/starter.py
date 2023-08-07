# File: libs/azure/functions/blueprints/esquire/dashboard/xandr/starter.py

from azure.durable_functions import DurableOrchestrationClient
from azure.functions import TimerRequest
from libs.azure.functions import Blueprint
from libs.openapi.clients.xandr import XandrAPI
import logging

# Create a Blueprint instance
bp = Blueprint()


@bp.timer_trigger("timer", schedule="0 0 */4 * * *")
@bp.durable_client_input("client")
async def daily_dashboard_xandr_starter(
    timer: TimerRequest, client: DurableOrchestrationClient
):
    XA = XandrAPI()
    report_factory = XA.createRequest(("/report", "post"))
    for request in [
        {
            "report": {
                "timezone": "America/New_York",
                "report_type": "network_site_domain_performance",
                "report_interval": "last_30_days",
                "columns": [
                    "day",
                    "advertiser_id",
                    "advertiser_name",
                    "line_item_id",
                    "line_item_name",
                    "site_domain",
                    "mobile_application_id",
                    "mobile_application_name",
                    "imps",
                    "clicks",
                ],
                "format": "csv",
            }
        },
        {
            "report": {
                "timezone": "America/New_York",
                "report_type": "network_analytics",
                "report_interval": "last_30_days",
                "columns": [
                    "day",
                    "advertiser_currency",
                    "advertiser_id",
                    "advertiser_name",
                    "campaign_id",
                    "campaign_name",
                    "creative_id",
                    "creative_name",
                    "insertion_order_id",
                    "insertion_order_name",
                    "clicks",
                    "imps",
                    "line_item_id",
                    "line_item_name",
                    "cost",
                    "revenue",
                ],
                "format": "csv",
            }
        },
    ]:
        _, data, _ = await report_factory(data=request)
        await client.start_new(
            "esquire_dashboard_xandr_orchestrator_reporting",
            data.response.report_id,
            request,
        )
        logging.warn(client.create_http_management_payload(data.response.report_id))
