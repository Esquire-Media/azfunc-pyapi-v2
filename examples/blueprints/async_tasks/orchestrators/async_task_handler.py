from azure.data.tables import TableServiceClient
from azure.durable_functions import DurableOrchestrationContext
from libs.azure.functions import Blueprint
import logging
import os

TABLE_SERVICE = TableServiceClient.from_connection_string(
    conn_str=os.environ["AzureWebJobsStorage"]
)
TABLE_CLIENTS = {
    "advertisers": TABLE_SERVICE.get_table_client("advertisers"),
    "campaigns": TABLE_SERVICE.get_table_client("campaigns"),
    "creatives": TABLE_SERVICE.get_table_client("creatives"),
    "flights": TABLE_SERVICE.get_table_client("flights"),
}

bp = Blueprint()


# Orchestrator
@bp.orchestration_trigger(context_name="context")
def async_task_handler(context: DurableOrchestrationContext):
    context.set_custom_status("Validating Request Data")
    request = yield context.call_activity("validate_request", context.get_input())
    
    context.set_custom_status("Validating Creative Asset")
    creative_md5 = yield context.call_activity("validate_creative", request["creative"])

    try:
        advertiser = TABLE_CLIENTS["advertisers"].get_entity(
            partition_key=request["advertiser"]["tenant"],
            row_key=request["advertiser"]["id"],
        )
    except:
        context.set_custom_status("Awaiting: New Advertiser Registration")
        advertiser_event = yield context.wait_for_external_event("oneview_advertiser_id")
        
        advertiser = TABLE_CLIENTS["advertisers"].create_entity({
            "PartitionKey": request["advertiser"]["tenant"],
            "RowKey": request["advertiser"]["id"],
            "OrganizationName": request["advertiser"]["name"],
            "Domain": request["advertiser"]["domain"],
            "ContentCategory": request["advertiser"]["category"],
            "OneView_AdvertiserID": advertiser_event["advertiser_id"],
            "OneView_OrgID": advertiser_event["organization_id"],
        })
        
    try:
        creative = TABLE_CLIENTS["creatives"].get_entity(
            partition_key=advertiser["RowKey"],
            row_key=creative_md5,
        )
    except:
        context.set_custom_status("Awaiting: Creative Registration")
        creative_event = yield context.wait_for_external_event("oneview_creative_id")
        creative = TABLE_CLIENTS["creatives"].create_entity({
            "PartitionKey": advertiser["RowKey"],
            "RowKey": creative_md5,
            "OneView_CreativeID": creative_event["creative_id"]
        })
        
    context.set_custom_status("Awaiting: Campaign Registration")
    campaign_event = yield context.wait_for_external_event("oneview_campaign_id")
    campaign = TABLE_CLIENTS["campaigns"].create_entity({
        "PartitionKey": advertiser["RowKey"],
        "RowKey": campaign_event["campaign_id"]
    })
    
    
    context.set_custom_status("Awaiting: Flight Registration")
    flight_event = yield context.wait_for_external_event("oneview_flight_id")
    flight = TABLE_CLIENTS["flights"].create_entity({
        "PartitionKey": campaign["RowKey"],
        "RowKey": flight_event["flight_id"],
    })
