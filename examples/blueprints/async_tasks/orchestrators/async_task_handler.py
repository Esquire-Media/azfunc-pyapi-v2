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
    creative_md5 = yield context.call_activity("validate_creative", request["creative"])

    try:
        advertiser = TABLE_CLIENTS["advertisers"].get_entity(
            partition_key=request["advertiser"]["tenant"],
            row_key=request["advertiser"]["id"],
        )
    except:
        context.set_custom_status("Awaiting: New Advertiser Registration")
        advertiser = yield context.wait_for_external_event("oneview_advertiser_id")
        
    events = []
    try:
        campaign = TABLE_CLIENTS["campaigns"].get_entity(
            partition_key=advertiser["RowKey"],
            row_key=context.instance_id,
        )
    except:
        events.append["oneview_campaign_id"]
    try:
        creative = TABLE_CLIENTS["creatives"].get_entity(
            partition_key=advertiser["RowKey"],
            row_key=creative_md5,
        )
    except:
        events.append["oneview_campaign_id"]
        
    events = ['oneview_campaign_id', 'oneview_creative_id', 'event3']  # Replace with your actual event names
    event_tasks = [context.wait_for_external_event(event) for event in events]
    
    while event_tasks:
        done_task = yield context.task_any(event_tasks)
        completed_event = events[event_tasks.index(done_task)]
        event_tasks.remove(done_task)
        
        # When each event completes, update an Azure Table entity based on the event name
        entity = {'PartitionKey': 'pkey', 'RowKey': completed_event, 'completed': True}
        table_service.insert_or_replace_entity(table_name, entity)

    approval = yield context.wait_for_external_event("Approval")
    if approval:
        return "Approved"
    else:
        return "Rejected"
