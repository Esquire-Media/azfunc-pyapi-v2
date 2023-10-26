from libs.azure.functions import Blueprint
from libs.azure.functions.http import HttpRequest, HttpResponse
from azure.durable_functions import (
    # DurableOrchestrationClient,
    DurableOrchestrationContext,
    # EntityId,
    # DurableEntityContext,
    RetryOptions,
)
import os
import logging
import json

bp = Blueprint()


@bp.orchestration_trigger(context_name="context")
def esquire_campaign_proposal_orchestrator_root(context: DurableOrchestrationContext):
    
    # load payload information from orchestration context
    settings = context.get_input()
    
    # commonly used variables
    logging.warning(settings)
    taskHubName = str(context.get_input())
    retry = RetryOptions(15000, 1)
    egress = {
        "taskHubName": taskHubName,
        "instance_id": context.instance_id
    }

    # call activity to collect geocode addresses and get latlongs
    yield context.call_activity_with_retry(
        "activity_campaignProposal_geocodeAddresses",
        retry,
        {
            **egress,
            **settings
        },
    )

    # call activity to collect mover counts for each individual location as well as a deduped total
    yield context.call_activity_with_retry(
        "Activity_CollectMovers",
        retry,
        {
            **egress,
            **settings
        }
    )

    # # call activity to collect nearby competitors to each location
    # yield context.call_activity_with_retry(
    #     'Activity_CollectCompetitors',
    #     retry,
    #     {
    #         **egress,
    #     }
    # )

    # # call activity to populate the PPTX report template and upload it as bytes to Azure storage
    # yield context.call_activity_with_retry(
    #     'Activity_ExecuteReport',
    #     retry,
    #     {
    #         **egress,
    #     }
    # )

    # # generate the message body for the callback email
    # message_body = yield context.call_activity_with_retry(
    #     "activity_campaignProposal_generateCallback",
    #     retry,
    #     {
    #         **egress,
    #     },
    # )

    # # send the callback email
    # yield context.call_activity_with_retry(
    #     "activity_microsoftGraph_sendEmail",
    #     retry,
    #     {
    #         "from_id": os.environ["O365_EMAIL_ACCOUNT_ID"],
    #         "to_addresses": ["ryan@esquireadvertising.com"],
    #         "subject": "Test Callback",
    #         "message": message_body,
    #         "content_type": "HTML",
    #     },
    # )

    return ""
